"""下游不可用时的有界边界缓冲、溢出策略与恢复补发。

容量与溢出策略支持按设备差异化（见 policy.DevicePolicyResolver）：
    构造时给 capacity/overflow_policy 作为全局默认；
    额外给 policy_provider(device_id) -> (capacity, overflow_policy) 后，
    每台设备满不满、满了丢谁，都按它自己的策略判定，设备之间互不影响。
    未注册覆盖的设备始终回退全局默认。

队列结构：
    _q                全局 FIFO（deque[Reading]），恢复补发严格按全局到达序
    _queued[device]   该设备当前在 _q 中的键的 FIFO（容量按它计数）
    _delivered        已成功投递的 (device_id, seq)，补发幂等防线

策略（容量为每设备硬上限，绝不无限增长）：
    DROP_NEWEST（默认） 队列满时拒绝新读数（理由 BUFFER_OVERFLOW）。
                         语义：优先保证已缓存数据按序、不丢旧地补发。
    DROP_OLDEST         队列满时腾退该设备最旧读数再放入新读数。
                         语义：优先保证实时性，丢弃积压中最旧数据。
    两种策略下被丢弃的读数都计入 stats.dropped_overflow，且可通过
    snapshot_stats() 查询，溢出行为确定且可解释。

恢复补发（drain）：
    * 严格全局 FIFO，逐条调用 deliver(reading)；
    * deliver 抛异常立即中断，失败项保留在队首，下次恢复重试，保证顺序；
    * 已成功投递的 (device_id, seq) 记录在案，再次出现跳过且计入
      duplicate_delivery，补发不会重复计数；
    * 全部操作在同一把锁下串行，并发接入/补发下计数不错乱。

状态可导出/恢复（export_state/load_state），用于进程重启后接着跑。
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable

from .models import BufferStats, Reading, RejectReason


class OverflowPolicy:
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


PolicyProvider = Callable[[str], "tuple[int, str]"]


class BoundedSpool:
    """全局 FIFO 缓冲，容量与溢出策略可按设备解析。"""

    def __init__(
        self,
        capacity: int = 100,
        overflow_policy: str = OverflowPolicy.DROP_NEWEST,
        *,
        policy_provider: PolicyProvider | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if overflow_policy not in (
            OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST,
        ):
            raise ValueError(f"unknown overflow_policy: {overflow_policy!r}")
        self.capacity = capacity
        self.overflow_policy = overflow_policy
        self._policy_provider = policy_provider
        self._q: deque[Reading] = deque()
        self._queued: dict[str, deque[tuple[str, int]]] = {}
        self._delivered: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        self.stats = BufferStats(capacity=capacity)

    def _policy_for(self, device_id: str) -> tuple[int, str]:
        if self._policy_provider is None:
            return self.capacity, self.overflow_policy
        cap, policy = self._policy_provider(device_id)
        cap = int(cap)
        if cap < 1:
            raise ValueError(
                f"resolved spool capacity for {device_id!r} must be >= 1",
            )
        if policy not in (OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST):
            raise ValueError(f"unknown overflow_policy: {policy!r}")
        return cap, policy

    def _device_deque(self, device_id: str) -> deque[tuple[str, int]]:
        dq = self._queued.get(device_id)
        if dq is None:
            dq = deque()
            self._queued[device_id] = dq
        return dq

    def put(self, reading: Reading) -> tuple[bool, RejectReason | None]:
        """入缓冲。

        返回 (accepted, reason)：
            DROP_NEWEST 该设备满时 -> (False, BUFFER_OVERFLOW)；
            DROP_OLDEST 该设备满时 -> 腾退其最旧并 (True, None)，
                被腾退读数计入 dropped_overflow。
        """
        key = (reading.device_id, reading.seq)
        with self._lock:
            if key in self._delivered:
                # 幂等防线：该读数此前已成功投递
                self.stats.duplicate_delivery += 1
                return True, None
            capacity, policy = self._policy_for(reading.device_id)
            dq = self._device_deque(reading.device_id)
            if len(dq) >= capacity:
                self.stats.dropped_overflow += 1
                if policy == OverflowPolicy.DROP_NEWEST:
                    return False, RejectReason.BUFFER_OVERFLOW
                # DROP_OLDEST：腾退该设备最旧读数（同时移出全局 FIFO）
                old_key = dq.popleft()
                self._remove_global(old_key)
            self._q.append(reading)
            dq.append(key)
            self.stats.enqueued += 1
            self.stats.size = len(self._q)
            return True, None

    def _remove_global(self, key: tuple[str, int]) -> None:
        for i, reading in enumerate(self._q):
            if (reading.device_id, reading.seq) == key:
                del self._q[i]
                return

    def _discard_device_key(self, key: tuple[str, int]) -> None:
        """投递成功后从对应设备的 FIFO 中移除该键（正常即在队首）。"""
        dq = self._queued.get(key[0])
        if not dq:
            return
        if dq[0] == key:
            dq.popleft()
            return
        try:
            dq.remove(key)
        except ValueError:
            pass

    def drain(self, deliver: Callable[[Reading], None]) -> int:
        """下游可用时按全局 FIFO 序补发；返回本次成功投递条数。

        deliver 抛异常即中断（失败项及后续全部保留），并累计
        downstream_failures；已投递键用于幂等去重。
        """
        delivered_now = 0
        with self._lock:
            while self._q:
                reading = self._q[0]
                key = (reading.device_id, reading.seq)
                if key in self._delivered:
                    self._q.popleft()
                    self._discard_device_key(key)
                    self.stats.duplicate_delivery += 1
                    continue
                try:
                    deliver(reading)
                except Exception:
                    self.stats.downstream_failures += 1
                    break
                self._delivered.add(key)
                self._q.popleft()
                self._discard_device_key(key)
                self.stats.delivered += 1
                delivered_now += 1
            self.stats.size = len(self._q)
            if delivered_now:
                self.stats.flushes += 1
        return delivered_now

    def export_state(self) -> dict:
        """导出缓冲真实运行状态（队列内容、已投递键、计数）。"""
        with self._lock:
            queued = [
                {
                    "device_id": r.device_id,
                    "seq": r.seq,
                    "timestamp_ms": r.timestamp_ms,
                    "metric": r.metric,
                    "value": r.value,
                    "source": r.source,
                }
                for r in self._q
            ]
            return {
                "queued": queued,
                "delivered": sorted(self._delivered),
                "stats": {
                    "enqueued": self.stats.enqueued,
                    "dropped_overflow": self.stats.dropped_overflow,
                    "delivered": self.stats.delivered,
                    "duplicate_delivery": self.stats.duplicate_delivery,
                    "downstream_failures": self.stats.downstream_failures,
                    "flushes": self.stats.flushes,
                },
            }

    def load_state(self, state: dict) -> None:
        """从快照恢复队列、已投递键与计数（整体替换）。"""
        readings = deque(
            Reading(
                device_id=item["device_id"],
                seq=int(item["seq"]),
                timestamp_ms=int(item["timestamp_ms"]),
                metric=item["metric"],
                value=float(item["value"]),
                source=item["source"],
            )
            for item in state.get("queued", ())
        )
        queued: dict[str, deque[tuple[str, int]]] = {}
        for reading in readings:
            dq = queued.setdefault(reading.device_id, deque())
            dq.append((reading.device_id, reading.seq))
        stats = state.get("stats", {})
        with self._lock:
            self._q = readings
            self._queued = queued
            self._delivered = {
                (device_id, int(seq))
                for device_id, seq in state.get("delivered", ())
            }
            self.stats = BufferStats(
                capacity=self.capacity,
                size=len(readings),
                enqueued=int(stats.get("enqueued", 0)),
                dropped_overflow=int(stats.get("dropped_overflow", 0)),
                delivered=int(stats.get("delivered", 0)),
                duplicate_delivery=int(stats.get("duplicate_delivery", 0)),
                downstream_failures=int(stats.get("downstream_failures", 0)),
                flushes=int(stats.get("flushes", 0)),
            )

    def snapshot_stats(self) -> BufferStats:
        """返回统计快照（线程安全拷贝）。"""
        with self._lock:
            return BufferStats(
                capacity=self.stats.capacity,
                size=len(self._q),
                enqueued=self.stats.enqueued,
                dropped_overflow=self.stats.dropped_overflow,
                delivered=self.stats.delivered,
                duplicate_delivery=self.stats.duplicate_delivery,
                downstream_failures=self.stats.downstream_failures,
                flushes=self.stats.flushes,
            )
