"""下游不可用时的有界边界缓冲、溢出策略与恢复补发。

容量与溢出策略可以按设备差异化：每个设备有自己的容量上限与满队策略，
缺失配置的设备回退全局默认；队列本身仍为全局 FIFO，设备之间互不挤占。

策略（容量 capacity 为每设备硬上限，绝不无限增长）：
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
"""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable

from .models import BufferStats, Reading, RejectReason


class OverflowPolicy:
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class BoundedSpool:
    """全局 FIFO 有界缓冲；容量/溢出策略按设备解析。"""

    def __init__(
        self,
        capacity: int = 100,
        overflow_policy: str = OverflowPolicy.DROP_NEWEST,
        *,
        capacity_for: Callable[[str], int] | None = None,
        policy_for: Callable[[str], str] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if overflow_policy not in (
            OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST,
        ):
            raise ValueError(f"unknown overflow_policy: {overflow_policy!r}")
        self.capacity = capacity  # 全局默认容量（兼容查询）
        self.overflow_policy = overflow_policy  # 全局默认策略
        self._capacity_for = capacity_for or (lambda _d: capacity)
        self._policy_for = policy_for or (lambda _d: overflow_policy)
        self._q: deque[Reading] = deque()
        # 每设备在队中的序号集合，用于按设备统计在队长度与定位最旧项
        self._pending: dict[str, set[int]] = {}
        self._delivered: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        self.stats = BufferStats(capacity=capacity)

    def effective_capacity(self, device_id: str) -> int:
        """该设备实际生效的缓冲容量。"""
        return self._capacity_for(device_id)

    def effective_policy(self, device_id: str) -> str:
        """该设备实际生效的溢出策略。"""
        return self._policy_for(device_id)

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
            pending = self._pending.setdefault(reading.device_id, set())
            cap = self._capacity_for(reading.device_id)
            if len(pending) >= cap:
                self.stats.dropped_overflow += 1
                if self._policy_for(reading.device_id) == \
                        OverflowPolicy.DROP_NEWEST:
                    return False, RejectReason.BUFFER_OVERFLOW
                # DROP_OLDEST：腾退该设备在全局队列中最旧的读数
                self._evict_oldest(reading.device_id)
                pending = self._pending[reading.device_id]
            self._q.append(reading)
            pending.add(reading.seq)
            self.stats.enqueued += 1
            self.stats.size = len(self._q)
            return True, None

    def _evict_oldest(self, device_id: str) -> None:
        """从全局 FIFO 中移除该设备最旧的在队读数（调用方持锁）。"""
        for i, queued in enumerate(self._q):
            if queued.device_id == device_id:
                del self._q[i]
                self._pending[device_id].discard(queued.seq)
                return

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
                    self._discard_pending(reading)
                    self.stats.duplicate_delivery += 1
                    continue
                try:
                    deliver(reading)
                except Exception:
                    self.stats.downstream_failures += 1
                    break
                self._delivered.add(key)
                self._q.popleft()
                self._discard_pending(reading)
                self.stats.delivered += 1
                delivered_now += 1
            self.stats.size = len(self._q)
            if delivered_now:
                self.stats.flushes += 1
        return delivered_now

    def _discard_pending(self, reading: Reading) -> None:
        """出队后同步每设备在队集合（调用方持锁）。"""
        pending = self._pending.get(reading.device_id)
        if pending is not None:
            pending.discard(reading.seq)
            if not pending:
                del self._pending[reading.device_id]

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

    def export_state(self) -> dict:
        """导出缓冲运行状态（待发 FIFO、已投递键、计数），无配置/无凭据。"""
        with self._lock:
            return {
                "queue": [
                    {
                        "device_id": r.device_id,
                        "seq": r.seq,
                        "timestamp_ms": r.timestamp_ms,
                        "metric": r.metric,
                        "value": r.value,
                        "source": r.source,
                    }
                    for r in self._q
                ],
                "delivered": [
                    {"device_id": d, "seq": s} for d, s in self._delivered
                ],
                "stats": {
                    "enqueued": self.stats.enqueued,
                    "dropped_overflow": self.stats.dropped_overflow,
                    "delivered": self.stats.delivered,
                    "duplicate_delivery": self.stats.duplicate_delivery,
                    "downstream_failures": self.stats.downstream_failures,
                    "flushes": self.stats.flushes,
                },
            }

    def import_state(self, data: dict) -> None:
        """从快照恢复缓冲（全新进程调用：队列、已投递键与计数接得上）。"""
        if not isinstance(data, dict):
            raise TypeError("spool snapshot must be an object")
        queue = data.get("queue", [])
        delivered = data.get("delivered", [])
        stats = data.get("stats", {})
        if not isinstance(queue, list) or not isinstance(delivered, list):
            raise TypeError("spool snapshot fields malformed")

        q: deque[Reading] = deque()
        pending: dict[str, set[int]] = {}
        for item in queue:
            reading = Reading(
                device_id=item["device_id"],
                seq=item["seq"],
                timestamp_ms=item["timestamp_ms"],
                metric=item["metric"],
                value=item["value"],
                source=item["source"],
            )
            q.append(reading)
            pending.setdefault(reading.device_id, set()).add(reading.seq)

        delivered_keys: set[tuple[str, int]] = set()
        for item in delivered:
            delivered_keys.add((item["device_id"], item["seq"]))

        with self._lock:
            self._q = q
            self._pending = pending
            self._delivered = delivered_keys
            self.stats = BufferStats(
                capacity=self.capacity,
                size=len(q),
                enqueued=int(stats.get("enqueued", 0)),
                dropped_overflow=int(stats.get("dropped_overflow", 0)),
                delivered=int(stats.get("delivered", 0)),
                duplicate_delivery=int(stats.get("duplicate_delivery", 0)),
                downstream_failures=int(stats.get("downstream_failures", 0)),
                flushes=int(stats.get("flushes", 0)),
            )
