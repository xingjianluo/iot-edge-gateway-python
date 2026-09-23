"""下游不可用时的有界边界缓冲、溢出策略与恢复补发。

策略（容量 capacity 为硬上限，绝不无限增长）：
    DROP_NEWEST（默认） 队列满时拒绝新读数（理由 BUFFER_OVERFLOW）。
                         语义：优先保证已缓存数据按序、不丢旧地补发。
    DROP_OLDEST         队列满时腾退最旧读数再放入新读数。
                         语义：优先保证实时性，丢弃积压中最旧数据。
    两种策略下被丢弃的读数都计入 stats.dropped_overflow，且可通过
    snapshot_stats() 查询，溢出行为确定且可解释。

恢复补发（drain）：
    * 严格 FIFO，逐条调用 deliver(reading)；
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
    """FIFO 有界缓冲。"""

    def __init__(
        self,
        capacity: int = 100,
        overflow_policy: str = OverflowPolicy.DROP_NEWEST,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if overflow_policy not in (
            OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST,
        ):
            raise ValueError(f"unknown overflow_policy: {overflow_policy!r}")
        self.capacity = capacity
        self.overflow_policy = overflow_policy
        self._q: deque[Reading] = deque()
        self._delivered: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        self.stats = BufferStats(capacity=capacity)

    def put(self, reading: Reading) -> tuple[bool, RejectReason | None]:
        """入缓冲。

        返回 (accepted, reason)：
            DROP_NEWEST 满时 -> (False, BUFFER_OVERFLOW)；
            DROP_OLDEST 满时 -> 腾退最旧并 (True, None)，
                被腾退读数计入 dropped_overflow。
        """
        key = (reading.device_id, reading.seq)
        with self._lock:
            if key in self._delivered:
                # 幂等防线：该读数此前已成功投递
                self.stats.duplicate_delivery += 1
                return True, None
            if len(self._q) >= self.capacity:
                self.stats.dropped_overflow += 1
                if self.overflow_policy == OverflowPolicy.DROP_NEWEST:
                    return False, RejectReason.BUFFER_OVERFLOW
                self._q.popleft()  # DROP_OLDEST：腾退最旧
            self._q.append(reading)
            self.stats.enqueued += 1
            self.stats.size = len(self._q)
            return True, None

    def drain(self, deliver: Callable[[Reading], None]) -> int:
        """下游可用时按 FIFO 序补发；返回本次成功投递条数。

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
                    self.stats.duplicate_delivery += 1
                    continue
                try:
                    deliver(reading)
                except Exception:
                    self.stats.downstream_failures += 1
                    break
                self._delivered.add(key)
                self._q.popleft()
                self.stats.delivered += 1
                delivered_now += 1
            self.stats.size = len(self._q)
            if delivered_now:
                self.stats.flushes += 1
        return delivered_now

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
