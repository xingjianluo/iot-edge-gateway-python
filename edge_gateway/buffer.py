"""下游不可用时的有界边界缓冲、溢出策略与恢复补发（实现待填充）。"""

from __future__ import annotations

import threading
from collections import deque

from .models import BufferStats, Reading, RejectReason


class OverflowPolicy:
    DROP_OLDEST = "drop_oldest"  # 腾退最旧读数（可解释：保实时性）
    DROP_NEWEST = "drop_newest"  # 拒绝最新读数（默认：保有序补发）


class BoundedSpool:
    """FIFO 有界缓冲；容量上限固定，溢出统计可查，补发幂等。"""

    def __init__(self, capacity: int = 100,
                 overflow_policy: str = OverflowPolicy.DROP_NEWEST) -> None:
        self.capacity = capacity
        self.overflow_policy = overflow_policy
        self._q: deque[Reading] = deque()
        self._delivered: set[tuple[str, int]] = set()
        self._lock = threading.Lock()
        self.stats = BufferStats(capacity=capacity)

    def put(self, reading: Reading) -> tuple[bool, RejectReason | None]:
        """入缓冲。骨架占位。"""
        raise NotImplementedError

    def drain(self, deliver) -> int:
        """下游可用时按 FIFO 序补发；deliver 抛异常即中断（保序）。骨架占位。"""
        raise NotImplementedError

    def snapshot_stats(self) -> BufferStats:
        """返回统计快照（线程安全拷贝）。"""
        raise NotImplementedError
