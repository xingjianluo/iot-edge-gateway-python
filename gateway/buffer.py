"""有界缓冲、确定性溢出策略与统计。

- 容量 ``capacity`` 为缓冲中读数条数的硬上限。
- 溢出策略：
  * DROP_OLDEST：缓冲满时驱逐最旧的一条（FIFO 队首），再放入新读数；
    被驱逐读数计入 ``dropped_oldest``，且会记录其 (device, seq)。
  * DROP_NEWEST：缓冲满时拒绝新读数（不改变缓冲），计入 ``dropped_newest``。
- 两种策略的结果都是确定且可解释的：任何丢弃都有原因与被丢弃项标识。
"""
from __future__ import annotations

from collections import deque
from enum import Enum

from gateway.model import Reading


class OverflowPolicy(Enum):
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"


class BoundedBuffer:
    """容量有上限的 FIFO 补发缓冲。"""

    def __init__(self, capacity: int, policy: OverflowPolicy) -> None:
        if capacity <= 0:
            raise ValueError("capacity 必须为正整数")
        self.capacity = capacity
        self.policy = policy
        self._q: deque[Reading] = deque()
        self.total_in: int = 0
        self.dropped_oldest: int = 0
        self.dropped_newest: int = 0
        self.last_dropped: tuple[str, int] | None = None

    def put(self, reading: Reading) -> tuple[bool, Reading | None]:
        """放入读数。

        返回 (是否放入, 被丢弃的读数)：
        - 正常放入：(True, None)
        - DROP_OLDEST 驱逐旧值后放入：(True, 被驱逐读数)
        - DROP_NEWEST 拒绝：(False, 新读数本身)
        """
        self.total_in += 1
        if len(self._q) < self.capacity:
            self._q.append(reading)
            return True, None

        if self.policy is OverflowPolicy.DROP_OLDEST:
            evicted = self._q.popleft()
            self.dropped_oldest += 1
            self.last_dropped = evicted.key
            self._q.append(reading)
            return True, evicted

        # DROP_NEWEST
        self.dropped_newest += 1
        self.last_dropped = reading.key
        return False, reading

    def drain(self, max_items: int | None = None) -> list[Reading]:
        """按 FIFO 顺序取出（用于下游恢复后补发）。"""
        out: list[Reading] = []
        if max_items is None:
            while self._q:
                out.append(self._q.popleft())
        else:
            for _ in range(min(max_items, len(self._q))):
                out.append(self._q.popleft())
        return out

    def restore_front(self, items: list[Reading]) -> None:
        """把未投递成功的读数按原顺序退回队首（恢复补发途中再次中断）。

        退回项不重复计入 total_in；若空间不足（理论上不会发生，因为这些项
        本就来自该缓冲），按当前溢出策略处理。
        """
        for reading in reversed(items):
            if len(self._q) < self.capacity:
                self._q.appendleft(reading)
            elif self.policy is OverflowPolicy.DROP_OLDEST:
                # 极端兜底：丢弃队尾（最新）以保住退回的更旧数据顺序
                evicted = self._q.pop()
                self.dropped_oldest += 1
                self.last_dropped = evicted.key
                self._q.appendleft(reading)
            else:
                self.dropped_newest += 1
                self.last_dropped = reading.key

    def peek_all(self) -> tuple[Reading, ...]:
        return tuple(self._q)

    def stats(self) -> dict[str, object]:
        return {
            "capacity": self.capacity,
            "size": len(self._q),
            "total_in": self.total_in,
            "dropped_oldest": self.dropped_oldest,
            "dropped_newest": self.dropped_newest,
            "last_dropped": self.last_dropped,
            "policy": self.policy.value,
        }

    def __len__(self) -> int:
        return len(self._q)
