"""模拟下游汇聚端：可控通断，按序计数，拒绝重复投递。"""
from __future__ import annotations

import threading

from gateway.model import Reading


class DownstreamUnavailable(Exception):
    """下游不可用时由 deliver_strict 抛出；deliver 本身以 False 返回。"""


class MockDownstream:
    """进程内模拟下游汇聚端。

    - ``set_available`` 可在测试中制造中断与恢复；
    - 仅在可用时接受投递，按投递顺序记录 ``received``；
    - 维护已接收 (device, seq) 集合，任何重复投递都被拒绝并计数，
      即使网关补发也不会在下游重复计数。
    """

    def __init__(self, available: bool = True) -> None:
        self._available = available
        self._received: list[Reading] = []
        self._keys: set[tuple[str, int]] = set()
        self._lock = threading.RLock()
        self.delivery_attempts: int = 0
        self.duplicate_delivery: int = 0

    @property
    def available(self) -> bool:
        return self._available

    def set_available(self, available: bool) -> None:
        with self._lock:
            self._available = available

    def deliver(self, reading: Reading) -> bool:
        """投递一条读数；不可用或重复时返回 False。"""
        with self._lock:
            self.delivery_attempts += 1
            if not self._available:
                return False
            if reading.key in self._keys:
                self.duplicate_delivery += 1
                return False
            self._keys.add(reading.key)
            self._received.append(reading)
            return True

    @property
    def received(self) -> list[Reading]:
        with self._lock:
            return list(self._received)

    @property
    def received_count(self) -> int:
        with self._lock:
            return len(self._received)

    def reset(self) -> None:
        with self._lock:
            self._received.clear()
            self._keys.clear()
            self.delivery_attempts = 0
            self.duplicate_delivery = 0
