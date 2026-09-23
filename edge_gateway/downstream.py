"""模拟下游汇聚端（可切换可用/不可用，统计实际接收）。"""

from __future__ import annotations

import threading

from .models import Reading


class DownstreamUnavailable(RuntimeError):
    """下游不可用时投递应抛出的异常。"""


class MockDownstream:
    """进程内模拟下游：支持故障注入、按 (device, seq) 记录实际接收。"""

    def __init__(self) -> None:
        self.available: bool = True
        self.received: list[Reading] = []
        self.received_keys: set[tuple[str, int]] = set()
        self._lock = threading.Lock()

    def set_available(self, available: bool) -> None:
        self.available = available

    def deliver(self, reading: Reading) -> None:
        """投递一条读数；不可用或模拟重复时表现需可预期。骨架占位。"""
        raise NotImplementedError

    def reset(self) -> None:
        """清空接收记录。"""
        raise NotImplementedError
