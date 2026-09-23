"""模拟下游汇聚端（进程内）：可切换可用/不可用，统计实际接收。

行为约定：
    available=False 时 deliver 抛 DownstreamUnavailable，调用方应将数据
    留在边界缓冲；恢复后由网关按序补发。
    下游按 (device_id, seq) 做最终幂等：即使网关层以外的路径重复投递，
    同一读数也只会计入一次（duplicates 单独统计，便于核对"不得重复计数"）。
"""

from __future__ import annotations

import threading

from .models import Reading


class DownstreamUnavailable(RuntimeError):
    """下游不可用时投递应抛出的异常。"""


class MockDownstream:
    """进程内模拟下游。"""

    def __init__(self) -> None:
        self.available: bool = True
        self.received: list[Reading] = []
        self.received_keys: set[tuple[str, int]] = set()
        self.duplicates: int = 0
        self._lock = threading.Lock()

    def set_available(self, available: bool) -> None:
        """注入/解除故障。"""
        self.available = available

    def deliver(self, reading: Reading) -> None:
        """投递一条读数。不可用抛异常；重复键只计一次。"""
        if not self.available:
            raise DownstreamUnavailable("simulated downstream outage")
        key = (reading.device_id, reading.seq)
        with self._lock:
            if key in self.received_keys:
                self.duplicates += 1
                return
            self.received_keys.add(key)
            self.received.append(reading)

    def reset(self) -> None:
        """清空接收记录（available 状态保持不变）。"""
        with self._lock:
            self.received.clear()
            self.received_keys.clear()
            self.duplicates = 0

    def delivered_seqs(self, device_id: str) -> list[int]:
        """返回某设备已接收序号（按接收顺序），便于测试断言。"""
        with self._lock:
            return [r.seq for r in self.received if r.device_id == device_id]

    def count(self) -> int:
        with self._lock:
            return len(self.received)
