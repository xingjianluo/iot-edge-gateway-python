"""面向设备时序的幂等、乱序窗口与单调水位（实现待填充）。

语义：
- watermark = 该设备已接受的最大序号；
- 窗口 [watermark-window_size+1, watermark] 内已见序号再次到达 => DUPLICATE_SEQ；
- seq < watermark-window_size+1 => OUT_OF_WINDOW（迟到超窗）；
- 序号相对上次提交发生回退且不在窗口内 => SEQ_ROLLBACK（与超窗区分，
  回退特指携带更旧水位的重放源，按 last_seen_seq 判断）；
- 新序号接受，watermark 只增不减。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .models import DedupDecision, RejectReason


@dataclass
class _DeviceState:
    watermark: int = -1
    last_seen_seq: int = -1
    seen: set[int] = field(default_factory=set)


class DeviceSequencer:
    """每设备独立状态 + 独立锁，保证并发下计数与水位不错乱。"""

    def __init__(self, window_size: int = 32) -> None:
        self.window_size = window_size
        self._states: dict[str, _DeviceState] = {}
        self._lock = threading.Lock()

    def _state(self, device_id: str) -> _DeviceState:
        raise NotImplementedError

    def check(self, device_id: str, seq: int) -> DedupDecision:
        """提交一个序号并给出判定。骨架占位。"""
        raise NotImplementedError

    def watermark(self, device_id: str) -> int:
        """返回设备当前水位（无数据时为 -1）。"""
        raise NotImplementedError
