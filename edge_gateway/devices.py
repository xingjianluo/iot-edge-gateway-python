"""进程内模拟设备端：按脚本产生寄存器帧 / 结构化报文（含异常帧）。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RawMessage:
    """设备侧发出的原始报文及它实际使用的协议。"""

    protocol: str  # "register" | "structured"
    payload: bytes
    label: str  # 人类可读标签，便于测试日志


class SimulatedDevice:
    """按 (seq, 异常类型) 脚本产生原始报文。骨架占位。"""

    def __init__(self, device_id: str, protocol: str = "register",
                 metric: str = "temperature", base_ts: int = 1_700_000_000_000,
                 interval_ms: int = 100) -> None:
        self.device_id = device_id
        self.protocol = protocol
        self.metric = metric
        self.base_ts = base_ts
        self.interval_ms = interval_ms

    def emit(self, seq: int, *, value: float | None = None,
             corrupt: str | None = None) -> RawMessage:
        """产生第 seq 条报文；corrupt 可选:
        truncate/bad_crc/bad_magic/missing_field/bad_value/malformed。
        骨架占位。"""
        raise NotImplementedError
