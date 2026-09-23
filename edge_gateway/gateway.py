"""网关编排：接入 -> 解码归一 -> 时序判定 -> 直投/缓冲 -> 恢复补发。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .buffer import BoundedSpool
from .dedup import DeviceSequencer
from .devices import RawMessage
from .downstream import MockDownstream
from .models import DecodeResult, DedupDecision, RejectReason
from .protocols import ProtocolDecoder, RegisterFrameDecoder, StructuredDecoder

# 各阶段拒绝/异常计数
StageReject = tuple[str, RejectReason, str]


@dataclass
class IngestResult:
    """一次接入的完整结论（测试日志据此打印序号/水位/依据）。"""

    ok: bool
    stage: str = ""
    reason: RejectReason | None = None
    detail: str = ""
    seq: int | None = None
    watermark: int | None = None


@dataclass
class GatewayStats:
    total_in: int = 0
    accepted: int = 0
    rejected: int = 0
    decode_rejected: int = 0
    duplicate: int = 0
    out_of_window: int = 0
    rollback: int = 0
    overflow_dropped: int = 0
    delivered: int = 0
    watermarks: dict[str, int] = field(default_factory=dict)


class Gateway:
    """进程内边缘网关。"""

    def __init__(self, *, window_size: int = 32, spool_capacity: int = 100,
                 overflow_policy: str = "drop_newest",
                 downstream: MockDownstream | None = None) -> None:
        self.sequencer = DeviceSequencer(window_size=window_size)
        self.spool = BoundedSpool(capacity=spool_capacity,
                                  overflow_policy=overflow_policy)
        self.downstream = downstream or MockDownstream()
        self._decoders: dict[str, ProtocolDecoder] = {
            "register": RegisterFrameDecoder(),
            "structured": StructuredDecoder(),
        }
        self._lock = threading.Lock()
        self.stats = GatewayStats()

    def ingest(self, msg: RawMessage) -> IngestResult:
        """处理一条原始报文。骨架占位。"""
        raise NotImplementedError

    def on_downstream_recovered(self) -> int:
        """下游恢复：按序补发缓冲。骨架占位。"""
        raise NotImplementedError

    def stats_snapshot(self) -> GatewayStats:
        """汇总统计快照。骨架占位。"""
        raise NotImplementedError
