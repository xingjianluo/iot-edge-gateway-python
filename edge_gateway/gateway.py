"""网关编排：接入 -> 解码归一 -> 时序判定 -> 直投/缓冲 -> 恢复补发。

线程模型：
    ingest 使用单把网关锁串行化"判定 + 投递/入缓冲"这一临界区，
    保证并发接入下接受计数、水位与缓冲队列不会错乱；锁内不做阻塞 I/O
    （下游为进程内模拟，deliver 不阻塞）。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .buffer import BoundedSpool
from .dedup import DeviceSequencer
from .devices import RawMessage
from .downstream import DownstreamUnavailable, MockDownstream
from .models import RejectReason
from .protocols import ProtocolDecoder, RegisterFrameDecoder, StructuredDecoder


@dataclass
class IngestResult:
    """一次接入的完整结论（测试日志据此打印序号/水位/依据）。"""

    ok: bool
    stage: str = ""                 # decode | sequence | buffer | delivered
    reason: RejectReason | None = None
    detail: str = ""
    device_id: str = ""
    seq: int | None = None
    watermark: int | None = None
    delivered: bool = False
    buffered: bool = False


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

    def __init__(
        self,
        *,
        window_size: int = 32,
        spool_capacity: int = 100,
        overflow_policy: str = "drop_newest",
        downstream: MockDownstream | None = None,
    ) -> None:
        self.sequencer = DeviceSequencer(window_size=window_size)
        self.spool = BoundedSpool(
            capacity=spool_capacity, overflow_policy=overflow_policy,
        )
        self.downstream = downstream or MockDownstream()
        self._decoders: dict[str, ProtocolDecoder] = {
            "register": RegisterFrameDecoder(),
            "structured": StructuredDecoder(),
        }
        self._lock = threading.Lock()
        self.stats = GatewayStats()

    def ingest(self, msg: RawMessage) -> IngestResult:
        """处理一条原始报文，返回分阶段、原因可区分的结论。"""
        with self._lock:
            self.stats.total_in += 1

            # 1) 协议解码归一
            decoder = self._decoders.get(msg.protocol)
            if decoder is None:
                return self._reject_decode(
                    RejectReason.MALFORMED,
                    f"unknown protocol: {msg.protocol!r}",
                )
            decoded = decoder.decode(msg.payload)
            if not decoded.ok or decoded.reading is None:
                return self._reject_decode(
                    decoded.reason or RejectReason.MALFORMED,
                    decoded.detail,
                )
            reading = decoded.reading

            # 2) 设备时序判定（幂等/窗口/水位）
            decision = self.sequencer.check(reading.device_id, reading.seq)
            wm = decision.watermark_after
            self.stats.watermarks[reading.device_id] = wm
            if not decision.accepted:
                self.stats.rejected += 1
                reason = decision.reason or RejectReason.DUPLICATE_SEQ
                if reason is RejectReason.DUPLICATE_SEQ:
                    self.stats.duplicate += 1
                elif reason is RejectReason.OUT_OF_WINDOW:
                    self.stats.out_of_window += 1
                elif reason is RejectReason.SEQ_ROLLBACK:
                    self.stats.rollback += 1
                return IngestResult(
                    ok=False, stage="sequence", reason=reason,
                    detail=(
                        f"seq={reading.seq} rejected by window policy "
                        f"(watermark={decision.watermark_before}, "
                        f"window={self.sequencer.window_size})"
                    ),
                    device_id=reading.device_id, seq=reading.seq,
                    watermark=wm,
                )

            # 3) 已被时序层接受：尝试下游直投，失败则入边界缓冲
            self.stats.accepted += 1
            if self.downstream.available:
                try:
                    self.downstream.deliver(reading)
                    self.stats.delivered += 1
                    return IngestResult(
                        ok=True, stage="delivered",
                        device_id=reading.device_id, seq=reading.seq,
                        watermark=wm, delivered=True,
                    )
                except DownstreamUnavailable:
                    pass  # 竞态：检查后被置为不可用，落入缓冲

            accepted, reason = self.spool.put(reading)
            if not accepted:
                self.stats.rejected += 1
                return IngestResult(
                    ok=False, stage="buffer",
                    reason=RejectReason.BUFFER_OVERFLOW,
                    detail=(
                        f"spool full (capacity={self.spool.capacity}, "
                        f"policy={self.spool.overflow_policy})"
                    ),
                    device_id=reading.device_id, seq=reading.seq,
                    watermark=wm,
                )
            return IngestResult(
                ok=True, stage="buffer",
                device_id=reading.device_id, seq=reading.seq,
                watermark=wm, buffered=True,
            )

    def on_downstream_recovered(self) -> int:
        """下游恢复后按 FIFO 序补发，返回本次投递条数；同步统计。"""
        with self._lock:
            n = self.spool.drain(self.downstream.deliver)
            self.stats.delivered += n
            return n

    def _reject_decode(self, reason: RejectReason, detail: str) -> IngestResult:
        self.stats.rejected += 1
        self.stats.decode_rejected += 1
        return IngestResult(
            ok=False, stage="decode", reason=reason, detail=detail,
        )

    def stats_snapshot(self) -> GatewayStats:
        """汇总统计快照（含水位表与缓冲统计）。"""
        with self._lock:
            snap = GatewayStats(
                total_in=self.stats.total_in,
                accepted=self.stats.accepted,
                rejected=self.stats.rejected,
                decode_rejected=self.stats.decode_rejected,
                duplicate=self.stats.duplicate,
                out_of_window=self.stats.out_of_window,
                rollback=self.stats.rollback,
                overflow_dropped=self.spool.stats.dropped_overflow,
                delivered=self.stats.delivered,
                watermarks=dict(self.stats.watermarks),
            )
            return snap
