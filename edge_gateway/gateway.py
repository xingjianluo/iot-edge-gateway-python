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
from .config import GatewayConfig, config_from_env
from .dedup import DeviceSequencer
from .devices import RawMessage
from .downstream import DownstreamUnavailable, MockDownstream
from .models import RejectReason
from .policies import PolicyResolver
from .protocols import (
    ProtocolDecoder,
    ProtocolDetector,
    RegisterFrameDecoder,
    StructuredDecoder,
)

AUTO_PROTOCOL = "auto"


@dataclass
class IngestResult:
    """一次接入的完整结论（测试日志据此打印序号/水位/依据）。"""

    ok: bool
    stage: str = ""                 # detect | decode | sequence | buffer | delivered
    reason: RejectReason | None = None
    detail: str = ""
    device_id: str = ""
    seq: int | None = None
    watermark: int | None = None
    delivered: bool = False
    buffered: bool = False
    detected_protocol: str = ""    # 自动识别命中的协议名（显式协议时为空）


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
        config: GatewayConfig | None = None,
        *,
        window_size: int | None = None,
        spool_capacity: int | None = None,
        overflow_policy: str | None = None,
        downstream: MockDownstream | None = None,
    ) -> None:
        if config is None:
            config = config_from_env()
        # 显式关键字参数覆盖配置对象，便于测试
        window_size = (
            config.window_size if window_size is None else window_size
        )
        spool_capacity = (
            config.spool_capacity if spool_capacity is None else spool_capacity
        )
        overflow_policy = (
            config.overflow_policy
            if overflow_policy is None else overflow_policy
        )
        self.config = GatewayConfig(
            window_size=window_size,
            spool_capacity=spool_capacity,
            overflow_policy=overflow_policy,
            device_policies=dict(config.device_policies),
        ).validated()

        # 以 device_id 为唯一依据解析策略；同一设备任意时刻结论一致
        self.policy_resolver = PolicyResolver(
            default_window_size=window_size,
            default_spool_capacity=spool_capacity,
            default_overflow_policy=overflow_policy,
            device_policies=config.device_policies,
        )
        self.sequencer = DeviceSequencer(
            window_size=window_size,
            window_for=self.policy_resolver.window_for,
        )
        self.spool = BoundedSpool(
            capacity=spool_capacity,
            overflow_policy=overflow_policy,
            capacity_for=self.policy_resolver.capacity_for,
            policy_for=self.policy_resolver.policy_for,
        )
        self.downstream = downstream or MockDownstream()
        self._decoders: dict[str, ProtocolDecoder] = {
            "register": RegisterFrameDecoder(),
            "structured": StructuredDecoder(),
        }
        self._detector = ProtocolDetector(list(self._decoders.values()))
        self._lock = threading.Lock()
        self.stats = GatewayStats()

    def ingest(self, msg: RawMessage) -> IngestResult:
        """处理一条原始报文，返回分阶段、原因可区分的结论。

        msg.protocol == "auto" 时先自行判别编码：
        识别不出 -> UNRECOGNIZED_PROTOCOL；同时像多种编码
        -> AMBIGUOUS_PROTOCOL；恰好命中一种才按该协议正式解析。
        显式指定 register/structured 的老路径行为不变。
        """
        with self._lock:
            self.stats.total_in += 1

            # 0) 协议自行判别（仅 auto 路径）
            detected = ""
            if msg.protocol == AUTO_PROTOCOL:
                verdict = self._detector.detect(msg.payload)
                if not verdict.ok or verdict.protocol is None:
                    return self._reject_decode(
                        verdict.reason or RejectReason.UNRECOGNIZED_PROTOCOL,
                        verdict.detail,
                        stage="detect",
                    )
                detected = verdict.protocol
                decoder = self._decoders[detected]
            else:
                # 1) 已知协议：直接按指定协议解码（老路径保持不变）
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
                    (
                        f"detected={detected}: {decoded.detail}"
                        if detected else decoded.detail
                    ),
                    detected=detected,
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
                        f"window={decision.window_size})"
                    ),
                    device_id=reading.device_id, seq=reading.seq,
                    watermark=wm, detected_protocol=detected,
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
                        detected_protocol=detected,
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
                        f"spool full "
                        f"(capacity={self.spool.effective_capacity(reading.device_id)}, "
                        f"policy={self.spool.effective_policy(reading.device_id)})"
                    ),
                    device_id=reading.device_id, seq=reading.seq,
                    watermark=wm, detected_protocol=detected,
                )
            return IngestResult(
                ok=True, stage="buffer",
                device_id=reading.device_id, seq=reading.seq,
                watermark=wm, buffered=True,
                detected_protocol=detected,
            )

    def on_downstream_recovered(self) -> int:
        """下游恢复后按 FIFO 序补发，返回本次投递条数；同步统计。"""
        with self._lock:
            n = self.spool.drain(self.downstream.deliver)
            self.stats.delivered += n
            return n

    def export_snapshot(self) -> dict:
        """导出运行状态快照（委托 snapshot.build_snapshot 做白名单校验）。"""
        from .snapshot import build_snapshot

        return build_snapshot(self)

    def import_snapshot(self, snap: dict) -> None:
        """从快照恢复水位/已见序号/待发缓存/全部计数（同配置新进程）。"""
        from .snapshot import validate_snapshot

        validate_snapshot(snap)
        with self._lock:
            self.sequencer.import_state(snap["sequencer"])
            self.spool.import_state(snap["spool"])
            st = snap["stats"]
            self.stats = GatewayStats(
                total_in=st["total_in"],
                accepted=st["accepted"],
                rejected=st["rejected"],
                decode_rejected=st["decode_rejected"],
                duplicate=st["duplicate"],
                out_of_window=st["out_of_window"],
                rollback=st["rollback"],
                overflow_dropped=st["overflow_dropped"],
                delivered=st["delivered"],
                watermarks=dict(st["watermarks"]),
            )

    @classmethod
    def restore_from_snapshot(
        cls,
        snap: dict,
        *,
        config: GatewayConfig | None = None,
        downstream: MockDownstream | None = None,
        **kwargs,
    ) -> "Gateway":
        """在全新进程语义下：先按配置构造网关，再灌入快照运行状态。

        可额外传 window_size/spool_capacity/overflow_policy 覆盖配置，
        含义与 Gateway(...) 一致（按设备策略仍以配置对象为准）。
        """
        gw = cls(config=config, downstream=downstream, **kwargs)
        gw.import_snapshot(snap)
        return gw

    def _reject_decode(
        self,
        reason: RejectReason,
        detail: str,
        *,
        stage: str = "decode",
        detected: str = "",
    ) -> IngestResult:
        self.stats.rejected += 1
        self.stats.decode_rejected += 1
        return IngestResult(
            ok=False, stage=stage, reason=reason, detail=detail,
            detected_protocol=detected,
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
