"""网关编排：接入 -> 解码/自行判别归一 -> 时序判定 -> 直投/缓冲 -> 恢复补发。

两条接入路径：
    指定协议（老路）  RawMessage.protocol 为 "register"/"structured"，
                      严格按该协议解码；未知协议名 -> MALFORMED。
    协议自行判别      RawMessage.protocol 为 identification.AUTO_PROTOCOL，
                      网关对原始字节自行识别：
                        * 唯一解码成功 -> 接受；
                        * 唯一"认得出但帧坏" -> 该协议自己的、可区分的
                          坏帧结论（校验失败/截断/缺字段/非法值…），并带
                          上能读出的设备、序号依据；
                        * 认不出 -> UNKNOWN_PROTOCOL；
                        * 同时像两种（解码成功或都认得出）->
                          AMBIGUOUS_PROTOCOL，与认不出分开计数、分开日志。
                      任何情况下都不静默丢弃、不二选一硬塞。

按设备策略：
    窗口大小、缓冲容量、溢出策略均通过 DevicePolicyResolver 按 device_id
    解析；同一设备每次接入结论一致，设备之间互不影响，未覆盖设备回退默认。

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
from .policy import DevicePolicyResolver
from .protocols import ProtocolDecoder, RegisterFrameDecoder, StructuredDecoder
from .protocols.identification import (
    AUTO_PROTOCOL,
    ProtocolIdentifier,
    default_decoders,
)


@dataclass
class IngestResult:
    """一次接入的完整结论（测试日志据此打印序号/水位/依据）。"""

    ok: bool
    stage: str = ""                 # decode | identify | sequence | buffer | delivered
    reason: RejectReason | None = None
    detail: str = ""
    device_id: str = ""
    seq: int | None = None
    watermark: int | None = None
    delivered: bool = False
    buffered: bool = False
    protocol: str = ""              # 指定协议；自动识别时为命中/归属的协议名
    evidence: dict | None = None    # 坏帧里尽力读出的依据（device_id/seq 等）


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
    unidentified: int = 0
    ambiguous: int = 0
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
        device_policies: dict | None = None,
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
        policies = (
            config.device_policies if device_policies is None
            else device_policies
        )
        self.config = GatewayConfig(
            window_size=window_size,
            spool_capacity=spool_capacity,
            overflow_policy=overflow_policy,
            device_policies=dict(policies),
        ).validated()

        self.policies = DevicePolicyResolver(
            default_window_size=window_size,
            default_spool_capacity=spool_capacity,
            default_overflow_policy=overflow_policy,
            overrides=self.config.device_policies,
        )
        self.sequencer = DeviceSequencer(
            window_size=window_size,
            window_provider=self.policies.window_for,
        )
        self.spool = BoundedSpool(
            capacity=spool_capacity,
            overflow_policy=overflow_policy,
            policy_provider=lambda device_id: (
                self.policies.spool_capacity_for(device_id),
                self.policies.overflow_policy_for(device_id),
            ),
        )
        self.downstream = downstream or MockDownstream()
        self._decoders: dict[str, ProtocolDecoder] = {
            "register": RegisterFrameDecoder(),
            "structured": StructuredDecoder(),
        }
        self.identifier = ProtocolIdentifier(default_decoders())
        self._lock = threading.Lock()
        self.stats = GatewayStats()

    def ingest(self, msg: RawMessage) -> IngestResult:
        """处理一条原始报文，返回分阶段、原因可区分的结论。"""
        with self._lock:
            self.stats.total_in += 1

            # 1) 协议解码归一（指定协议老路 / 自动识别新路）
            if msg.protocol == AUTO_PROTOCOL:
                return self._ingest_auto(msg)

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
            protocol = msg.protocol

            return self._ingest_reading(reading, protocol)

    def _ingest_auto(self, msg: RawMessage) -> IngestResult:
        """自动判别路径：命中则进时序/缓冲；否则按三类结论分别拒绝。"""
        identified = self.identifier.identify(msg.payload)
        if identified.ok and identified.reading is not None:
            return self._ingest_reading(
                identified.reading, identified.reading.source,
            )
        reason = identified.reason or RejectReason.UNKNOWN_PROTOCOL
        return self._reject_identify(reason, identified)

    def _ingest_reading(self, reading, protocol: str) -> IngestResult:
        """已归一读数的时序判定 + 直投/缓冲（两条接入路径共用）。"""
        # 2) 设备时序判定（幂等/窗口/水位，窗口按设备解析）
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
                    f"seq={reading.seq} rejected by device window policy "
                    f"(watermark={decision.watermark_before}, "
                    f"window={self.policies.window_for(reading.device_id)})"
                ),
                device_id=reading.device_id, seq=reading.seq,
                watermark=wm, protocol=protocol,
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
                    watermark=wm, delivered=True, protocol=protocol,
                )
            except DownstreamUnavailable:
                pass  # 竞态：检查后被置为不可用，落入缓冲

        dropped_before = self.spool.stats.dropped_overflow
        accepted, reason = self.spool.put(reading)
        # 无论 DROP_NEWEST（拒绝新读数）还是 DROP_OLDEST（腾退旧读数），
        # 真实发生的溢出丢弃都必须进网关计数与快照
        dropped_delta = self.spool.stats.dropped_overflow - dropped_before
        if dropped_delta:
            self.stats.overflow_dropped += dropped_delta
        if not accepted:
            self.stats.rejected += 1
            cap = self.policies.spool_capacity_for(reading.device_id)
            pol = self.policies.overflow_policy_for(reading.device_id)
            return IngestResult(
                ok=False, stage="buffer",
                reason=RejectReason.BUFFER_OVERFLOW,
                detail=(
                    f"spool full for device {reading.device_id!r} "
                    f"(capacity={cap}, policy={pol})"
                ),
                device_id=reading.device_id, seq=reading.seq,
                watermark=wm, protocol=protocol,
            )
        return IngestResult(
            ok=True, stage="buffer",
            device_id=reading.device_id, seq=reading.seq,
            watermark=wm, buffered=True, protocol=protocol,
        )

    def _reject_identify(self, reason: RejectReason, result) -> IngestResult:
        """自动识别阶段的三类结论：归属坏帧 / 认不出 / 歧义，分开计数。"""
        self.stats.rejected += 1
        self.stats.decode_rejected += 1
        if reason is RejectReason.AMBIGUOUS_PROTOCOL:
            self.stats.ambiguous += 1
        elif reason is RejectReason.UNKNOWN_PROTOCOL:
            self.stats.unidentified += 1
        # 其它原因：认得出归属协议、但帧本身坏了，按该协议结论上报，
        # 只计入 decode_rejected，不进 unidentified/ambiguous。
        evidence = result.evidence or None
        if evidence and "device_id" in evidence:
            device_id = str(evidence["device_id"])
        else:
            device_id = ""
        seq = evidence.get("seq") if evidence else None
        protocol = result.protocol or ""
        seq_out = (
            int(seq)
            if isinstance(seq, int) and not isinstance(seq, bool)
            else None
        )
        return IngestResult(
            ok=False, stage="identify", reason=reason, detail=result.detail,
            device_id=device_id, seq=seq_out,
            protocol=protocol, evidence=evidence,
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

    def export_state(self) -> dict:
        """导出真实运行状态（水位/序号集/缓存/计数），供快照落盘。"""
        with self._lock:
            return {
                "stats": {
                    "total_in": self.stats.total_in,
                    "accepted": self.stats.accepted,
                    "rejected": self.stats.rejected,
                    "decode_rejected": self.stats.decode_rejected,
                    "duplicate": self.stats.duplicate,
                    "out_of_window": self.stats.out_of_window,
                    "rollback": self.stats.rollback,
                    "overflow_dropped": self.stats.overflow_dropped,
                    "delivered": self.stats.delivered,
                    "unidentified": self.stats.unidentified,
                    "ambiguous": self.stats.ambiguous,
                    "watermarks": dict(self.stats.watermarks),
                },
                "sequencer": self.sequencer.export_state(),
                "spool": self.spool.export_state(),
            }

    def load_state(self, state: dict) -> None:
        """从快照恢复运行状态（需用相同策略尺度新建网关后再调用）。

        严格校验：只接受运行状态白名单字段，任何未知字段或凭据类内容
        一律拒绝（见 snapshot.validate_runtime_state）。
        """
        from .snapshot import validate_runtime_state

        validate_runtime_state(state, envelope=False)
        with self._lock:
            self.sequencer.load_state(state["sequencer"])
            self.spool.load_state(state["spool"])
            st = state["stats"]
            wm = st["watermarks"]
            self.stats = GatewayStats(
                total_in=int(st["total_in"]),
                accepted=int(st["accepted"]),
                rejected=int(st["rejected"]),
                decode_rejected=int(st["decode_rejected"]),
                duplicate=int(st["duplicate"]),
                out_of_window=int(st["out_of_window"]),
                rollback=int(st["rollback"]),
                overflow_dropped=int(st["overflow_dropped"]),
                delivered=int(st["delivered"]),
                unidentified=int(st["unidentified"]),
                ambiguous=int(st["ambiguous"]),
                watermarks={k: int(v) for k, v in wm.items()},
            )
            # 溢出丢弃计数以缓冲内部计数为准，保证两边始终对得上
            self.stats.overflow_dropped = self.spool.stats.dropped_overflow

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
                overflow_dropped=self.stats.overflow_dropped,
                delivered=self.stats.delivered,
                unidentified=self.stats.unidentified,
                ambiguous=self.stats.ambiguous,
                watermarks=dict(self.stats.watermarks),
            )
            return snap
