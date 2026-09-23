"""网关编排：解码 -> 去重/窗口判定 -> 提交水位 -> 下游投递/有界缓冲/补发。

所有接入在同一把可重入锁下串行化，保证并发接入下计数、水位、缓冲不错乱。
只在“水位连续推进、读数真正就绪”时才尝试下发，因此下游收到的数据严格按
每设备序号有序；下游不可用时读数进入有界缓冲，恢复后 FIFO 补发。
"""
from __future__ import annotations

import logging
import threading

from gateway.buffer import BoundedBuffer, OverflowPolicy
from gateway.dedup import DeviceStateRegistry
from gateway.downstream import MockDownstream
from gateway.model import AcceptResult, Reading, RejectReason
from gateway.protocols import FrameDecoder

logger = logging.getLogger("gateway")


class EdgeGateway:
    def __init__(
        self,
        decoders: list[FrameDecoder],
        window: int,
        capacity: int,
        overflow_policy: OverflowPolicy,
        downstream: MockDownstream | None = None,
        log_decisions: bool = True,
    ) -> None:
        if not decoders:
            raise ValueError("至少需要一个解码器")
        self.decoders = decoders
        self.registry = DeviceStateRegistry(window)
        self.downstream = downstream or MockDownstream()
        self.buffer = BoundedBuffer(capacity, overflow_policy)
        self.log_decisions = log_decisions
        self._lock = threading.RLock()
        # 已接收但因乱序尚未连续就绪的读数：(device, seq) -> Reading
        self._pending: dict[tuple[str, int], Reading] = {}
        # 统计
        self.received_frames: int = 0
        self.rejected_decode: int = 0
        self.rejected_duplicate: int = 0
        self.rejected_rollback: int = 0
        self.rejected_out_of_window: int = 0
        self.accepted_readings: int = 0

    # ---- 日志 ---------------------------------------------------------

    def _log(
        self,
        device: str,
        seq: int | None,
        watermark: int,
        accepted: bool,
        reason: RejectReason | None,
        detail: str,
    ) -> None:
        if not self.log_decisions:
            return
        verdict = "ACCEPT" if accepted else f"REJECT[{reason.value if reason else '?'}]"
        logger.info(
            "device=%s seq=%s watermark=%d => %s | %s",
            device,
            seq,
            watermark,
            verdict,
            detail,
        )

    # ---- 解码 ---------------------------------------------------------

    def _decode(self, raw: bytes) -> tuple[Reading | None, RejectReason | None, str]:
        detail = "帧不被任何解码器认领"
        for dec in self.decoders:
            if dec.match(raw):
                return dec.decode(raw)
            detail = f"帧不被任何解码器认领（{type(dec).__name__} 未匹配）"
        return None, RejectReason.DECODE_ERROR, detail

    # ---- 投递与补发 ----------------------------------------------------

    def _buffer_and_drain(self, ready_in_order: list[Reading]) -> None:
        """先缓冲新就绪读数，再在下游可用时按 FIFO 整体补发。"""
        for rd in ready_in_order:
            stored, victim = self.buffer.put(rd)
            if victim is rd and not stored:
                logger.warning(
                    "溢出策略=%s 拒绝最新读数：device=%s seq=%d（缓冲已满 %d）",
                    self.buffer.policy.value,
                    rd.device_id,
                    rd.seq,
                    self.buffer.capacity,
                )
            elif victim is not None:
                logger.warning(
                    "溢出策略=%s 丢弃最旧读数 device=%s seq=%d，为放入 "
                    "device=%s seq=%d",
                    self.buffer.policy.value,
                    victim.device_id,
                    victim.seq,
                    rd.device_id,
                    rd.seq,
                )

        if self.downstream.available and len(self.buffer) > 0:
            self._drain_buffer_locked()

    def _drain_buffer_locked(self) -> int:
        """调用方持锁。按 FIFO 补发；途中再次中断则把剩余项按原顺序退回队首。"""
        pending = self.buffer.drain()
        sent = 0
        unsent: list[Reading] = []
        for rd in pending:
            if self.downstream.deliver(rd):
                sent += 1
            else:
                unsent.append(rd)
        if unsent:
            self.buffer.restore_front(unsent)
        return sent

    def on_downstream_restored(self) -> int:
        """下游恢复事件：按序补发缓冲，返回本次补发条数。"""
        with self._lock:
            if not self.downstream.available or len(self.buffer) == 0:
                return 0
            sent = self._drain_buffer_locked()
            logger.info(
                "下游恢复补发完成：本次补发 %d 条，剩余缓冲 %d",
                sent,
                len(self.buffer),
            )
            return sent

    # ---- 主入口 -------------------------------------------------------

    def ingest(self, raw: bytes) -> AcceptResult:
        """接入一帧原始字节，返回可区分的判定结论。"""
        with self._lock:
            self.received_frames += 1
            reading, reason, detail = self._decode(raw)
            if reading is None:
                self.rejected_decode += 1
                self._log("-", None, -1, False, reason, detail)
                return AcceptResult(False, None, reason, detail, -1)

            state = self.registry.get(reading.device_id)
            ok, reject_reason, why, watermark = state.inspect(reading.seq)
            if not ok:
                if reject_reason is RejectReason.DUPLICATE_SEQ:
                    self.rejected_duplicate += 1
                elif reject_reason is RejectReason.SEQ_ROLLBACK:
                    self.rejected_rollback += 1
                elif reject_reason is RejectReason.OUT_OF_WINDOW:
                    self.rejected_out_of_window += 1
                self._log(
                    reading.device_id, reading.seq, watermark, False,
                    reject_reason, why,
                )
                return AcceptResult(False, reading, reject_reason, why, watermark)

            # 通过判定：先存入乱序待就绪表，再提交水位
            self._pending[reading.key] = reading
            advanced = state.commit(reading.seq)
            self.accepted_readings += 1
            wm = state.watermark
            self._log(
                reading.device_id, reading.seq, wm, True, None,
                f"{why}；提交后水位={wm}，本次连续就绪序号={advanced}",
            )

            ready: list[Reading] = []
            for s in advanced:
                rd = self._pending.pop((reading.device_id, s), None)
                if rd is not None:
                    ready.append(rd)
            self._buffer_and_drain(ready)
            return AcceptResult(True, reading, None, why, wm)

    # ---- 观测 ---------------------------------------------------------

    def stats(self) -> dict[str, object]:
        with self._lock:
            return {
                "received_frames": self.received_frames,
                "accepted_readings": self.accepted_readings,
                "rejected_decode": self.rejected_decode,
                "rejected_duplicate": self.rejected_duplicate,
                "rejected_rollback": self.rejected_rollback,
                "rejected_out_of_window": self.rejected_out_of_window,
                "downstream_available": self.downstream.available,
                "downstream_received": self.downstream.received_count,
                "duplicate_delivery": self.downstream.duplicate_delivery,
                "watermarks": {
                    dev: st.watermark for dev, st in self.registry._states.items()
                },
                "pending_in_window": {
                    f"{dev}:{seq}": True for (dev, seq) in self._pending
                },
                "buffer": self.buffer.stats(),
            }
