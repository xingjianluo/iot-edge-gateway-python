"""面向设备时序的幂等、乱序窗口与单调水位。

核心状态（每设备独立）：
    watermark     已接受序号的最大值，只增不减（单调水位）
    last_seen_seq 最近一次到达（无论接受与否）的序号
    seen          已接受序号集合，仅保留窗口 [low, watermark] 内成员

判定规则（window_size=W，low=watermark-W+1）：
    seq > watermark          接受（前向推进，允许跳号），watermark=seq
    low <= seq <= watermark:
        seq in seen          拒绝 DUPLICATE_SEQ（重复，只计一次）
        seq not seen         接受（窗口内乱序/迟到补缺，不推进水位）
    seq < low:
        seq < 上一条到达序号  拒绝 SEQ_ROLLBACK（观察到序号回退）
        否则                 拒绝 OUT_OF_WINDOW（迟到超出可接受窗口）

水位推进时修剪 seen，集合大小有界；每个设备一把锁，
设备之间互不阻塞，并发下计数与水位不错乱。
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
    lock: threading.Lock = field(default_factory=threading.Lock)


class DeviceSequencer:
    """每设备独立状态 + 独立锁。"""

    def __init__(self, window_size: int = 32) -> None:
        if window_size < 1:
            raise ValueError("window_size must be >= 1")
        self.window_size = window_size
        self._states: dict[str, _DeviceState] = {}
        self._registry_lock = threading.Lock()

    def _state(self, device_id: str) -> _DeviceState:
        st = self._states.get(device_id)
        if st is None:
            with self._registry_lock:
                st = self._states.get(device_id)
                if st is None:
                    st = _DeviceState()
                    self._states[device_id] = st
        return st

    def check(self, device_id: str, seq: int) -> DedupDecision:
        """提交一个序号，返回判定（幂等：重复序号永远不会被接受第二次）。"""
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValueError(f"seq must be non-negative int, got {seq!r}")

        st = self._state(device_id)
        with st.lock:
            wm_before = st.watermark
            prev_seen = st.last_seen_seq
            st.last_seen_seq = seq

            if seq > st.watermark:
                # 前向推进（允许跳号），水位单调上升
                st.seen.add(seq)
                st.watermark = seq
                self._prune(st)
                return DedupDecision(
                    accepted=True,
                    watermark_before=wm_before,
                    watermark_after=st.watermark,
                )

            # seq <= watermark：落在当前水位及以前
            low = st.watermark - self.window_size + 1
            if seq < low:
                reason = (
                    RejectReason.SEQ_ROLLBACK
                    if seq < prev_seen
                    else RejectReason.OUT_OF_WINDOW
                )
                return DedupDecision(
                    accepted=False, reason=reason,
                    watermark_before=wm_before,
                    watermark_after=st.watermark,
                )

            if seq in st.seen:
                return DedupDecision(
                    accepted=False, reason=RejectReason.DUPLICATE_SEQ,
                    watermark_before=wm_before,
                    watermark_after=st.watermark,
                )

            # 窗口内乱序补缺：接受，但水位不移动（单调）
            st.seen.add(seq)
            return DedupDecision(
                accepted=True,
                watermark_before=wm_before,
                watermark_after=st.watermark,
            )

    def _prune(self, st: _DeviceState) -> None:
        """水位推进后淘汰窗口外成员，seen 大小被钳制在 O(window_size)。"""
        low = st.watermark - self.window_size + 1
        if low > 0:
            st.seen = {s for s in st.seen if s >= low}

    def watermark(self, device_id: str) -> int:
        """返回设备当前水位（无数据时为 -1）。"""
        return self._state(device_id).watermark
