"""按设备序号的幂等去重、乱序窗口与单调水位。

语义（单设备）：
- ``watermark``：已连续提交的最大序号，初始 -1 表示尚未建立基线；只增不减。
- 首帧建立基线：首帧序号 N 到达后，水位基线为 N-1，N 成为第一个连续提交点；
  更早到达的序号一律按序号回退拒绝（基线之前的数据不再受理）。
- 窗口 ``W``：相对下一个期望序号 ``watermark+1``，最多容忍超前 W，
  即可接受区间为 ``[watermark+1, watermark+1+W]``；超出即 OUT_OF_WINDOW。
- ``seq < watermark`` 为序号回退 SEQ_ROLLBACK；``seq == watermark`` 或该序号
  已在乱序待提交集合中为重复 DUPLICATE_SEQ；二者都不重复计数。
- commit 只在序号连续时推进水位，并返回本次新连续就绪的序号（按序）。
"""
from __future__ import annotations

import threading

from gateway.model import RejectReason


class DeviceWatermarkState:
    """单设备序号状态（非线程安全，由上层管理器加锁）。"""

    def __init__(self, window: int) -> None:
        if window < 0:
            raise ValueError("window 必须非负")
        self.window = window
        self.watermark: int = -1
        self._pending: set[int] = set()

    @property
    def pending_seqs(self) -> frozenset[int]:
        return frozenset(self._pending)

    def inspect(
        self, seq: int
    ) -> tuple[bool, RejectReason | None, str, int]:
        """判定序号是否可接收，不改变状态。返回 (接受?, 原因, 说明, 当前水位)。"""
        if seq < 0:
            return (
                False,
                RejectReason.BAD_FIELD_TYPE,
                f"序号不能为负：{seq}",
                self.watermark,
            )

        if self.watermark == -1:
            # 首帧：建立基线，接受任意非负序号
            return True, None, f"首帧建立基线，接受序号 {seq}", self.watermark

        if seq < self.watermark:
            return (
                False,
                RejectReason.SEQ_ROLLBACK,
                f"序号回退：seq={seq} < 水位={self.watermark}",
                self.watermark,
            )
        if seq == self.watermark:
            return (
                False,
                RejectReason.DUPLICATE_SEQ,
                f"重复序号：seq={seq} 等于水位={self.watermark}（已计数）",
                self.watermark,
            )
        if seq in self._pending:
            return (
                False,
                RejectReason.DUPLICATE_SEQ,
                f"重复序号：seq={seq} 已在乱序待提交集合中",
                self.watermark,
            )
        upper = self.watermark + 1 + self.window
        if seq > upper:
            return (
                False,
                RejectReason.OUT_OF_WINDOW,
                f"超出窗口：seq={seq} > 可接受上界={upper}（水位={self.watermark}，"
                f"窗口 W={self.window}）",
                self.watermark,
            )
        return (
            True,
            None,
            f"窗口内接受：seq={seq}（水位={self.watermark}，上界={upper}）",
            self.watermark,
        )

    def commit(self, seq: int) -> list[int]:
        """登记已通过判定的序号，推进水位，返回本次新连续就绪的序号（升序）。"""
        if self.watermark == -1:
            # 首帧建立基线
            self.watermark = seq - 1
        self._pending.add(seq)
        advanced: list[int] = []
        while (self.watermark + 1) in self._pending:
            nxt = self.watermark + 1
            self._pending.discard(nxt)
            self.watermark = nxt
            advanced.append(nxt)
        return advanced


class DeviceStateRegistry:
    """按 device_id 管理状态；一把全局锁保证并发接入下水印与计数不错乱。"""

    def __init__(self, window: int) -> None:
        self._window = window
        self._states: dict[str, DeviceWatermarkState] = {}
        self._lock = threading.RLock()

    def get(self, device_id: str) -> DeviceWatermarkState:
        with self._lock:
            st = self._states.get(device_id)
            if st is None:
                st = DeviceWatermarkState(self._window)
                self._states[device_id] = st
            return st

    def watermark_of(self, device_id: str) -> int:
        with self._lock:
            st = self._states.get(device_id)
            return st.watermark if st is not None else -1
