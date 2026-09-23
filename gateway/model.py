"""统一读数模型与接入判定类型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RejectReason(Enum):
    """可区分的拒绝/标记原因。"""

    DECODE_ERROR = "decode_error"                # 帧本身无法解码（畸形）
    MISSING_FIELD = "missing_field"              # 必填字段缺失
    LENGTH_MISMATCH = "length_mismatch"          # 长度不符
    CHECKSUM_FAILED = "checksum_failed"          # 校验失败
    BAD_FIELD_TYPE = "bad_field_type"            # 字段类型非法
    DUPLICATE_SEQ = "duplicate_seq"              # 重复序号
    SEQ_ROLLBACK = "seq_rollback"                # 序号回退（低于已提交水位）
    OUT_OF_WINDOW = "out_of_window"              # 超出可接受迟到窗口


@dataclass(frozen=True, slots=True)
class Reading:
    """归一化后的统一读数模型。"""

    device_id: str
    seq: int
    metric: str
    value: float
    timestamp: float
    source: str  # "register" | "json"

    @property
    def key(self) -> tuple[str, int]:
        """(设备, 序号) 全局去重键。"""
        return (self.device_id, self.seq)


@dataclass(frozen=True, slots=True)
class AcceptResult:
    """单帧接入的判定结论。"""

    accepted: bool
    reading: Reading | None = None
    reason: RejectReason | None = None
    detail: str = ""
    watermark: int = -1
