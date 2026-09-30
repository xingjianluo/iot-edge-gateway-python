"""统一读数模型与判定结果类型定义。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RejectReason(str, Enum):
    """拒绝原因（协议解析与序号语义两阶段共用，取值必须可区分）。"""

    # --- 协议解析阶段 ---
    TRUNCATED_FRAME = "TRUNCATED_FRAME"          # 长度不符 / 帧被截断
    BAD_MAGIC = "BAD_MAGIC"                      # 帧头魔数错误
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"    # 校验失败
    FIELD_MISSING = "FIELD_MISSING"              # 必填字段缺失
    BAD_VALUE = "BAD_VALUE"                      # 字段类型/取值非法
    MALFORMED = "MALFORMED"                      # 结构性报文无法解析
    UNKNOWN_PROTOCOL = "UNKNOWN_PROTOCOL"        # 自动识别：没有任何协议匹配
    AMBIGUOUS_PROTOCOL = "AMBIGUOUS_PROTOCOL"    # 自动识别：多种协议同时匹配

    # --- 序号/水位阶段 ---
    DUPLICATE_SEQ = "DUPLICATE_SEQ"              # 重复序号（窗口内）
    OUT_OF_WINDOW = "OUT_OF_WINDOW"              # 迟到数据，超出可接受窗口
    SEQ_ROLLBACK = "SEQ_ROLLBACK"                # 序号回退
    BUFFER_OVERFLOW = "BUFFER_OVERFLOW"          # 边界缓冲溢出


@dataclass(frozen=True, slots=True)
class Reading:
    """归一化后的统一读数。"""

    device_id: str
    seq: int
    timestamp_ms: int
    metric: str
    value: float
    source: str  # "register" | "structured"


@dataclass(frozen=True, slots=True)
class DecodeResult:
    """协议解码结果：成功携带 reading，失败携带可区分原因。

    自动判别场景下，失败结果还需回答两个问题：
        attributable  这串字节是否"足以确认属于本协议，只是帧坏了"。
                      True  -> 自动判别应给出本协议自己的坏帧结论；
                      False -> 只是"不像本协议"，不能据此归属。
        protocol      归属协议名（attributed failure 时由识别器补上）。
        evidence      能从坏帧里尽力读出的依据（device_id/seq 等），
                      供日志与排查使用；读不出的字段不出现在字典里。
    """

    ok: bool
    reading: Reading | None = None
    reason: RejectReason | None = None
    detail: str = ""
    attributable: bool = False
    protocol: str | None = None
    evidence: dict | None = None


@dataclass(frozen=True, slots=True)
class DedupDecision:
    """设备时序判定结果。"""

    accepted: bool
    reason: RejectReason | None = None
    watermark_before: int = 0
    watermark_after: int = 0


@dataclass
class BufferStats:
    """边界缓冲与转发统计（可查询）。"""

    capacity: int = 0
    size: int = 0
    enqueued: int = 0
    dropped_overflow: int = 0
    delivered: int = 0
    duplicate_delivery: int = 0
    downstream_failures: int = 0
    flushes: int = 0
