"""协议自行判别：设备侧只给原始字节，由网关识别编码。

规则（确定性，绝不瞎猜、绝不静默丢弃）：
    对所有已注册解码器调用 sniff 做结构签名匹配：
      恰好一个命中 -> 判定为该协议（正式 decode 仍可能失败并返回明确原因）；
      0 个命中     -> UNRECOGNIZED_PROTOCOL；
      多于 1 个    -> AMBIGUOUS_PROTOCOL（detail 中列出全部候选）。
    "已知协议、直接按指定协议解析"的老路径不经过本模块。
"""

from __future__ import annotations

from dataclasses import dataclass

from ..models import RejectReason
from .base import ProtocolDecoder
from .register import RegisterFrameDecoder
from .structured import StructuredDecoder


@dataclass(frozen=True, slots=True)
class DetectResult:
    """自动识别结论：成功携带协议名，失败携带可区分原因。"""

    ok: bool
    protocol: str | None = None
    reason: RejectReason | None = None
    detail: str = ""


class ProtocolDetector:
    """对一组解码器做保守签名匹配。"""

    def __init__(self, decoders: list[ProtocolDecoder] | None = None) -> None:
        self._decoders = list(decoders) if decoders is not None else [
            RegisterFrameDecoder(), StructuredDecoder(),
        ]

    def detect(self, raw: bytes | bytearray | str) -> DetectResult:
        matches = [d for d in self._decoders if d.sniff(raw)]
        if len(matches) == 1:
            return DetectResult(ok=True, protocol=matches[0].name)
        if not matches:
            preview = _preview(raw)
            return DetectResult(
                ok=False, reason=RejectReason.UNRECOGNIZED_PROTOCOL,
                detail=f"no protocol signature matched: {preview}",
            )
        names = [d.name for d in matches]
        return DetectResult(
            ok=False, reason=RejectReason.AMBIGUOUS_PROTOCOL,
            detail=f"multiple protocol signatures matched: {names}",
        )


def _preview(raw: bytes | bytearray | str, limit: int = 16) -> str:
    """生成可放入日志的报文预览（十六进制 + 长度），不依赖编码是否合法。"""
    if isinstance(raw, str):
        data = raw.encode("utf-8", errors="replace")
    else:
        data = bytes(raw)
    shown = data[:limit].hex(" ")
    if len(data) > limit:
        shown += " ..."
    return f"len={len(data)} bytes={shown}"
