"""协议自行判别：设备只给原始字节时，自动识别编码并归一为 Reading。

使用场景：上游设备不再声明自己的协议类型，只给一段原始字节，网关自行
识别并归一到同一套读数模型。

判定原则（绝不瞎猜、绝不静默丢弃）：
    恰好一种协议解码成功  -> 接受，Reading.source 标明命中协议
    没有任何协议成功      -> UNKNOWN_PROTOCOL（结论与各类帧级拒绝不同，
                            即"我们认不出这是什么"，而非某协议的坏帧）
    两种及以上同时成功    -> AMBIGUOUS_PROTOCOL（歧义，硬塞任何一种都可能
                            错，必须由明确拒绝暴露出来）

为让结论可解释，detail 中固定携带：参与尝试的协议、各自的拒绝原因、
命中数量。
"""

from __future__ import annotations

from ..models import DecodeResult, RejectReason
from .register import RegisterFrameDecoder
from .structured import StructuredDecoder

# RawMessage.protocol 取该值时走自动识别；其它取值维持"指定协议解析"老路
AUTO_PROTOCOL = "auto"


def default_decoders():
    """网关默认注册的解码器（顺序固定，保证判定可复现）。"""
    return [RegisterFrameDecoder(), StructuredDecoder()]


class ProtocolIdentifier:
    """依次尝试已注册解码器，按"唯一命中"原则判定。"""

    def __init__(self, decoders) -> None:
        if not decoders:
            raise ValueError("ProtocolIdentifier needs at least one decoder")
        self._decoders = list(decoders)

    def identify(self, raw: bytes | bytearray | str) -> DecodeResult:
        """对一段原始字节做协议自行判别。"""
        if not isinstance(raw, (bytes, bytearray)):
            return DecodeResult(
                ok=False, reason=RejectReason.MALFORMED,
                detail=(
                    "auto-detection requires raw bytes, got "
                    f"{type(raw).__name__}"
                ),
            )

        hits: list = []
        failures: list[tuple[str, RejectReason | None, str]] = []
        for decoder in self._decoders:
            result = decoder.decode(raw)
            if result.ok and result.reading is not None:
                hits.append((decoder.name, result.reading))
            else:
                failures.append(
                    (decoder.name, result.reason, result.detail),
                )

        tried = ", ".join(d.name for d in self._decoders)
        if len(hits) == 1:
            name, reading = hits[0]
            return DecodeResult(
                ok=True, reading=reading,
                detail=f"protocol identified as {name!r} (tried: {tried})",
            )
        if len(hits) >= 2:
            names = ", ".join(name for name, _ in hits)
            return DecodeResult(
                ok=False, reason=RejectReason.AMBIGUOUS_PROTOCOL,
                detail=(
                    f"payload matches {len(hits)} protocols at once: "
                    f"[{names}]; cannot pick one safely (tried: {tried})"
                ),
            )

        breakdown = "; ".join(
            f"{name}: {(reason.value if reason else 'UNKNOWN')}"
            f"({detail})"
            for name, reason, detail in failures
        )
        return DecodeResult(
            ok=False, reason=RejectReason.UNKNOWN_PROTOCOL,
            detail=(
                f"no known protocol matched (tried: {tried}); "
                f"per-decoder result -> {breakdown}"
            ),
        )
