"""协议自行判别：设备只给原始字节时，自动识别编码并归一为 Reading。

使用场景：上游设备不再声明自己的协议类型，只给一段原始字节，网关自行
识别并归一到同一套读数模型。

判定原则（绝不瞎猜、绝不静默丢弃）：
    恰好一种协议解码成功    -> 接受，Reading.source 标明命中协议
    仅一种协议"认得出但帧坏" -> 给出该协议自己的、可区分的坏帧结论
                               （CHECKSUM_MISMATCH/TRUNCATED_FRAME/
                               FIELD_MISSING/BAD_VALUE/MALFORMED 等），
                               并带上能从坏帧读出的 device_id/seq 等依据；
                               绝不一概回成"认不出"
    没有任何协议认得出      -> UNKNOWN_PROTOCOL（"我们不认识这是什么"，
                               与某协议的坏帧是两回事，分开计数、分开日志）
    两种及以上同时成功，或
      两种及以上同时认得出   -> AMBIGUOUS_PROTOCOL（歧义：硬塞任何一种都
      （但都坏了）             可能错，必须明确拒绝），与"认不出"分开计数

"认得出"的标准由各解码器用 DecodeResult.attributable 声明：
    register  —— magic 0xA55A 对上即归属（此后版本/长度/CRC/字段问题都是
                 register 坏帧）；magic 不符或短于 2 字节则不归属。
    structured —— 字节能解析成 JSON 对象即归属（此后缺字段/非法值都是
                 structured 坏帧）；非 JSON 或顶层不是对象则不归属。

为让结论可解释，detail 中固定携带：参与尝试的协议、各自的拒绝原因、
命中/归属数量。
"""

from __future__ import annotations

from dataclasses import replace

from ..models import DecodeResult, RejectReason
from .register import RegisterFrameDecoder
from .structured import StructuredDecoder

# RawMessage.protocol 取该值时走自动识别；其它取值维持"指定协议解析"老路
AUTO_PROTOCOL = "auto"


def default_decoders():
    """网关默认注册的解码器（顺序固定，保证判定可复现）。"""
    return [RegisterFrameDecoder(), StructuredDecoder()]


class ProtocolIdentifier:
    """依次尝试已注册解码器，按"唯一命中/唯一归属"原则判定。"""

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
        attributed: list[tuple[str, DecodeResult]] = []
        failures: list[tuple[str, RejectReason | None, str]] = []
        for decoder in self._decoders:
            result = decoder.decode(raw)
            if result.ok and result.reading is not None:
                hits.append((decoder.name, result.reading))
            elif result.attributable and result.reason is not None:
                name = result.protocol or decoder.name
                attributed.append((name, result))
                failures.append((name, result.reason, result.detail))
            else:
                failures.append(
                    (decoder.name, result.reason, result.detail),
                )

        tried = ", ".join(d.name for d in self._decoders)
        breakdown = "; ".join(
            f"{name}: {(reason.value if reason else 'UNKNOWN')}({detail})"
            for name, reason, detail in failures
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
        if len(hits) == 1:
            name, reading = hits[0]
            return DecodeResult(
                ok=True, reading=reading,
                detail=f"protocol identified as {name!r} (tried: {tried})",
            )

        if len(attributed) >= 2:
            names = ", ".join(name for name, _ in attributed)
            return DecodeResult(
                ok=False, reason=RejectReason.AMBIGUOUS_PROTOCOL,
                detail=(
                    f"payload looks like a corrupt frame of "
                    f"{len(attributed)} protocols at once: [{names}]; "
                    f"cannot attribute safely (tried: {tried}); "
                    f"per-decoder result -> {breakdown}"
                ),
            )
        if len(attributed) == 1:
            name, result = attributed[0]
            return replace(
                result,
                detail=(
                    f"corrupt {name!r} frame under auto-detection "
                    f"(tried: {tried}): {result.detail}"
                ),
            )

        return DecodeResult(
            ok=False, reason=RejectReason.UNKNOWN_PROTOCOL,
            detail=(
                f"no known protocol matched (tried: {tried}); "
                f"per-decoder result -> {breakdown}"
            ),
        )
