"""协议自行判别：设备只给原始字节时，自动识别编码并归一为 Reading。

使用场景：上游设备不再声明自己的协议类型，只给一段原始字节，网关自行
识别并归一到同一套读数模型。

判定原则（绝不瞎猜、绝不静默丢弃）：
    每台解码器先用 recognize() 回答"这段字节与本协议的归属"：
        MATCH     属于本协议且完整解码成功
        CORRUPT   能确认属于本协议（帧头/结构对得上），但帧本身坏了
                  （校验失败/被截断/必填字段缺失/取值非法等）
        UNRELATED 不属于本协议
    汇总后：
        恰好 1 个 MATCH、无其它认领  -> 接受，Reading.source 标明命中协议
        恰好 1 个认领（MATCH/CORRUPT）-> 若是 CORRUPT，按该协议自己的、
                  可区分的帧级原因拒绝（CHECKSUM_MISMATCH / TRUNCATED_FRAME /
                  FIELD_MISSING / BAD_VALUE / MALFORMED），并带上从字节里
                  实际读出的 device_id / seq 等依据
        没有任何解码器认领            -> UNKNOWN_PROTOCOL（"认不出是什么"）
        2 个及以上认领                -> AMBIGUOUS_PROTOCOL（歧义，绝不硬挑
                  一种塞进去）

与指定协议老路的差别仅在"归属"这一步：老路不做归属判断，严格按设备声明
的协议解码；归属一旦确认，自动路径给出的帧级结论与老路完全一致。
"""

from __future__ import annotations

from ..models import (
    CORRUPT,
    MATCH,
    UNRELATED,
    DecodeResult,
    RejectReason,
)
from .register import RegisterFrameDecoder
from .structured import StructuredDecoder

# RawMessage.protocol 取该值时走自动识别；其它取值维持"指定协议解析"老路
AUTO_PROTOCOL = "auto"


def default_decoders():
    """网关默认注册的解码器（顺序固定，保证判定可复现）。"""
    return [RegisterFrameDecoder(), StructuredDecoder()]


def _fallback_recognize(decoder, raw) -> object:
    """没有 recognize() 的解码器（如测试桩）：退化为 decode 推断归属。

    decode 成功 -> MATCH；BAD_MAGIC 视为"不像本协议"（UNRELATED），
    其余帧级失败一律视为"像本协议但帧坏了"（CORRUPT）。
    """
    from ..models import Recognition

    result = decoder.decode(raw)
    if result.ok:
        kind = MATCH
    elif result.reason is RejectReason.BAD_MAGIC:
        kind = UNRELATED
    else:
        kind = CORRUPT
    return Recognition(kind=kind, result=result)


class ProtocolIdentifier:
    """依次让已注册解码器做归属判断，按"唯一认领"原则判定。"""

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

        tried = ", ".join(d.name for d in self._decoders)
        claims: list = []        # (name, recognition)
        unrelated: list = []     # (name, reason, detail)
        for decoder in self._decoders:
            recognize = getattr(decoder, "recognize", None)
            rec = (
                recognize(raw)
                if recognize is not None
                else _fallback_recognize(decoder, raw)
            )
            if rec.kind is UNRELATED:
                unrelated.append(
                    (decoder.name, rec.result.reason, rec.result.detail),
                )
            else:
                claims.append((decoder.name, rec))

        matches = [
            (name, rec) for name, rec in claims if rec.kind is MATCH
        ]
        corrupt = [
            (name, rec) for name, rec in claims if rec.kind is CORRUPT
        ]

        # 1) 唯一 MATCH 且没有其它认领：接受
        if len(matches) == 1 and not corrupt:
            name, rec = matches[0]
            return DecodeResult(
                ok=True, reading=rec.result.reading,
                protocol=name,
                detail=(
                    f"protocol identified as {name!r} (tried: {tried})"
                ),
            )

        # 2) 两个及以上协议同时认领（无论 MATCH 还是 CORRUPT）：歧义
        if len(claims) >= 2:
            names = ", ".join(
                f"{name}({rec.kind})" for name, rec in claims
            )
            return DecodeResult(
                ok=False, reason=RejectReason.AMBIGUOUS_PROTOCOL,
                protocol="",
                detail=(
                    f"payload is claimed by {len(claims)} protocols at once: "
                    f"[{names}]; cannot pick one safely (tried: {tried})"
                ),
            )

        # 3) 恰好一个 CORRUPT 认领：这是该协议的坏帧，给出它自己的结论
        if len(corrupt) == 1:
            name, rec = corrupt[0]
            inner = rec.result
            evidence = rec.evidence or {}
            ev = ", ".join(f"{k}={v!r}" for k, v in evidence.items())
            ev_part = f"; evidence from bytes: {ev}" if ev else ""
            return DecodeResult(
                ok=False,
                reason=inner.reason or RejectReason.MALFORMED,
                protocol=name,
                evidence=dict(evidence),
                detail=(
                    f"recognized as {name!r} frame but it is corrupt: "
                    f"{inner.reason.value if inner.reason else 'MALFORMED'}"
                    f"({inner.detail}){ev_part} (tried: {tried})"
                ),
            )

        # 4) 无人认领：确实认不出
        breakdown = "; ".join(
            f"{name}: {(reason.value if reason else 'UNKNOWN')}({detail})"
            for name, reason, detail in unrelated
        )
        return DecodeResult(
            ok=False, reason=RejectReason.UNKNOWN_PROTOCOL,
            detail=(
                f"no known protocol matched (tried: {tried}); "
                f"per-decoder result -> {breakdown}"
            ),
        )
