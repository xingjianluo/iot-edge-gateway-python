"""结构化报文（JSON 文本）编解码。

报文（单行 JSON）::

    {"device":"dev-1","seq":3,"ts":1700000000123,
     "metric":"temperature","value":21.5}

必填字段：
    device : 非空 str
    seq    : int 且 >= 0（不接受 bool）
    ts     : int 且 >= 0
    metric : 非空 str
    value  : int/float（有限数，不接受 bool/NaN/Infinity）

拒绝原因：
    MALFORMED     —— 非合法 JSON 或顶层不是对象（不可归属：认不出它是
                     结构化报文）
    FIELD_MISSING —— 必填字段缺失 / 空字符串（归属 structured：已经能确认
                     它是一条结构化报文，只是内容坏了）
    BAD_VALUE     —— 字段类型或取值非法（归属 structured）

归属语义（供协议自行判别使用）：
    只要字节能解析成 JSON 对象，就足以确认它属于结构化编码；此后缺字段、
    空值、类型非法等结论都是"structured 坏帧"（attributable=True），
    而不是"认不出是什么协议"。对象里能安全读出的 device/seq 会作为
    evidence 一并带出，供现场排查定位。
"""

from __future__ import annotations

import json
import math

from ..models import DecodeResult, Reading, RejectReason

REQUIRED_FIELDS = ("device", "seq", "ts", "metric", "value")


def encode_structured(
    *,
    device_id: str,
    seq: int,
    timestamp_ms: int,
    metric: str,
    value: float,
) -> bytes:
    """构造一条结构化报文（UTF-8 JSON，无多余空白、无换行）。"""
    return json.dumps(
        {
            "device": device_id,
            "seq": seq,
            "ts": timestamp_ms,
            "metric": metric,
            "value": value,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _reject(reason: RejectReason, detail: str) -> DecodeResult:
    return DecodeResult(ok=False, reason=reason, detail=detail)


def _bad(reason: RejectReason, detail: str, evidence: dict) -> DecodeResult:
    """属于 structured 的坏帧：自动判别时按 structured 自己的结论上报。"""
    return DecodeResult(
        ok=False, reason=reason, detail=detail,
        attributable=True, protocol="structured",
        evidence=evidence or None,
    )


def _evidence_from(obj: dict) -> dict:
    """从 JSON 对象里尽力收集能定位的依据（只取类型确定的安全字段）。"""
    evidence: dict = {}
    device = obj.get("device")
    if isinstance(device, str) and device:
        evidence["device_id"] = device
    seq = obj.get("seq")
    if isinstance(seq, int) and not isinstance(seq, bool) and seq >= 0:
        evidence["seq"] = seq
    return evidence


class StructuredDecoder:
    """解码 JSON 结构化报文并归一为 Reading。"""

    name = "structured"

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        if isinstance(raw, (bytes, bytearray)):
            try:
                text = bytes(raw).decode("utf-8")
            except UnicodeDecodeError as exc:
                return _reject(
                    RejectReason.MALFORMED, f"payload not utf-8: {exc}",
                )
        else:
            text = raw

        try:
            obj = json.loads(text)
        except (json.JSONDecodeError, ValueError) as exc:
            return _reject(RejectReason.MALFORMED, f"invalid json: {exc}")

        if not isinstance(obj, dict):
            # 合法 JSON 但不是对象（数字/数组/字符串…）：不是本协议报文
            return _reject(
                RejectReason.MALFORMED,
                f"top-level must be object, got {type(obj).__name__}",
            )

        # 到这里已能确认它是一条结构化报文，以下失败都归属 structured
        evidence = _evidence_from(obj)
        for key in REQUIRED_FIELDS:
            if key not in obj:
                return _bad(
                    RejectReason.FIELD_MISSING,
                    f"missing field: {key!r}", evidence,
                )

        device = obj["device"]
        metric = obj["metric"]
        if not isinstance(device, str) or not device:
            return _bad(
                RejectReason.BAD_VALUE,
                f"device must be non-empty string, got {device!r}", evidence,
            )
        evidence["device_id"] = device
        if not isinstance(metric, str) or not metric:
            return _bad(
                RejectReason.BAD_VALUE,
                f"metric must be non-empty string, got {metric!r}", evidence,
            )

        seq = obj["seq"]
        ts = obj["ts"]
        # bool 是 int 子类，必须显式排除
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            return _bad(
                RejectReason.BAD_VALUE, f"seq must be uint, got {seq!r}",
                evidence,
            )
        evidence["seq"] = seq
        if isinstance(ts, bool) or not isinstance(ts, int) or ts < 0:
            return _bad(
                RejectReason.BAD_VALUE, f"ts must be uint(ms), got {ts!r}",
                evidence,
            )

        value = obj["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return _bad(
                RejectReason.BAD_VALUE,
                f"value must be number, got {value!r}", evidence,
            )
        fv = float(value)
        if not math.isfinite(fv):
            return _bad(
                RejectReason.BAD_VALUE, f"value must be finite, got {value!r}",
                evidence,
            )

        return DecodeResult(
            ok=True,
            reading=Reading(
                device_id=device,
                seq=seq,
                timestamp_ms=ts,
                metric=metric,
                value=fv,
                source=self.name,
            ),
        )
