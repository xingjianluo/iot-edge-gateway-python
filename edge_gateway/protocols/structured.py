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
    MALFORMED     —— 非合法 JSON 或顶层不是对象
    FIELD_MISSING —— 必填字段缺失 / 空字符串
    BAD_VALUE     —— 字段类型或取值非法
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


class StructuredDecoder:
    """解码 JSON 结构化报文并归一为 Reading。"""

    name = "structured"

    def sniff(self, raw: bytes | bytearray | str) -> bool:
        """结构签名：可解为 UTF-8 文本，首个非空白字符为 '{'。"""
        if isinstance(raw, (bytes, bytearray)):
            try:
                text = bytes(raw).decode("utf-8")
            except UnicodeDecodeError:
                return False
        else:
            text = raw
        return bool(text.lstrip()[:1] == "{")

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
            return _reject(
                RejectReason.MALFORMED,
                f"top-level must be object, got {type(obj).__name__}",
            )

        for key in REQUIRED_FIELDS:
            if key not in obj:
                return _reject(
                    RejectReason.FIELD_MISSING, f"missing field: {key!r}",
                )

        device = obj["device"]
        metric = obj["metric"]
        if not isinstance(device, str) or not device:
            return _reject(
                RejectReason.BAD_VALUE,
                f"device must be non-empty string, got {device!r}",
            )
        if not isinstance(metric, str) or not metric:
            return _reject(
                RejectReason.BAD_VALUE,
                f"metric must be non-empty string, got {metric!r}",
            )

        seq = obj["seq"]
        ts = obj["ts"]
        # bool 是 int 子类，必须显式排除
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            return _reject(
                RejectReason.BAD_VALUE, f"seq must be uint, got {seq!r}",
            )
        if isinstance(ts, bool) or not isinstance(ts, int) or ts < 0:
            return _reject(
                RejectReason.BAD_VALUE, f"ts must be uint(ms), got {ts!r}",
            )

        value = obj["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return _reject(
                RejectReason.BAD_VALUE,
                f"value must be number, got {value!r}",
            )
        fv = float(value)
        if not math.isfinite(fv):
            return _reject(
                RejectReason.BAD_VALUE, f"value must be finite, got {value!r}",
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
