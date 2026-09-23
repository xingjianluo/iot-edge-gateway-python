"""结构化报文（JSON 文本行）编解码（实现待填充）。

报文示例::

    {"device":"dev-1","seq":3,"ts":1700000000123,
     "metric":"temperature","value":21.5}

必填字段：device(str)、seq(int>=0)、ts(int>=0)、metric(str)、value(number)
"""

from __future__ import annotations

import json

from ..models import DecodeResult, Reading

REQUIRED_FIELDS = ("device", "seq", "ts", "metric", "value")


def encode_structured(
    *,
    device_id: str,
    seq: int,
    timestamp_ms: int,
    metric: str,
    value: float,
) -> bytes:
    """构造一条结构化报文（UTF-8 JSON，无换行）。骨架阶段占位。"""
    return b""


class StructuredDecoder:
    """解码 JSON 结构化报文。"""

    name = "structured"

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        """解码并归一为 Reading；骨架占位。"""
        return DecodeResult(ok=False)
