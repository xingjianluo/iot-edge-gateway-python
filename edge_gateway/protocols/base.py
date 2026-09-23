"""协议解码器接口定义。"""

from __future__ import annotations

from typing import Protocol

from ..models import DecodeResult


class ProtocolDecoder(Protocol):
    """所有协议解码器统一返回 DecodeResult，绝不静默丢弃。"""

    name: str

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        """解码一帧/一条报文；失败必须返回带 RejectReason 的结果。"""
        ...
