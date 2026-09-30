"""协议解码器接口定义。"""

from __future__ import annotations

from typing import Protocol

from ..models import DecodeResult, Recognition


class ProtocolDecoder(Protocol):
    """所有协议解码器统一返回 DecodeResult，绝不静默丢弃。"""

    name: str

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        """解码一帧/一条报文；失败必须返回带 RejectReason 的结果。"""
        ...

    def recognize(self, raw: bytes | bytearray | str) -> Recognition:
        """自动识别用：判断字节与本协议的归属（MATCH/CORRUPT/UNRELATED）。

        与 decode 的区别：decode 回答"按本协议解这帧，成不成、为什么"；
        recognize 还要回答"这段字节到底像不像本协议"——帧头/结构对得上、
        只是内容坏了时必须认领为 CORRUPT，而不是把它推给"认不出"。
        """
        ...
