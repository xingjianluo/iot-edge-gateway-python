"""协议解码器接口定义。"""

from __future__ import annotations

from typing import Protocol

from ..models import DecodeResult


class ProtocolDecoder(Protocol):
    """所有协议解码器统一返回 DecodeResult，绝不静默丢弃。"""

    name: str

    def sniff(self, raw: bytes | bytearray | str) -> bool:
        """仅凭结构签名判断报文是否"像"本协议（不解码、不校验内容）。

        自动识别阶段对每个解码器调用 sniff：
            恰好一个命中 -> 交给该解码器正式解码；
            0 个命中     -> UNRECOGNIZED_PROTOCOL；
            多于 1 个    -> AMBIGUOUS_PROTOCOL。
        因此 sniff 必须保守（只认强特征），不同协议的签名集合不得相交。
        """
        ...

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        """解码一帧/一条报文；失败必须返回带 RejectReason 的结果。"""
        ...
