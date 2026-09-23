"""寄存器风格二进制帧编解码（实现待填充）。

帧格式（小端，所有多字节整数 little-endian）::

    偏移  长度  字段
    0     2     magic 0xA55A
    2     1     version = 0x01
    3     1     flags（bit0: 有符号值）
    4     2     device_id_len N
    6     N     device_id（ASCII）
    6+N   4     seq uint32
    10+N  8     timestamp_ms uint64
    18+N  2     metric_len M
    20+N  M     metric（ASCII）
    20+N+M 4    int32 原始寄存器值（由 scale 转 float，scale=100）
    24+N+M 2    CRC16/MODBUS，覆盖此前全部字节
"""

from __future__ import annotations

from ..models import DecodeResult, Reading

MAGIC = 0xA55A
VERSION = 0x01
VALUE_SCALE = 100.0
HEADER_FIXED = 6  # magic+version+flags+devid_len
TAIL_FIXED = 2  # crc


def crc16_modbus(data: bytes, crc: int = 0xFFFF) -> int:
    """CRC16/MODBUS。骨架阶段占位。"""
    return 0


def encode_register_frame(
    *,
    device_id: str,
    seq: int,
    timestamp_ms: int,
    metric: str,
    value: float,
    signed: bool = False,
    bad_crc: bool = False,
) -> bytes:
    """构造一帧寄存器风格报文（测试用）。骨架阶段占位。"""
    return b""


class RegisterFrameDecoder:
    """解码寄存器风格帧。"""

    name = "register"

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        """解码并归一为 Reading；骨架占位。"""
        return DecodeResult(ok=False)
