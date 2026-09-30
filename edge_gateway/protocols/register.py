"""寄存器风格二进制帧编解码。

帧格式（所有多字节整数 little-endian）::

    偏移     长度  字段
    0        2     magic 0xA55A（线上字节 5A A5）
    2        1     version = 0x01
    3        1     flags（bit0=1 表示 int32 寄存器值按有符号解析）
    4        2     device_id_len N
    6        N     device_id（ASCII）
    6+N      4     seq uint32
    10+N     8     timestamp_ms uint64
    18+N     2     metric_len M
    20+N     M     metric（ASCII）
    20+N+M   4     int32 原始寄存器值（除以 VALUE_SCALE 还原 float）
    24+N+M   2     CRC16/MODBUS，覆盖此前全部字节

合法帧总长 = 26 + N + M。

拒绝判定顺序（确定性，便于复现）：
    1) 不足 2 字节 / 头不足 / 总长不符 -> TRUNCATED_FRAME
    2) magic 不符                       -> BAD_MAGIC
    3) 版本不符                         -> MALFORMED
    4) CRC 不符                         -> CHECKSUM_MISMATCH
    5) 字段为空 / 非 ASCII              -> FIELD_MISSING / BAD_VALUE
"""

from __future__ import annotations

import struct

from ..models import (
    CORRUPT,
    MATCH,
    UNRELATED,
    DecodeResult,
    Reading,
    Recognition,
    RejectReason,
)

MAGIC = 0xA55A
VERSION = 0x01
VALUE_SCALE = 100.0
FLAG_SIGNED = 0x01

_MIN_LEN = 26  # 6 头 + 4 seq + 8 ts + 2 metric_len + 4 value + 2 crc


def crc16_modbus(data: bytes, crc: int = 0xFFFF) -> int:
    """标准 CRC16/MODBUS（多项式 0xA001 反射形式）。"""
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


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
    """构造一帧寄存器风格报文（测试/模拟器用）。

    bad_crc=True 时写入取反的 CRC，用于制造可区分的校验失败帧。
    """
    dev = device_id.encode("ascii")
    met = metric.encode("ascii")
    flags = FLAG_SIGNED if signed else 0
    raw_val = int(round(value * VALUE_SCALE))
    body = struct.pack(
        "<HBBH", MAGIC, VERSION, flags, len(dev),
    )
    body += dev
    body += struct.pack("<IQH", seq, timestamp_ms, len(met))
    body += met
    body += struct.pack("<i" if signed else "<I", raw_val)
    crc = crc16_modbus(body)
    if bad_crc:
        crc ^= 0xFFFF
    return body + struct.pack("<H", crc)


class RegisterFrameDecoder:
    """解码寄存器风格帧并归一为 Reading。"""

    name = "register"

    def decode(self, raw: bytes | bytearray | str) -> DecodeResult:
        if isinstance(raw, str):
            return DecodeResult(
                ok=False, reason=RejectReason.MALFORMED,
                detail="register frame must be bytes, got str",
            )
        data = bytes(raw)
        n = len(data)

        if n < 2:
            return DecodeResult(
                ok=False, reason=RejectReason.TRUNCATED_FRAME,
                detail=f"frame shorter than magic: {n} bytes",
            )
        if struct.unpack_from("<H", data, 0)[0] != MAGIC:
            return DecodeResult(
                ok=False, reason=RejectReason.BAD_MAGIC,
                detail=f"bad magic: 0x{struct.unpack_from('<H', data, 0)[0]:04X}",
            )
        if n < 6:
            return DecodeResult(
                ok=False, reason=RejectReason.TRUNCATED_FRAME,
                detail=f"header incomplete: {n} bytes < 6",
            )

        version, flags, dev_len = struct.unpack_from("<BBH", data, 2)
        if version != VERSION:
            return DecodeResult(
                ok=False, reason=RejectReason.MALFORMED,
                detail=f"unsupported version: 0x{version:02X}",
            )

        expected = _MIN_LEN + dev_len
        if n < expected:
            return DecodeResult(
                ok=False, reason=RejectReason.TRUNCATED_FRAME,
                detail=f"length mismatch: need metric_len/value/crc, "
                       f"got {n} < {expected} (dev_len={dev_len})",
            )

        off = 6 + dev_len
        seq, ts, met_len = struct.unpack_from("<IQH", data, off)
        expected = 26 + dev_len + met_len
        if n != expected:
            return DecodeResult(
                ok=False, reason=RejectReason.TRUNCATED_FRAME,
                detail=f"length mismatch: got {n} != {expected} "
                       f"(dev_len={dev_len}, metric_len={met_len})",
            )

        crc_want = crc16_modbus(data[:-2])
        crc_got = struct.unpack_from("<H", data, n - 2)[0]
        if crc_got != crc_want:
            return DecodeResult(
                ok=False, reason=RejectReason.CHECKSUM_MISMATCH,
                detail=f"crc16 mismatch: frame=0x{crc_got:04X} "
                       f"calc=0x{crc_want:04X}",
            )

        try:
            device_id = data[6:6 + dev_len].decode("ascii")
        except UnicodeDecodeError as exc:
            return DecodeResult(
                ok=False, reason=RejectReason.BAD_VALUE,
                detail=f"device_id not ascii: {exc}",
            )
        metric_off = off + 14  # seq(4)+ts(8)+mlen(2)
        try:
            metric = data[metric_off:metric_off + met_len].decode("ascii")
        except UnicodeDecodeError as exc:
            return DecodeResult(
                ok=False, reason=RejectReason.BAD_VALUE,
                detail=f"metric not ascii: {exc}",
            )

        if not device_id:
            return DecodeResult(
                ok=False, reason=RejectReason.FIELD_MISSING,
                detail="empty device_id",
            )
        if not metric:
            return DecodeResult(
                ok=False, reason=RejectReason.FIELD_MISSING,
                detail=f"empty metric (device={device_id}, seq={seq})",
            )

        val_off = metric_off + met_len
        if flags & FLAG_SIGNED:
            raw_val = struct.unpack_from("<i", data, val_off)[0]
        else:
            raw_val = struct.unpack_from("<I", data, val_off)[0]

        reading = Reading(
            device_id=device_id,
            seq=seq,
            timestamp_ms=ts,
            metric=metric,
            value=raw_val / VALUE_SCALE,
            source=self.name,
        )
        return DecodeResult(ok=True, reading=reading)

    def recognize(self, raw: bytes | bytearray | str) -> Recognition:
        """自动识别用：判断这段字节与 register 帧的归属关系。

        归属判据是帧头魔数：
            头不足 / 魔数不对 -> UNRELATED（它就不是 register 帧）；
            魔数对但帧坏了   -> CORRUPT，携带 register 自己的可区分拒绝，
                               并尽量带出从字节里读到的 device_id / seq；
            魔数对且完整合法 -> MATCH。
        """
        if isinstance(raw, str):
            return Recognition(
                kind=UNRELATED,
                result=DecodeResult(
                    ok=False, reason=RejectReason.MALFORMED,
                    detail="register frame must be bytes, got str",
                ),
            )
        data = bytes(raw)
        n = len(data)
        if n < 2:
            return Recognition(
                kind=UNRELATED,
                result=DecodeResult(
                    ok=False, reason=RejectReason.TRUNCATED_FRAME,
                    detail=f"frame shorter than magic: {n} bytes",
                ),
            )
        if struct.unpack_from("<H", data, 0)[0] != MAGIC:
            return Recognition(
                kind=UNRELATED,
                result=DecodeResult(
                    ok=False, reason=RejectReason.BAD_MAGIC,
                    detail=(
                        f"bad magic: 0x"
                        f"{struct.unpack_from('<H', data, 0)[0]:04X}"
                    ),
                ),
            )
        result = self.decode(data)
        return Recognition(
            kind=MATCH if result.ok else CORRUPT,
            result=result,
            evidence=_extract_evidence(data),
        )


def _extract_evidence(data: bytes) -> dict[str, str | int]:
    """从带正确魔数的字节里尽力读出 device_id / seq，读不出就不放。

    仅做长度防护后的原始读取，不做完整性假设：即便帧随后因截断/校验失败，
    这两个字段仍可作为现场排查依据。
    """
    evidence: dict[str, str | int] = {}
    n = len(data)
    if n >= 6:
        dev_len = struct.unpack_from("<H", data, 4)[0]
        if 6 + dev_len <= n:
            try:
                device_id = data[6:6 + dev_len].decode("ascii")
                if device_id:
                    evidence["device_id"] = device_id
            except UnicodeDecodeError:
                pass
            if 6 + dev_len + 4 <= n:
                evidence["seq"] = struct.unpack_from(
                    "<I", data, 6 + dev_len,
                )[0]
    return evidence
