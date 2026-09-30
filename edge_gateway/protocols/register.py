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
    1) 不足 2 字节                      -> TRUNCATED_FRAME（不可归属）
    2) magic 不符                       -> BAD_MAGIC（不可归属：像别的东西）
    3) 头不足 / 总长不符 / 被截断       -> TRUNCATED_FRAME（归属 register）
    4) 版本不符                         -> MALFORMED（归属 register）
    5) CRC 不符                         -> CHECKSUM_MISMATCH（归属 register）
    6) 字段为空 / 非 ASCII              -> FIELD_MISSING / BAD_VALUE（归属）

归属语义（供协议自行判别使用）：
    magic 一旦对得上 0xA55A，就足以确认这是一帧寄存器报文；此后无论版本、
    长度、CRC 还是字段出了什么问题，结论都是"register 坏帧"
    （attributable=True），而不是"认不出是什么协议"。帧里能安全读出的
    device_id/seq 会作为 evidence 一并带出，供现场排查定位。
    帧不足 2 字节、或 magic 不符，则不能归到 register 头上。
"""

from __future__ import annotations

import struct

from ..models import DecodeResult, Reading, RejectReason

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


def _extract_evidence(data: bytes, dev_len: int) -> dict:
    """从一帧 magic/version 已确认的（可能已坏的）报文中尽力读依据。

    只读定长、边界安全的字段，任何不确定的部分一律不放进结果；
    device_id 允许非 ASCII（替换符展示），因为 BAD_VALUE 场景下
    现场恰恰需要看到是哪台设备的字节坏了。
    """
    evidence: dict = {}
    if 6 + dev_len <= len(data):
        raw_dev = data[6:6 + dev_len]
        evidence["device_id"] = raw_dev.decode("ascii", errors="replace")
        if len(data) >= 6 + dev_len + 4:
            evidence["seq"] = struct.unpack_from("<I", data, 6 + dev_len)[0]
    return evidence


def _bad(reason: RejectReason, detail: str, evidence: dict | None = None):
    """属于 register 的坏帧：自动判别时按 register 自己的结论上报。"""
    return DecodeResult(
        ok=False, reason=reason, detail=detail,
        attributable=True, protocol="register",
        evidence=evidence or None,
    )


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

        # 不足 2 字节：连魔数都无从判断，不能归到任何协议
        if n < 2:
            return DecodeResult(
                ok=False, reason=RejectReason.TRUNCATED_FRAME,
                detail=f"frame shorter than magic: {n} bytes",
            )
        magic = struct.unpack_from("<H", data, 0)[0]
        if magic != MAGIC:
            # 魔数不符：这串字节不像寄存器帧，归属留给别的协议/判未知
            return DecodeResult(
                ok=False, reason=RejectReason.BAD_MAGIC,
                detail=f"bad magic: 0x{magic:04X}",
            )

        # magic 已对上：以下任何失败都是"register 帧坏了"
        if n < 6:
            return _bad(
                RejectReason.TRUNCATED_FRAME,
                f"header incomplete: {n} bytes < 6",
            )

        version, flags, dev_len = struct.unpack_from("<BBH", data, 2)
        evidence = _extract_evidence(data, dev_len)
        if version != VERSION:
            return _bad(
                RejectReason.MALFORMED,
                f"unsupported version: 0x{version:02X}", evidence,
            )

        expected = _MIN_LEN + dev_len
        if n < expected:
            return _bad(
                RejectReason.TRUNCATED_FRAME,
                f"length mismatch: need metric_len/value/crc, "
                f"got {n} < {expected} (dev_len={dev_len})",
                evidence,
            )

        off = 6 + dev_len
        seq, ts, met_len = struct.unpack_from("<IQH", data, off)
        evidence.setdefault("seq", seq)
        expected = 26 + dev_len + met_len
        if n != expected:
            return _bad(
                RejectReason.TRUNCATED_FRAME,
                f"length mismatch: got {n} != {expected} "
                f"(dev_len={dev_len}, metric_len={met_len})",
                evidence,
            )

        crc_want = crc16_modbus(data[:-2])
        crc_got = struct.unpack_from("<H", data, n - 2)[0]
        if crc_got != crc_want:
            return _bad(
                RejectReason.CHECKSUM_MISMATCH,
                f"crc16 mismatch: frame=0x{crc_got:04X} calc=0x{crc_want:04X}",
                evidence,
            )

        try:
            device_id = data[6:6 + dev_len].decode("ascii")
        except UnicodeDecodeError as exc:
            return _bad(
                RejectReason.BAD_VALUE,
                f"device_id not ascii: {exc}", evidence,
            )
        evidence["device_id"] = device_id
        metric_off = off + 14  # seq(4)+ts(8)+mlen(2)
        try:
            metric = data[metric_off:metric_off + met_len].decode("ascii")
        except UnicodeDecodeError as exc:
            return _bad(
                RejectReason.BAD_VALUE,
                f"metric not ascii: {exc}", evidence,
            )

        if not device_id:
            return _bad(
                RejectReason.FIELD_MISSING, "empty device_id", evidence,
            )
        if not metric:
            return _bad(
                RejectReason.FIELD_MISSING,
                f"empty metric (device={device_id}, seq={seq})", evidence,
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
