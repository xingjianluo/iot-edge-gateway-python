"""协议接入与归一测试：寄存器帧 + 结构化报文及全部拒绝原因。"""

import struct

import pytest

from edge_gateway.models import Reading, RejectReason
from edge_gateway.protocols.register import (
    MAGIC,
    VERSION,
    RegisterFrameDecoder,
    crc16_modbus,
    encode_register_frame,
)
from edge_gateway.protocols.structured import (
    StructuredDecoder,
    encode_structured,
)


# ---------- CRC ----------

def test_crc16_known_vector():
    # CRC16/MODBUS("123456789") = 0x4B37
    assert crc16_modbus(b"123456789") == 0x4B37


# ---------- 寄存器帧：正常归一 ----------

def test_register_roundtrip_unsigned():
    frame = encode_register_frame(
        device_id="dev-1", seq=42, timestamp_ms=1700,
        metric="temp", value=21.5,
    )
    r = RegisterFrameDecoder().decode(frame)
    assert r.ok
    assert isinstance(r.reading, Reading)
    assert r.reading == Reading(
        device_id="dev-1", seq=42, timestamp_ms=1700,
        metric="temp", value=21.5, source="register",
    )


def test_register_roundtrip_signed_negative():
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="m",
        value=-4.25, signed=True,
    )
    r = RegisterFrameDecoder().decode(frame)
    assert r.ok and r.reading is not None
    assert r.reading.value == -4.25


def test_register_magic_bytes_little_endian():
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="m", value=1.0,
    )
    assert frame[:2] == struct.pack("<H", MAGIC) == b"\x5a\xa5"


# ---------- 寄存器帧：拒绝原因必须可区分 ----------

@pytest.fixture
def good_frame():
    return encode_register_frame(
        device_id="dev-1", seq=1, timestamp_ms=10, metric="m", value=1.0,
    )


def test_register_empty_and_truncated(good_frame):
    dec = RegisterFrameDecoder()
    assert dec.decode(b"").reason is RejectReason.TRUNCATED_FRAME
    assert dec.decode(b"\x5a").reason is RejectReason.TRUNCATED_FRAME
    assert dec.decode(good_frame[:6]).reason is RejectReason.TRUNCATED_FRAME
    assert dec.decode(good_frame[:-1]).reason is RejectReason.TRUNCATED_FRAME
    # 多长也不行
    assert dec.decode(good_frame + b"\x00").reason is \
        RejectReason.TRUNCATED_FRAME


def test_register_bad_magic(good_frame):
    bad = b"\x00\x00" + good_frame[2:]
    assert RegisterFrameDecoder().decode(bad).reason is \
        RejectReason.BAD_MAGIC


def test_register_bad_crc():
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="m",
        value=1.0, bad_crc=True,
    )
    r = RegisterFrameDecoder().decode(frame)
    assert r.reason is RejectReason.CHECKSUM_MISMATCH
    assert "crc" in r.detail.lower()


def test_register_bad_version(good_frame):
    b = bytearray(good_frame)
    b[2] = VERSION ^ 0xFF
    assert RegisterFrameDecoder().decode(bytes(b)).reason is \
        RejectReason.MALFORMED


def test_register_empty_fields():
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="", value=1.0,
    )
    assert RegisterFrameDecoder().decode(frame).reason is \
        RejectReason.FIELD_MISSING


def test_register_rejects_str_input():
    r = RegisterFrameDecoder().decode("not-bytes")
    assert r.reason is RejectReason.MALFORMED


# ---------- 结构化报文：正常归一 ----------

def test_structured_roundtrip():
    payload = encode_structured(
        device_id="dev-2", seq=7, timestamp_ms=99, metric="vibe", value=-3.5,
    )
    r = StructuredDecoder().decode(payload)
    assert r.ok and r.reading == Reading(
        device_id="dev-2", seq=7, timestamp_ms=99,
        metric="vibe", value=-3.5, source="structured",
    )


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b"not json at all", RejectReason.MALFORMED),
        (b'{"a":1}', RejectReason.FIELD_MISSING),
        (b'{"device":"d","seq":1,"ts":1,"metric":"m"}',
         RejectReason.FIELD_MISSING),
        (b'{"device":"","seq":1,"ts":1,"metric":"m","value":1}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":-5,"ts":1,"metric":"m","value":1}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":1,"ts":-1,"metric":"m","value":1}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":"1","ts":1,"metric":"m","value":1}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":true,"ts":1,"metric":"m","value":1}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":1,"ts":1,"metric":"m","value":"x"}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":1,"ts":1,"metric":"m","value":NaN}',
         RejectReason.BAD_VALUE),
        (b'{"device":"d","seq":1,"ts":1,"metric":"m","value":Infinity}',
         RejectReason.BAD_VALUE),
        (b"[1,2,3]", RejectReason.MALFORMED),
    ],
)
def test_structured_rejection_matrix(payload, reason):
    r = StructuredDecoder().decode(payload)
    assert not r.ok
    assert r.reason is reason, (payload, r.reason)
