"""协议解码：两种编码归一 + 各类畸形帧给出可区分拒绝原因。"""
from gateway.model import RejectReason
from gateway.protocols import JsonFrameDecoder, RegisterFrameDecoder
from gateway.device import SimulatedDevice


def test_register_frame_roundtrip(device):
    frame = device.emit_register(7, "temp", 36.6, 1000.5)
    reading, reason, _ = RegisterFrameDecoder().decode(frame)
    assert reason is None
    assert reading is not None
    assert (reading.device_id, reading.seq, reading.metric, reading.source) == (
        "dev-1", 7, "temp", "register",
    )
    assert reading.value == 36.6
    assert reading.timestamp == 1000.5


def test_json_frame_roundtrip(device):
    frame = device.emit_json(9, "hum", 0.42, 2000.0)
    reading, reason, _ = JsonFrameDecoder().decode(frame)
    assert reason is None
    assert reading is not None
    assert reading.source == "json"
    assert (reading.device_id, reading.seq) == ("dev-1", 9)


def test_register_bad_checksum_distinguished(device):
    frame = device.emit_register_bad_checksum(1, "t", 1.0, 1.0)
    reading, reason, detail = RegisterFrameDecoder().decode(frame)
    assert reading is None
    assert reason is RejectReason.CHECKSUM_FAILED
    assert "校验" in detail


def test_register_truncated_is_length_mismatch(device):
    frame = device.emit_register_truncated(1, "t", 1.0, 1.0)
    reading, reason, _ = RegisterFrameDecoder().decode(frame)
    assert reading is None
    assert reason is RejectReason.LENGTH_MISMATCH


def test_register_bad_length_field_is_length_mismatch(device):
    frame = device.emit_register_bad_length(1, "t", 1.0, 1.0)
    reading, reason, _ = RegisterFrameDecoder().decode(frame)
    assert reading is None
    assert reason is RejectReason.LENGTH_MISMATCH


def test_register_empty_device_id_rejected():
    dev = SimulatedDevice("")
    frame = dev.emit_register(1, "t", 1.0, 1.0)
    reading, reason, _ = RegisterFrameDecoder().decode(frame)
    assert reading is None
    assert reason is RejectReason.MISSING_FIELD


def test_json_missing_field_distinguished(device):
    frame = device.emit_json_missing_field(1, "t", 1.0, 1.0)
    reading, reason, detail = JsonFrameDecoder().decode(frame)
    assert reading is None
    assert reason is RejectReason.MISSING_FIELD
    assert "value" in detail


def test_json_malformed_distinguished(device):
    reading, reason, _ = JsonFrameDecoder().decode(device.emit_json_malformed())
    assert reading is None
    assert reason is RejectReason.DECODE_ERROR


def test_json_bad_type_distinguished(device):
    reading, reason, _ = JsonFrameDecoder().decode(device.emit_json_bad_type(1))
    assert reading is None
    assert reason is RejectReason.BAD_FIELD_TYPE


def test_json_bool_seq_rejected(device):
    import json
    raw = json.dumps({
        "device_id": "dev-1", "seq": True, "metric": "m",
        "value": 1, "timestamp": 1,
    }).encode()
    reading, reason, _ = JsonFrameDecoder().decode(raw)
    assert reason is RejectReason.BAD_FIELD_TYPE


def test_unknown_bytes_not_silently_dropped():
    reading, reason, detail = RegisterFrameDecoder().decode(b"\x00\x01garbage")
    assert reading is None
    assert reason is RejectReason.DECODE_ERROR
    assert detail  # 必须给出说明，不得静默丢弃
