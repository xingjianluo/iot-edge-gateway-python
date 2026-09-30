"""协议自行判别：唯一命中归一、认不出与歧义必须可区分地拒绝。"""

import pytest

from edge_gateway.devices import RawMessage, SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import DecodeResult, Reading, RejectReason
from edge_gateway.protocols.identification import (
    AUTO_PROTOCOL,
    ProtocolIdentifier,
    default_decoders,
)


class _StubDecoder:
    """可编排解码结果的桩解码器，用于构造真实帧无法产生的歧义场景。"""

    def __init__(self, name: str, result: DecodeResult) -> None:
        self.name = name
        self._result = result

    def decode(self, raw):
        return self._result


def _reading(source: str) -> Reading:
    return Reading(
        device_id="d", seq=1, timestamp_ms=2, metric="m",
        value=1.5, source=source,
    )


def test_auto_identifies_register_and_structured_bytes():
    pi = ProtocolIdentifier(default_decoders())
    reg = SimulatedDevice("line-A", protocol="register")
    js = SimulatedDevice("line-B", protocol="structured")
    r1 = pi.identify(reg.emit(3).payload)
    r2 = pi.identify(js.emit(4).payload)
    assert r1.ok and r1.reading.source == "register"
    assert r1.reading.seq == 3
    assert r2.ok and r2.reading.source == "structured"
    assert r2.reading.seq == 4
    print(f"  register -> {r1.detail}")
    print(f"  structured -> {r2.detail}")


def test_gateway_auto_path_normalizes_and_marks_protocol():
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    js = SimulatedDevice("d2", protocol="structured")
    for dev in (reg, js):
        result = gw.ingest(dev.emit_auto(0))
        assert result.ok and result.protocol == dev.protocol
        assert result.stage == "delivered"
    # 同一设备自动识别的读数与显式协议路径进入同一读数模型/时序
    assert gw.downstream.delivered_seqs("d") == [0]
    assert gw.downstream.delivered_seqs("d2") == [0]


def test_explicit_protocol_path_still_works():
    """老路：知道协议时严格按指定协议解析，不做猜测式回退。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    result = gw.ingest(reg.emit(0))  # protocol="register"
    assert result.ok and result.protocol == "register"
    # 把 JSON 字节硬声明成 register：按 register 规则拒绝，不回退 JSON
    js = SimulatedDevice("d2", protocol="structured")
    wrong = RawMessage(
        protocol="register", payload=js.emit(0).payload, label="mislabeled",
    )
    result = gw.ingest(wrong)
    assert not result.ok and result.stage == "decode"
    print(f"  mislabeled -> {result.reason.value}: {result.detail}")


def test_unrecognized_bytes_rejected_as_unknown_protocol():
    gw = Gateway()
    msg = RawMessage(
        protocol=AUTO_PROTOCOL,
        payload=b"\x00\x01this-is-neither-frame-nor-json",
        label="mystery-bytes",
    )
    result = gw.ingest(msg)
    assert not result.ok
    assert result.stage == "identify"
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    # detail 必须能看出尝试了哪些协议、各自结论
    assert "register" in result.detail and "structured" in result.detail
    st = gw.stats_snapshot()
    assert st.unidentified == 1 and st.decode_rejected == 1
    assert st.ambiguous == 0
    assert gw.downstream.count() == 0  # 绝不硬塞、绝不静默丢弃
    print(f"  {result.reason.value}: {result.detail}")


def test_bad_magic_still_unknown_because_it_cannot_be_attributed():
    """魔数对不上时无法确认它属于 register：仍然是"认不出"，不是坏帧。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    msg = reg.emit_auto(0, corrupt="bad_magic")
    result = gw.ingest(msg)
    assert result.stage == "identify"
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    assert result.protocol == ""
    st = gw.stats_snapshot()
    assert st.unidentified == 1 and st.ambiguous == 0
    print(f"  bad_magic under auto -> {result.reason.value}")


@pytest.mark.parametrize(("corrupt", "want"), [
    ("bad_crc", RejectReason.CHECKSUM_MISMATCH),
    ("truncate", RejectReason.TRUNCATED_FRAME),
    ("missing_field", RejectReason.FIELD_MISSING),
    ("bad_value", RejectReason.BAD_VALUE),
    ("malformed", RejectReason.MALFORMED),  # 版本非法：magic 仍对得上
])
def test_corrupt_register_frame_identified_as_register_bad_frame(corrupt, want):
    """认得出是 register 但帧坏了：自动路径必须给 register 自己的结论。"""
    gw = Gateway()
    reg = SimulatedDevice("line-A", protocol="register")
    msg = reg.emit_auto(7, corrupt=corrupt)
    result = gw.ingest(msg)
    assert result.stage == "identify"
    assert result.reason is want, (corrupt, result.reason, result.detail)
    assert result.protocol == "register"
    # 坏帧结论不能算成"认不出"或"歧义"
    st = gw.stats_snapshot()
    assert st.unidentified == 0 and st.ambiguous == 0
    assert st.decode_rejected == 1
    # 依据：能从坏帧读出的设备/序号要带出来（bad_value 的 device_id 非 ASCII）
    if corrupt != "bad_value":
        assert result.device_id == "line-A"
        assert result.seq == 7
        assert result.evidence["device_id"] == "line-A"
        assert result.evidence["seq"] == 7
    else:
        assert result.seq == 7  # device 字节坏了，序号仍可读
    assert want.value in result.detail or "register" in result.detail
    assert gw.downstream.count() == 0
    print(f"  register[{corrupt}] -> {result.reason.value}: {result.detail}")


@pytest.mark.parametrize(("corrupt", "want"), [
    ("missing_field", RejectReason.FIELD_MISSING),
    ("bad_value", RejectReason.BAD_VALUE),
])
def test_corrupt_structured_frame_identified_as_structured_bad_frame(
    corrupt, want,
):
    """认得出是 structured JSON 对象但内容坏了：给 structured 自己的结论。"""
    gw = Gateway()
    js = SimulatedDevice("line-B", protocol="structured")
    msg = js.emit_auto(9, corrupt=corrupt)
    result = gw.ingest(msg)
    assert result.stage == "identify"
    assert result.reason is want
    assert result.protocol == "structured"
    st = gw.stats_snapshot()
    assert st.unidentified == 0 and st.ambiguous == 0
    assert st.decode_rejected == 1
    assert result.device_id == "line-B" and result.seq == 9
    print(f"  structured[{corrupt}] -> {result.reason.value}: {result.detail}")


def test_auto_corrupt_frame_conclusion_matches_explicit_path():
    """同一段坏字节：声明协议与自动判别给出的帧级结论必须一致。"""
    reg = SimulatedDevice("d", protocol="register")
    for corrupt in ("bad_crc", "truncate", "missing_field", "bad_value"):
        gw_explicit = Gateway()
        gw_auto = Gateway()
        r1 = gw_explicit.ingest(reg.emit(3, corrupt=corrupt))
        r2 = gw_auto.ingest(reg.emit_auto(3, corrupt=corrupt))
        assert r1.reason is r2.reason, corrupt
        assert r1.reason is not RejectReason.UNKNOWN_PROTOCOL
        assert r2.protocol == "register"


def test_three_verdicts_are_separately_counted_and_logged():
    """坏帧 / 认不出 / 歧义三种结论在同一网关里分开计数、互不混淆。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    # 1) 认得出但帧坏（CRC）
    r_bad = gw.ingest(reg.emit_auto(0, corrupt="bad_crc"))
    assert r_bad.reason is RejectReason.CHECKSUM_MISMATCH
    # 2) 真正认不出（坏魔数 + 非 JSON）
    r_unk = gw.ingest(reg.emit_auto(1, corrupt="bad_magic"))
    assert r_unk.reason is RejectReason.UNKNOWN_PROTOCOL
    # 3) 歧义（两个解码器都声称认得出）
    gw.identifier = ProtocolIdentifier([
        _StubDecoder(
            "register",
            DecodeResult(
                ok=False, reason=RejectReason.CHECKSUM_MISMATCH, detail="c",
                attributable=True, protocol="register",
            ),
        ),
        _StubDecoder(
            "structured",
            DecodeResult(
                ok=False, reason=RejectReason.FIELD_MISSING, detail="m",
                attributable=True, protocol="structured",
            ),
        ),
    ])
    r_amb = gw.ingest(RawMessage(AUTO_PROTOCOL, b"x", "amb"))
    assert r_amb.reason is RejectReason.AMBIGUOUS_PROTOCOL
    st = gw.stats_snapshot()
    assert st.decode_rejected == 3
    assert st.unidentified == 1
    assert st.ambiguous == 1
    # 三条日志的 detail 措辞也分得开
    assert "corrupt 'register' frame" in r_bad.detail
    assert "no known protocol matched" in r_unk.detail
    assert "corrupt frame of 2 protocols" in r_amb.detail


def test_ambiguous_match_is_rejected_distinctly():
    """两个解码器同时成功 -> AMBIGUOUS_PROTOCOL，绝不二选一硬塞。"""
    pi = ProtocolIdentifier([
        _StubDecoder("register", DecodeResult(ok=True, reading=_reading("register"))),
        _StubDecoder("structured", DecodeResult(ok=True, reading=_reading("structured"))),
    ])
    result = pi.identify(b"anything")
    assert not result.ok
    assert result.reason is RejectReason.AMBIGUOUS_PROTOCOL
    assert "register" in result.detail and "structured" in result.detail
    print(f"  {result.reason.value}: {result.detail}")


def test_ambiguous_is_distinct_from_unknown_at_gateway():
    gw = Gateway()
    gw.identifier = ProtocolIdentifier([
        _StubDecoder("register", DecodeResult(ok=True, reading=_reading("register"))),
        _StubDecoder("structured", DecodeResult(ok=True, reading=_reading("structured"))),
    ])
    result = gw.ingest(RawMessage(AUTO_PROTOCOL, b"x", "amb"))
    assert result.reason is RejectReason.AMBIGUOUS_PROTOCOL
    assert gw.stats_snapshot().ambiguous == 1
    # UNKNOWN 与 AMBIGUOUS 计数互不混淆
    gw.identifier = ProtocolIdentifier([
        _StubDecoder(
            "register",
            DecodeResult(ok=False, reason=RejectReason.BAD_MAGIC, detail="x"),
        ),
    ])
    result = gw.ingest(RawMessage(AUTO_PROTOCOL, b"y", "unk"))
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    st = gw.stats_snapshot()
    assert st.ambiguous == 1 and st.unidentified == 1
