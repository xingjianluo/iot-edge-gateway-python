"""协议自行判别：唯一命中归一、认不出与歧义必须可区分地拒绝。"""

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
    assert gw.downstream.count() == 0  # 绝不硬塞、绝不静默丢弃
    print(f"  {result.reason.value}: {result.detail}")


def test_garbage_register_frame_is_unknown_not_forced_as_register():
    """坏寄存器帧在自动路径下是"认不出"，而不是被当成 register 坏帧硬塞。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    # 魔数错误：register 视角 BAD_MAGIC，structured 视角 MALFORMED
    msg = reg.emit_auto(0, corrupt="bad_magic")
    result = gw.ingest(msg)
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    print(f"  bad_magic under auto -> {result.reason.value}")


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
