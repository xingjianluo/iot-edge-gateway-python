"""协议自行判别：认得出但帧坏 / 认不出 / 同时像两种，三类结论可区分。"""

from edge_gateway.devices import RawMessage, SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import (
    CORRUPT,
    MATCH,
    UNRELATED,
    DecodeResult,
    Reading,
    Recognition,
    RejectReason,
)
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


class _ClaimStubDecoder:
    """直接编排 Recognition（MATCH/CORRUPT/UNRELATED）的桩解码器。"""

    def __init__(self, name: str, recognition: Recognition) -> None:
        self.name = name
        self._recognition = recognition

    def decode(self, raw):
        return self._recognition.result

    def recognize(self, raw):
        return self._recognition


def _failure(reason: RejectReason, detail: str = "x") -> DecodeResult:
    return DecodeResult(ok=False, reason=reason, detail=detail)


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
    """魔数不对的字节 register/structured 都不认领 -> 认不出，不硬塞。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    # 魔数错误：register 视角"不像 register"，structured 视角"不像 JSON"
    msg = reg.emit_auto(0, corrupt="bad_magic")
    result = gw.ingest(msg)
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    assert result.protocol == "" and not result.claimed_corrupt
    print(f"  bad_magic under auto -> {result.reason.value}")


def test_recognizable_corrupt_register_frame_gets_its_own_reason():
    """register 帧坏了但魔数在：自动路径给 register 自己的可区分帧级结论。"""
    gw = Gateway()
    reg = SimulatedDevice("line-A", protocol="register")
    cases = {
        "truncate": RejectReason.TRUNCATED_FRAME,
        "bad_crc": RejectReason.CHECKSUM_MISMATCH,
        "missing_field": RejectReason.FIELD_MISSING,
        "bad_value": RejectReason.BAD_VALUE,
        "malformed": RejectReason.MALFORMED,
    }
    for corrupt, want in cases.items():
        result = gw.ingest(reg.emit_auto(7, corrupt=corrupt))
        assert not result.ok, corrupt
        assert result.stage == "identify", corrupt
        assert result.reason is want, (corrupt, result.reason)
        assert result.claimed_corrupt is True, corrupt
        assert result.protocol == "register", corrupt
        # 依据：从字节里实际读到的序号/设备必须带出来
        assert result.seq == 7, (corrupt, result.evidence)
        assert "device_id" not in result.evidence or result.device_id == "line-A"
        print(f"  register {corrupt:13s} -> {result.reason.value} | {result.detail}")

    # 与指定协议老路逐字节一致：同样的帧、同样的原因
    for corrupt, want in cases.items():
        explicit = gw.ingest(reg.emit(8, corrupt=corrupt))
        assert explicit.reason is want, (corrupt, explicit.reason)

    st = gw.stats_snapshot()
    # 认得出的坏帧不计入 unidentified；仍计入 decode_rejected，且绝不投递
    assert st.unidentified == 0 and st.ambiguous == 0
    assert st.decode_rejected == len(cases) * 2
    assert gw.downstream.count() == 0


def test_recognizable_corrupt_structured_frame_gets_its_own_reason():
    """JSON 对象但内容坏了：自动路径给 structured 自己的可区分结论。"""
    gw = Gateway()
    js = SimulatedDevice("line-B", protocol="structured")
    cases = {
        "missing_field": RejectReason.FIELD_MISSING,
        "bad_value": RejectReason.BAD_VALUE,
    }
    for corrupt, want in cases.items():
        result = gw.ingest(js.emit_auto(3, corrupt=corrupt))
        assert not result.ok
        assert result.reason is want, (corrupt, result.reason)
        assert result.protocol == "structured" and result.claimed_corrupt
        assert result.seq == 3 and result.device_id == "line-B"
        print(f"  structured {corrupt:13s} -> {result.reason.value} "
              f"evidence={result.evidence}")
    # 截断到连 JSON 都不是：结构化解码器不认领 -> 仍然认不出
    result = gw.ingest(js.emit_auto(3, corrupt="truncate"))
    assert result.reason is RejectReason.UNKNOWN_PROTOCOL
    st = gw.stats_snapshot()
    assert st.unidentified == 1 and st.ambiguous == 0
    assert st.decode_rejected == 3  # 2 个认得出的坏帧 + 1 个认不出


def test_corrupt_unknown_ambiguous_are_separately_counted_and_logged():
    """三类自动识别结论在同一次运行里各走各的计数，互不混淆。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    # 1) 认得出但帧坏
    r1 = gw.ingest(reg.emit_auto(0, corrupt="bad_crc"))
    assert r1.reason is RejectReason.CHECKSUM_MISMATCH
    assert r1.claimed_corrupt and r1.protocol == "register"
    # 2) 真正认不出
    r2 = gw.ingest(RawMessage(AUTO_PROTOCOL, b"\x00\x01mystery-bytes", "?"))
    assert r2.reason is RejectReason.UNKNOWN_PROTOCOL
    assert not r2.claimed_corrupt and r2.protocol == ""
    # 3) 歧义
    gw.identifier = ProtocolIdentifier([
        _StubDecoder("register", DecodeResult(ok=True, reading=_reading("register"))),
        _StubDecoder("structured", DecodeResult(ok=True, reading=_reading("structured"))),
    ])
    r3 = gw.ingest(RawMessage(AUTO_PROTOCOL, b"x", "amb"))
    assert r3.reason is RejectReason.AMBIGUOUS_PROTOCOL
    st = gw.stats_snapshot()
    assert st.unidentified == 1 and st.ambiguous == 1
    assert st.decode_rejected == 3   # 三类都算解码阶段拒绝
    assert st.rejected == 3
    assert gw.downstream.count() == 0


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


def test_ambiguous_when_two_protocols_both_claim_corrupt():
    """两种编码都认领（帧都坏）也要单独报歧义，不能挑一种当坏帧。"""
    pi = ProtocolIdentifier([
        _ClaimStubDecoder(
            "register",
            Recognition(
                kind=CORRUPT,
                result=_failure(RejectReason.CHECKSUM_MISMATCH),
                evidence={"device_id": "d", "seq": 1},
            ),
        ),
        _ClaimStubDecoder(
            "structured",
            Recognition(
                kind=CORRUPT,
                result=_failure(RejectReason.FIELD_MISSING),
                evidence={"device_id": "d", "seq": 1},
            ),
        ),
    ])
    result = pi.identify(b"claimable-by-both")
    assert result.reason is RejectReason.AMBIGUOUS_PROTOCOL
    assert "CORRUPT" in result.detail
    assert result.protocol == ""  # 歧义时不带任何协议名，不硬挑

    # 一个 MATCH + 一个 CORRUPT 同样是歧义，绝不接受 MATCH 那一方了事
    pi2 = ProtocolIdentifier([
        _ClaimStubDecoder(
            "register",
            Recognition(kind=MATCH, result=DecodeResult(
                ok=True, reading=_reading("register"))),
        ),
        _ClaimStubDecoder(
            "structured",
            Recognition(kind=CORRUPT,
                        result=_failure(RejectReason.BAD_VALUE)),
        ),
    ])
    result2 = pi2.identify(b"x")
    assert result2.reason is RejectReason.AMBIGUOUS_PROTOCOL


def test_single_corrupt_claim_passes_protocol_reason_and_evidence():
    """只有一方认领且是坏帧：透传协议名、可区分原因与读出的依据。"""
    pi = ProtocolIdentifier([
        _ClaimStubDecoder(
            "register",
            Recognition(
                kind=CORRUPT,
                result=_failure(RejectReason.TRUNCATED_FRAME, "cut"),
                evidence={"device_id": "dev-9", "seq": 42},
            ),
        ),
        _ClaimStubDecoder(
            "structured",
            Recognition(
                kind=UNRELATED,
                result=_failure(RejectReason.MALFORMED, "not json"),
            ),
        ),
    ])
    result = pi.identify(b"yy")
    assert result.reason is RejectReason.TRUNCATED_FRAME
    assert result.protocol == "register"
    assert result.evidence == {"device_id": "dev-9", "seq": 42}
    assert "dev-9" in result.detail and "42" in result.detail


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
