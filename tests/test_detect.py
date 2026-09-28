"""协议自行判别（auto）测试：唯一命中归一、认不出/同时像两种要明确拒绝。

每个用例打印序号、水位与判定依据；老的"显式指定协议"路径单独验证不变。
"""

from edge_gateway.devices import RawMessage, SimulatedDevice, auto_message
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.protocols import ProtocolDetector, RegisterFrameDecoder
from edge_gateway.protocols.register import encode_register_frame
from edge_gateway.protocols.structured import encode_structured


def _feed(gw, msg):
    result = gw.ingest(msg)
    reason = result.reason.value if result.reason else "-"
    print(
        f"  seq={str(result.seq):>4s} wm={str(result.watermark):>4s} "
        f"stage={result.stage:9s} {'ACCEPT' if result.ok else 'REJECT'} "
        f"reason={reason:22s} detected={result.detected_protocol or '-'}",
    )
    if not result.ok and result.detail:
        print(f"             └─ {result.detail}")
    return result


def test_auto_detect_register_and_structured_are_normalized():
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    js = SimulatedDevice("d", protocol="structured")
    r1 = _feed(gw, auto_message(reg.emit(0).payload, "auto-reg#0"))
    r2 = _feed(gw, auto_message(js.emit(1).payload, "auto-json#1"))
    assert r1.ok and r1.detected_protocol == "register"
    assert r2.ok and r2.detected_protocol == "structured"
    assert gw.downstream.delivered_seqs("d") == [0, 1]
    sources = {x.source for x in gw.downstream.received}
    assert sources == {"register", "structured"}


def test_auto_unrecognized_bytes_are_rejected_distinguishably():
    gw = Gateway()
    for payload in (b"", b"\x00\x01weird", b"[1,2,3]", b" plain text ", b"\xff\xff\xff"):
        r = _feed(gw, auto_message(payload, f"auto-unknown:{payload!r}"))
        assert not r.ok
        assert r.stage == "detect"
        assert r.reason is RejectReason.UNRECOGNIZED_PROTOCOL
    # 不瞎猜、不静默丢弃：没有读数进入下游
    assert gw.downstream.count() == 0
    assert gw.stats_snapshot().decode_rejected == 5


def test_auto_detects_signature_then_decode_failure_is_separate_stage():
    """签名命中但内容损坏：识别阶段通过，解析阶段给明确、可区分原因。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    r1 = _feed(gw, auto_message(reg.emit(0, corrupt="bad_crc").payload, "crc"))
    r2 = _feed(gw, auto_message(reg.emit(1, corrupt="truncate").payload, "trunc"))
    r3 = _feed(gw, auto_message(b"{oops", "json-broken"))
    assert r1.stage == "decode"
    assert r1.reason is RejectReason.CHECKSUM_MISMATCH
    assert r1.detected_protocol == "register"
    assert "detected=register" in r1.detail
    assert r2.reason is RejectReason.TRUNCATED_FRAME
    # 以 '{' 开头的坏 JSON：能认出是 structured，但解析失败而非 UNRECOGNIZED
    assert r3.stage == "decode" and r3.reason is RejectReason.MALFORMED
    assert r3.detected_protocol == "structured"


def test_explicit_protocol_path_is_unchanged():
    """老路径：显式指定协议时不做自动识别，行为与此前一致。"""
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    r = _feed(gw, reg.emit(0))
    assert r.ok and r.detected_protocol == ""
    # 显式未知协议仍是 MALFORMED（而非 UNRECOGNIZED_PROTOCOL）
    r2 = _feed(gw, RawMessage("mystery", b"\x5a\xa5x", "x"))
    assert not r2.ok and r2.reason is RejectReason.MALFORMED


def test_detector_ambiguous_when_two_signatures_match():
    """两个解码器都声称命中 -> AMBIGUOUS_PROTOCOL，绝不硬塞一种。"""

    class AlwaysSniff:
        name = "fake"

        def sniff(self, raw):
            return True

        def decode(self, raw):  # pragma: no cover - 不应被调用
            raise AssertionError("decode must not run on ambiguity")

    detector = ProtocolDetector(
        [RegisterFrameDecoder(), AlwaysSniff()],
    )
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="m", value=1.0,
    )
    verdict = detector.detect(frame)
    print(f"  ambiguous verdict: {verdict}")
    assert not verdict.ok
    assert verdict.reason is RejectReason.AMBIGUOUS_PROTOCOL
    assert set(verdict.detail)  # detail 列出候选
    assert "register" in verdict.detail and "fake" in verdict.detail


def test_native_signatures_are_disjoint():
    """内置两种编码的合法报文绝不互相命中（避免内置产生歧义）。"""
    detector = ProtocolDetector()
    frame = encode_register_frame(
        device_id="d", seq=1, timestamp_ms=1, metric="m", value=1.0,
    )
    js = encode_structured(
        device_id="d", seq=1, timestamp_ms=1, metric="m", value=1.0,
    )
    assert detector.detect(frame).protocol == "register"
    assert detector.detect(js).protocol == "structured"
    # register 解码器 sniff 拒绝 str；structured sniff 拒绝非 UTF-8
    assert detector.detect("not-bytes").reason is \
        RejectReason.UNRECOGNIZED_PROTOCOL
