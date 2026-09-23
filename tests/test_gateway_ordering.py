"""网关层：多协议归一、乱序按序下发、重复只计一次、重放不改变聚合。"""
from gateway.buffer import OverflowPolicy
from gateway.model import RejectReason


def _seq_list(downstream):
    return [r.seq for r in sorted(downstream.received, key=lambda r: (r.device_id, r.seq))]


def test_two_protocols_normalized_and_counted(make_gateway, device):
    gw = make_gateway()
    r1 = gw.ingest(device.emit_register(1, "temp", 1.0, 1.0))
    r2 = gw.ingest(device.emit_json(2, "hum", 2.0, 2.0))
    assert r1.accepted and r2.accepted
    sources = {r.source for r in gw.downstream.received}
    assert sources == {"register", "json"}
    assert gw.downstream.received_count == 2


def test_out_of_order_delivered_in_order(make_gateway, device, caplog):
    gw = make_gateway(window=5)
    # 1, 3, 5, 2, 4 乱序到达
    for seq in (1, 3, 5, 2, 4):
        enc = device.emit_register if seq % 2 else device.emit_json
        res = gw.ingest(enc(seq, "m", float(seq), float(seq)))
        assert res.accepted
    seqs = [r.seq for r in gw.downstream.received]
    assert seqs == [1, 2, 3, 4, 5]  # 下游严格有序
    assert gw.stats()["watermarks"]["dev-1"] == 5
    # 日志必须出现序号、水位与判定依据
    joined = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "watermark=5" in joined and "ACCEPT" in joined


def test_duplicate_counts_once(make_gateway, device):
    gw = make_gateway()
    gw.ingest(device.emit_json(1, "m", 1, 1))
    dup = gw.ingest(device.emit_json(1, "m", 1, 1))
    assert dup.accepted is False
    assert dup.reason is RejectReason.SEQ_ROLLBACK or dup.reason is RejectReason.DUPLICATE_SEQ
    # 用同一字节帧再发一次（真实重放）
    frame = device.emit_register(2, "m", 2, 2)
    assert gw.ingest(frame).accepted
    replay = gw.ingest(frame)
    assert replay.accepted is False
    assert replay.reason in (RejectReason.DUPLICATE_SEQ, RejectReason.SEQ_ROLLBACK)
    assert gw.downstream.received_count == 2
    assert gw.stats()["accepted_readings"] == 2


def test_replay_does_not_change_aggregate(make_gateway, device):
    gw = make_gateway(window=10)
    frames = [device.emit_json(s, "m", s, s) for s in range(1, 6)]
    # 乱序喂入
    for f in [frames[0], frames[3], frames[1], frames[4], frames[2]]:
        assert gw.ingest(f).accepted
    snapshot1 = [r.seq for r in gw.downstream.received]
    assert snapshot1 == [1, 2, 3, 4, 5]
    # 整体重放
    for f in frames:
        res = gw.ingest(f)
        assert res.accepted is False
    snapshot2 = [r.seq for r in gw.downstream.received]
    assert snapshot2 == snapshot1  # 聚合结果不改变
    stats = gw.stats()
    assert stats["downstream_received"] == 5
    assert stats["rejected_duplicate"] + stats["rejected_rollback"] == 5


def test_rollback_and_out_of_window_marked(make_gateway, device):
    gw = make_gateway(window=2)
    gw.ingest(device.emit_json(10, "m", 10, 10))  # 首帧基线 9，水位 10
    back = gw.ingest(device.emit_json(1, "m", 1, 1))
    assert back.reason is RejectReason.SEQ_ROLLBACK
    far = gw.ingest(device.emit_json(14, "m", 14, 14))  # 上界 10+1+2=13
    assert far.reason is RejectReason.OUT_OF_WINDOW
    in_win = gw.ingest(device.emit_json(12, "m", 12, 12))
    assert in_win.accepted  # 乱序窗口内


def test_multi_device_independent_watermarks(make_gateway):
    from gateway.device import SimulatedDevice
    gw = make_gateway(window=5)
    a = SimulatedDevice("dev-A")
    b = SimulatedDevice("dev-B")
    gw.ingest(a.emit_json(1, "m", 1, 1))
    gw.ingest(a.emit_json(3, "m", 3, 3))
    gw.ingest(b.emit_json(1, "m", 1, 1))
    wm = gw.stats()["watermarks"]
    assert wm["dev-A"] == 1 and wm["dev-B"] == 1
    # A 的 seq=2 补齐后按序下发 A:2,A:3
    gw.ingest(a.emit_json(2, "m", 2, 2))
    per_device = {}
    for r in gw.downstream.received:
        per_device.setdefault(r.device_id, []).append(r.seq)
    assert per_device["dev-A"] == [1, 2, 3]
    assert per_device["dev-B"] == [1]
