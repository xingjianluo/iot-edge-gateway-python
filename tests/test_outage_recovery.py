"""下游从中断到恢复的完整链路：缓冲、溢出统计、按序补发、不重复计数。"""
from gateway.buffer import OverflowPolicy
from gateway.model import RejectReason


def test_outage_buffers_and_restore_replays_in_order(make_gateway, device):
    gw = make_gateway(capacity=100)
    gw.downstream.set_available(False)
    # 乱序送入 1..4
    for seq in (1, 4, 2, 3):
        assert gw.ingest(device.emit_json(seq, "m", seq, seq)).accepted
    assert gw.downstream.received_count == 0
    assert gw.stats()["buffer"]["size"] == 4

    gw.downstream.set_available(True)
    sent = gw.on_downstream_restored()
    assert sent == 4
    assert [r.seq for r in gw.downstream.received] == [1, 2, 3, 4]
    assert gw.stats()["buffer"]["size"] == 0
    # 补发后再到新数据直接下发
    gw.ingest(device.emit_json(5, "m", 5, 5))
    assert gw.downstream.received_count == 5


def test_outage_overflow_drop_oldest_explained(make_gateway, device, caplog):
    gw = make_gateway(capacity=2, policy=OverflowPolicy.DROP_OLDEST)
    gw.downstream.set_available(False)
    for seq in range(1, 6):
        gw.ingest(device.emit_register(seq, "m", seq, seq))
    stats = gw.stats()["buffer"]
    assert stats["size"] == 2
    assert stats["dropped_oldest"] == 3
    assert stats["last_dropped"] == ("dev-1", 3)
    # 被丢弃的不应在恢复后出现
    gw.downstream.set_available(True)
    gw.on_downstream_restored()
    assert [r.seq for r in gw.downstream.received] == [4, 5]
    assert any("丢弃最旧读数" in rec.getMessage() for rec in caplog.records)


def test_outage_overflow_drop_newest_explained(make_gateway, device, caplog):
    gw = make_gateway(capacity=2, policy=OverflowPolicy.DROP_NEWEST)
    gw.downstream.set_available(False)
    for seq in range(1, 6):
        gw.ingest(device.emit_register(seq, "m", seq, seq))
    stats = gw.stats()["buffer"]
    assert stats["dropped_newest"] == 3
    assert stats["last_dropped"] == ("dev-1", 5)
    gw.downstream.set_available(True)
    gw.on_downstream_restored()
    assert [r.seq for r in gw.downstream.received] == [1, 2]
    assert any("拒绝最新读数" in rec.getMessage() for rec in caplog.records)


def test_restore_no_double_counting(make_gateway, device):
    gw = make_gateway(capacity=10)
    gw.downstream.set_available(False)
    for seq in range(1, 4):
        gw.ingest(device.emit_json(seq, "m", seq, seq))
    gw.downstream.set_available(True)
    gw.on_downstream_restored()
    # 再次触发恢复（缓冲已空），不得产生任何重复投递
    again = gw.on_downstream_restored()
    assert again == 0
    assert gw.downstream.received_count == 3
    assert gw.downstream.duplicate_delivery == 0


def test_delivery_ordered_per_device_across_outage(make_gateway):
    from gateway.device import SimulatedDevice
    gw = make_gateway(window=10, capacity=100)
    a, b = SimulatedDevice("A"), SimulatedDevice("B")
    gw.downstream.set_available(False)
    gw.ingest(a.emit_json(1, "m", 1, 1))
    gw.ingest(b.emit_json(1, "m", 1, 1))
    gw.ingest(a.emit_json(3, "m", 3, 3))
    gw.ingest(a.emit_json(2, "m", 2, 2))
    gw.downstream.set_available(True)
    gw.on_downstream_restored()
    per = {}
    for r in gw.downstream.received:
        per.setdefault(r.device_id, []).append(r.seq)
    assert per["A"] == [1, 2, 3]
    assert per["B"] == [1]


def test_malformed_during_outage_not_buffered(make_gateway, device):
    gw = make_gateway(capacity=10)
    gw.downstream.set_available(False)
    res = gw.ingest(device.emit_register_bad_checksum(1, "m", 1, 1))
    assert res.accepted is False
    assert res.reason is RejectReason.CHECKSUM_FAILED
    assert gw.stats()["buffer"]["size"] == 0
    assert gw.stats()["rejected_decode"] == 1
