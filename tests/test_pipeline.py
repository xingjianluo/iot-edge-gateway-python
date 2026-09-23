"""完整链路：双协议接入 + 乱序/重复/畸形帧 + 下游中断->缓冲->恢复。

所有用例打印序号、水位与判定依据，结论可区分、可复现。
"""

import threading

from edge_gateway.devices import SimulatedDevice
from edge_gateway.downstream import DownstreamUnavailable
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason


def _log(result):
    reason = result.reason.value if result.reason else "-"
    print(
        f"  seq={str(result.seq):>4s} wm={str(result.watermark):>4s} "
        f"stage={result.stage:9s} {'ACCEPT' if result.ok else 'REJECT'} "
        f"reason={reason:18s} delivered={result.delivered} "
        f"buffered={result.buffered}",
    )


def _feed(gw, msg):
    result = gw.ingest(msg)
    _log(result)
    return result


def test_multi_protocol_normalized_into_same_model():
    gw = Gateway()
    reg = SimulatedDevice("d", protocol="register")
    js = SimulatedDevice("d", protocol="structured")
    r1 = _feed(gw, reg.emit(0))
    r2 = _feed(gw, js.emit(1))
    assert r1.ok and r2.ok
    assert gw.downstream.delivered_seqs("d") == [0, 1]
    sources = {x.source for x in gw.downstream.received}
    assert sources == {"register", "structured"}


def test_outage_to_recovery_full_chain_is_ordered_and_exactly_once():
    gw = Gateway(window_size=16, spool_capacity=5)
    dev = SimulatedDevice("line", protocol="register")

    # 乱序接入：先 1 再 0
    assert _feed(gw, dev.emit(1)).delivered
    assert _feed(gw, dev.emit(0)).delivered
    # 重复
    assert _feed(gw, dev.emit(1)).reason is RejectReason.DUPLICATE_SEQ

    # 中断：2..6 缓冲（容量 5）
    gw.downstream.set_available(False)
    for s in range(2, 7):
        assert _feed(gw, dev.emit(s)).buffered
    # 缓冲期间重复不重复入队计数
    assert _feed(gw, dev.emit(3)).reason is RejectReason.DUPLICATE_SEQ
    assert gw.spool.snapshot_stats().size == 5

    # 恢复：严格按序补发
    gw.downstream.set_available(True)
    flushed = gw.on_downstream_recovered()
    print(f"  flushed={flushed}")
    assert flushed == 5
    assert gw.downstream.delivered_seqs("line") == [1, 0, 2, 3, 4, 5, 6]
    assert gw.spool.snapshot_stats().size == 0


def test_malformed_frames_never_silently_dropped():
    gw = Gateway(window_size=16, spool_capacity=50)
    reg = SimulatedDevice("d", protocol="register")
    js = SimulatedDevice("d2", protocol="structured")
    cases = [
        (reg.emit(0, corrupt="truncate"), RejectReason.TRUNCATED_FRAME),
        (reg.emit(1, corrupt="bad_crc"), RejectReason.CHECKSUM_MISMATCH),
        (reg.emit(2, corrupt="bad_magic"), RejectReason.BAD_MAGIC),
        (reg.emit(3, corrupt="malformed"), RejectReason.MALFORMED),
        (reg.emit(4, corrupt="missing_field"),
         RejectReason.FIELD_MISSING),
        (reg.emit(5, corrupt="bad_value"), RejectReason.BAD_VALUE),
        (js.emit(0, corrupt="malformed"), RejectReason.MALFORMED),
        (js.emit(1, corrupt="missing_field"),
         RejectReason.FIELD_MISSING),
        (js.emit(2, corrupt="bad_value"), RejectReason.BAD_VALUE),
    ]
    for msg, want in cases:
        result = _feed(gw, msg)
        assert not result.ok and result.stage == "decode"
        assert result.reason is want, (msg.label, result.reason)
    st = gw.stats_snapshot()
    assert st.decode_rejected == 9 and st.accepted == 0
    assert gw.downstream.count() == 0  # 没有任何畸形数据流入下游


def test_overflow_policy_and_stats_are_explainable():
    gw = Gateway(window_size=64, spool_capacity=3)
    dev = SimulatedDevice("d", protocol="register")
    gw.downstream.set_available(False)
    for s in range(3):
        assert _feed(gw, dev.emit(s)).buffered
    r = _feed(gw, dev.emit(3))
    assert not r.ok and r.reason is RejectReason.BUFFER_OVERFLOW
    assert "capacity=3" in r.detail
    st = gw.stats_snapshot()
    assert st.overflow_dropped == 1
    sp = gw.spool.snapshot_stats()
    assert sp.size == 3 and sp.dropped_overflow == 1

    # drop_oldest：腾退最旧，恢复后收到 1,2,3（0 被确定地丢弃）
    gw2 = Gateway(
        window_size=64, spool_capacity=3, overflow_policy="drop_oldest",
    )
    dev2 = SimulatedDevice("d", protocol="register")
    gw2.downstream.set_available(False)
    for s in range(4):
        gw2.ingest(dev2.emit(s))
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("d") == [1, 2, 3]
    assert gw2.spool.snapshot_stats().dropped_overflow == 1


def test_replay_does_not_change_aggregate():
    gw = Gateway(window_size=8, spool_capacity=3)
    dev = SimulatedDevice("d", protocol="register")
    for s in (0, 1):
        gw.ingest(dev.emit(s))
    gw.downstream.set_available(False)
    for s in (2, 3, 4):
        gw.ingest(dev.emit(s))
    gw.ingest(dev.emit(5))  # 溢出
    gw.ingest(dev.emit(6, corrupt="bad_crc"))  # 畸形
    gw.downstream.set_available(True)
    gw.on_downstream_recovered()

    delivered_before = list(gw.downstream.delivered_seqs("d"))
    stats_before = gw.stats_snapshot()

    # 忠实重放全部历史（含坏帧），再补发
    for s in range(6):
        gw.ingest(dev.emit(s))
    gw.ingest(dev.emit(6, corrupt="bad_crc"))
    gw.on_downstream_recovered()

    assert gw.downstream.delivered_seqs("d") == delivered_before == [0, 1, 2, 3, 4]
    stats_after = gw.stats_snapshot()
    assert stats_after.delivered == stats_before.delivered
    assert stats_after.watermarks["d"] == stats_before.watermarks["d"] == 5


def test_concurrent_ingestion_keeps_counts_and_watermarks_consistent():
    gw = Gateway(window_size=1_000_000, spool_capacity=1_000_000)
    devices = [SimulatedDevice(f"dev-{i}", protocol="register")
               for i in range(4)]
    errors = []

    def worker(dev):
        try:
            for s in range(300):
                gw.ingest(dev.emit(s))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(d,)) for d in devices]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    for i in range(4):
        assert gw.downstream.delivered_seqs(f"dev-{i}") == list(range(300))
    st = gw.stats_snapshot()
    assert st.accepted == 1200 and st.duplicate == 0
    assert all(wm == 299 for wm in st.watermarks.values())


def test_downstream_flapping_then_recovery_loses_nothing_buffered():
    """中断期间若发生恢复尝试且再次失败，已成功部分不重复、剩余保序。"""
    gw = Gateway(window_size=16, spool_capacity=10)
    dev = SimulatedDevice("d", protocol="register")
    gw.downstream.set_available(False)
    for s in range(5):
        gw.ingest(dev.emit(s))
    gw.downstream.set_available(True)
    call = {"n": 0}
    real_deliver = gw.downstream.deliver

    def flapping(reading):
        call["n"] += 1
        if call["n"] == 3:
            # 恢复补发第 3 条时链路再次中断：明确抛异常，剩余必须保留
            raise DownstreamUnavailable("re-outage during flush")
        real_deliver(reading)

    n = gw.spool.drain(flapping)
    assert n == 2
    assert [r.seq for r in gw.spool._q] == [2, 3, 4]
    n2 = gw.on_downstream_recovered()
    assert n2 == 3
    assert gw.downstream.delivered_seqs("d") == [0, 1, 2, 3, 4]
    assert len(gw.downstream.received_keys) == 5


def test_unknown_protocol_is_rejected_not_raised():
    from edge_gateway.devices import RawMessage

    gw = Gateway()
    result = gw.ingest(RawMessage(
        protocol="mystery", payload=b"\x00", label="x",
    ))
    assert not result.ok and result.reason is RejectReason.MALFORMED
    assert DownstreamUnavailable  # import sanity
