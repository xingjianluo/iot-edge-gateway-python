"""幂等、乱序窗口、单调水位与并发测试。

每个用例打印序号、水位（前后）与判定依据，满足可复现日志要求。
"""

import threading

import pytest

from edge_gateway.dedup import DeviceSequencer
from edge_gateway.models import RejectReason


def _log(device, seq, decision):
    print(
        f"  device={device} seq={seq:3d} -> "
        f"{'ACCEPT' if decision.accepted else 'REJECT'} "
        f"{decision.reason.value if decision.reason else '-':15s} "
        f"wm {decision.watermark_before}->{decision.watermark_after}",
    )


def test_forward_and_gap_advance_watermark_monotonically():
    sq = DeviceSequencer(window_size=4)
    for seq, wm in [(0, 0), (2, 2), (5, 5)]:
        d = sq.check("d", seq)
        _log("d", seq, d)
        assert d.accepted and d.watermark_after == wm
    # 水位只增不减
    d = sq.check("d", 3)
    _log("d", 3, d)
    assert d.accepted and d.watermark_after == 5
    assert sq.watermark("d") == 5


def test_duplicate_counts_once():
    sq = DeviceSequencer(window_size=4)
    assert sq.check("d", 1).accepted
    for _ in range(3):
        d = sq.check("d", 1)
        _log("d", 1, d)
        assert not d.accepted
        assert d.reason is RejectReason.DUPLICATE_SEQ
    assert sq.watermark("d") == 1


def test_out_of_order_within_window_accepted():
    sq = DeviceSequencer(window_size=4)
    for s in (10, 9, 8):
        assert sq.check("d", s).accepted
    d = sq.check("d", 7)  # low = 10-4+1 = 7，边界内
    _log("d", 7, d)
    assert d.accepted and d.watermark_after == 10
    # 补缺后再发仍是重复
    assert sq.check("d", 7).reason is RejectReason.DUPLICATE_SEQ


def test_out_of_window_vs_rollback_are_distinguishable():
    sq = DeviceSequencer(window_size=4)
    for s in (10, 9, 8, 7):
        sq.check("d", s)
    # 7 -> 6：观察到序号倒退 => SEQ_ROLLBACK
    d = sq.check("d", 6)
    _log("d", 6, d)
    assert d.reason is RejectReason.SEQ_ROLLBACK
    # 1 相对 6 倒退 => ROLLBACK；随后 2、3 不倒退但已超窗 => OUT_OF_WINDOW
    assert sq.check("d", 1).reason is RejectReason.SEQ_ROLLBACK
    for s in (2, 3):
        d = sq.check("d", s)
        _log("d", s, d)
        assert d.reason is RejectReason.OUT_OF_WINDOW


def test_pruned_seqs_are_treated_as_late():
    sq = DeviceSequencer(window_size=3)
    for s in (10, 11, 12):
        sq.check("d", s)
    # 窗口 [10,12]。补发更旧且不倒退的链路 8,9：
    d = sq.check("d", 8)
    assert d.reason in (
        RejectReason.SEQ_ROLLBACK, RejectReason.OUT_OF_WINDOW,
    )
    d = sq.check("d", 9)  # 8 -> 9 单调但仍超窗
    _log("d", 9, d)
    assert d.reason is RejectReason.OUT_OF_WINDOW
    # 窗口成员即使曾被接受，推进出窗后也算超窗/回退而非 DUPLICATE
    d = sq.check("d", 10)  # low=10 仍在窗内
    assert d.reason is RejectReason.DUPLICATE_SEQ


def test_per_device_independent_watermarks_and_locks():
    sq = DeviceSequencer(window_size=4)
    assert sq.check("a", 100).accepted
    assert sq.check("b", 1).accepted
    assert sq.watermark("a") == 100
    assert sq.watermark("b") == 1
    assert sq.check("a", 100).reason is RejectReason.DUPLICATE_SEQ
    assert sq.check("b", 1).reason is RejectReason.DUPLICATE_SEQ


def test_concurrent_same_device_exactly_once():
    sq = DeviceSequencer(window_size=100000)
    totals = []
    lock = threading.Lock()

    def worker():
        accepted = 0
        for i in range(1000):
            if sq.check("c", i).accepted:
                accepted += 1
        with lock:
            totals.append(accepted)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sq.watermark("c") == 999
    assert sum(totals) == 1000  # 4000 次提交恰好 1000 接受
    print("  accepted per thread:", sorted(totals))


def test_invalid_window_size_and_seq():
    with pytest.raises(ValueError):
        DeviceSequencer(window_size=0)
    sq = DeviceSequencer(window_size=4)
    with pytest.raises(ValueError):
        sq.check("d", -1)
    with pytest.raises(ValueError):
        sq.check("d", True)
