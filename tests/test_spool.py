"""边界缓冲：容量上限、溢出策略、失败保留、按序补发与统计。"""

import threading

import pytest

from edge_gateway.buffer import BoundedSpool, OverflowPolicy
from edge_gateway.models import Reading, RejectReason


def _rd(seq, device="d"):
    return Reading(
        device_id=device, seq=seq, timestamp_ms=seq, metric="m",
        value=float(seq), source="register",
    )


def test_drop_newest_rejects_and_counts():
    sp = BoundedSpool(capacity=3)
    for s in range(3):
        assert sp.put(_rd(s)) == (True, None)
    ok, reason = sp.put(_rd(3))
    assert (ok, reason) == (False, RejectReason.BUFFER_OVERFLOW)
    st = sp.snapshot_stats()
    assert (st.size, st.enqueued, st.dropped_overflow) == (3, 3, 1)
    assert st.capacity == 3


def test_drop_oldest_evicts_deterministically():
    sp = BoundedSpool(capacity=3, overflow_policy=OverflowPolicy.DROP_OLDEST)
    for s in range(6):
        assert sp.put(_rd(s)) == (True, None)
    seqs = []
    n = sp.drain(lambda r: seqs.append(r.seq))
    assert n == 3 and seqs == [3, 4, 5]
    assert sp.snapshot_stats().dropped_overflow == 3


def test_drain_is_fifo_and_idempotent():
    sp = BoundedSpool(capacity=10)
    for s in range(5):
        sp.put(_rd(s))
    seqs = []
    assert sp.drain(lambda r: seqs.append(r.seq)) == 5
    assert seqs == [0, 1, 2, 3, 4]
    # 已投递过的读数重新进入 spool 不重复计数
    assert sp.put(_rd(2))[0] is True
    st = sp.snapshot_stats()
    assert st.duplicate_delivery == 1
    assert sp.drain(lambda r: pytest.fail("should not redeliver")) == 0
    assert sp.stats.delivered == 5


def test_failure_keeps_head_and_resumes_in_order():
    sp = BoundedSpool(capacity=10)
    for s in range(4):
        sp.put(_rd(s))

    attempts = []

    def flaky(reading):
        attempts.append(reading.seq)
        if reading.seq == 1:
            raise RuntimeError("downstream down")

    assert sp.drain(flaky) == 1  # 0 投递成功，1 失败即停
    assert attempts == [0, 1]
    assert [r.seq for r in sp._q] == [1, 2, 3]
    assert sp.snapshot_stats().downstream_failures == 1

    seqs = []
    assert sp.drain(lambda r: seqs.append(r.seq)) == 3
    assert seqs == [1, 2, 3]
    assert sp.stats.delivered == 4
    assert sp.snapshot_stats().size == 0


def test_concurrent_put_and_drain_consistency():
    sp = BoundedSpool(capacity=10000)
    errors = []

    def putter(start):
        try:
            for s in range(start, start + 500):
                sp.put(_rd(s))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [
        threading.Thread(target=putter, args=(i * 100000,))
        for i in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    st = sp.snapshot_stats()
    assert st.size + st.delivered == st.enqueued
    # 按序补发
    seqs = []
    sp.drain(lambda r: seqs.append((r.device_id, r.seq)))
    assert seqs == sorted(seqs)
    assert sp.snapshot_stats().size == 0


def test_invalid_args():
    with pytest.raises(ValueError):
        BoundedSpool(capacity=0)
    with pytest.raises(ValueError):
        BoundedSpool(capacity=10, overflow_policy="nope")
