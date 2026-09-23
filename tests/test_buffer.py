"""有界缓冲：上限、两种溢出策略、统计可查询、退回保序。"""
import pytest

from gateway.buffer import BoundedBuffer, OverflowPolicy
from gateway.model import Reading


def _r(seq, dev="d"):
    return Reading(dev, seq, "m", float(seq), 0.0, "json")


def test_drop_newest_rejects_when_full():
    buf = BoundedBuffer(2, OverflowPolicy.DROP_NEWEST)
    assert buf.put(_r(1))[0]
    assert buf.put(_r(2))[0]
    stored, victim = buf.put(_r(3))
    assert stored is False and victim.seq == 3
    assert [r.seq for r in buf.drain()] == [1, 2]
    s = buf.stats()
    assert s["dropped_newest"] == 1 and s["dropped_oldest"] == 0
    assert s["last_dropped"] == ("d", 3)


def test_drop_oldest_evicts_when_full():
    buf = BoundedBuffer(2, OverflowPolicy.DROP_OLDEST)
    buf.put(_r(1))
    buf.put(_r(2))
    stored, victim = buf.put(_r(3))
    assert stored is True and victim.seq == 1
    assert [r.seq for r in buf.drain()] == [2, 3]
    assert buf.stats()["dropped_oldest"] == 1


def test_capacity_hard_limit():
    buf = BoundedBuffer(3, OverflowPolicy.DROP_NEWEST)
    for i in range(10):
        buf.put(_r(i))
    assert len(buf) == 3


def test_drain_is_fifo():
    buf = BoundedBuffer(5, OverflowPolicy.DROP_NEWEST)
    for i in range(5):
        buf.put(_r(i))
    assert [r.seq for r in buf.drain(2)] == [0, 1]
    assert [r.seq for r in buf.drain()] == [2, 3, 4]


def test_restore_front_preserves_order():
    buf = BoundedBuffer(5, OverflowPolicy.DROP_NEWEST)
    buf.put(_r(10))
    buf.restore_front([_r(1), _r(2)])  # 补发途中再次中断退回
    assert [r.seq for r in buf.peek_all()] == [1, 2, 10]


def test_invalid_capacity():
    with pytest.raises(ValueError):
        BoundedBuffer(0, OverflowPolicy.DROP_NEWEST)
