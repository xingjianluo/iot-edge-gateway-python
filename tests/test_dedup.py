"""序号幂等、乱序窗口、单调水位的单元语义。"""
import pytest

from gateway.dedup import DeviceWatermarkState
from gateway.model import RejectReason


def test_first_frame_sets_baseline():
    s = DeviceWatermarkState(window=2)
    ok, reason, _, wm = s.inspect(5)
    assert ok and reason is None and wm == -1
    assert s.commit(5) == [5]
    assert s.watermark == 5  # 基线 4，首个提交点 5


def test_duplicate_equal_watermark():
    s = DeviceWatermarkState(window=2)
    s.commit(1)
    ok, reason, _, wm = s.inspect(1)
    assert not ok and reason is RejectReason.DUPLICATE_SEQ and wm == 1


def test_duplicate_in_pending():
    s = DeviceWatermarkState(window=5)
    s.commit(1)
    s.commit(3)  # 乱序缓存，水位仍 1
    assert s.watermark == 1
    ok, reason, _, _ = s.inspect(3)
    assert not ok and reason is RejectReason.DUPLICATE_SEQ


def test_rollback_below_watermark():
    s = DeviceWatermarkState(window=5)
    s.commit(10)
    ok, reason, _, wm = s.inspect(9)
    assert not ok and reason is RejectReason.SEQ_ROLLBACK and wm == 10


def test_out_of_window_upper_bound():
    s = DeviceWatermarkState(window=2)
    s.commit(1)  # 下一个期望 2，上界 2+2=4
    assert s.inspect(4)[0] is True
    ok, reason, _, _ = s.inspect(5)
    assert not ok and reason is RejectReason.OUT_OF_WINDOW


def test_watermark_advances_monotonically_on_gap_fill():
    s = DeviceWatermarkState(window=5)
    s.commit(1)
    assert s.commit(4) == []        # 2,3 未到，不推进
    assert s.watermark == 1
    assert s.commit(2) == [2]       # 3 仍缺
    assert s.watermark == 2
    assert s.commit(3) == [3, 4]    # 空洞补齐，连续推进 3、4
    assert s.watermark == 4


def test_watermark_never_decreases():
    s = DeviceWatermarkState(window=10)
    for seq in (1, 5, 3, 2, 4):
        ok, _, _, _ = s.inspect(seq)
        assert ok
        s.commit(seq)
        assert s.watermark >= 1
    assert s.watermark == 5


def test_negative_window_rejected():
    with pytest.raises(ValueError):
        DeviceWatermarkState(window=-1)
