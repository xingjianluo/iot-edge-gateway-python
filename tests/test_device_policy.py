"""按设备差异化策略：窗口/缓冲容量/溢出策略各按设备、缺省回退、互不影响。"""

from edge_gateway.devices import SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policy import DevicePolicy


def _gw_with_policies(**overrides):
    return Gateway(
        window_size=100,
        spool_capacity=100,
        overflow_policy="drop_newest",
        device_policies=overrides,
    )


def test_resolver_defaults_and_stable_per_device():
    gw = _gw_with_policies(
        **{
            "dev-small": DevicePolicy(window_size=2),
            "dev-tiny-buf": DevicePolicy(spool_capacity=1),
            "dev-oldest": DevicePolicy(
                spool_capacity=2, overflow_policy="drop_oldest",
            ),
        }
    )
    p = gw.policies
    # 未覆盖设备回退全局默认；同一设备多次解析结果稳定
    for _ in range(3):
        assert p.window_for("dev-x") == 100
        assert p.spool_capacity_for("dev-x") == 100
        assert p.overflow_policy_for("dev-x") == "drop_newest"
    assert p.window_for("dev-small") == 2
    # 部分覆盖：没覆盖的项继续回退默认
    assert p.spool_capacity_for("dev-small") == 100
    assert p.window_for("dev-tiny-buf") == 100
    assert p.spool_capacity_for("dev-tiny-buf") == 1
    assert p.overflow_policy_for("dev-oldest") == "drop_oldest"
    print(f"  known devices: {p.known_devices()}")


def test_window_is_enforced_per_device():
    # dev-small 窗口=2，水位到 5 后 low=4；默认设备窗口 100 仍收同样旧序号
    gw = _gw_with_policies(**{"dev-small": DevicePolicy(window_size=2)})
    small = SimulatedDevice("dev-small", protocol="register")
    wide = SimulatedDevice("dev-wide", protocol="register")
    for s in (3, 4, 5):
        assert gw.ingest(small.emit(s)).ok
    # 宽窗口设备只推进水位到 5（跳号，3 未见过）
    assert gw.ingest(wide.emit(5)).ok
    late_small = gw.ingest(small.emit(3))  # 窗口 [4,5]，3 超窗
    late_wide = gw.ingest(wide.emit(3))  # 大窗口内补缺，接受
    assert not late_small.ok
    assert late_small.reason in (
        RejectReason.OUT_OF_WINDOW, RejectReason.SEQ_ROLLBACK,
    )
    assert late_wide.ok
    print(f"  small: {late_small.reason.value} ({late_small.detail})")
    print(f"  wide: accepted wm={late_wide.watermark}")


def test_window_decision_is_stable_across_reconnections():
    """同一设备不管第几条、哪一次接入，窗口判定尺度一致。"""
    gw = _gw_with_policies(**{"dev-a": DevicePolicy(window_size=3)})
    dev = SimulatedDevice("dev-a", protocol="register")
    for s in (10, 11, 12):
        assert gw.ingest(dev.emit(s)).ok
    # 模拟"重新接入"：设备对象重建，device_id 不变 -> 策略不变
    dev_again = SimulatedDevice("dev-a", protocol="structured")
    r1 = gw.ingest(dev_again.emit(10))  # 重复
    r2 = gw.ingest(dev_again.emit(9))   # 窗口 [10,12]，9 超窗
    assert r1.reason is RejectReason.DUPLICATE_SEQ
    assert r2.reason in (
        RejectReason.OUT_OF_WINDOW, RejectReason.SEQ_ROLLBACK,
    )
    assert "window=3" in r2.detail


def test_spool_capacity_is_independent_between_devices():
    gw = _gw_with_policies(**{
        "dev-a": DevicePolicy(spool_capacity=2),
        "dev-b": DevicePolicy(spool_capacity=3),
    })
    a = SimulatedDevice("dev-a", protocol="register")
    b = SimulatedDevice("dev-b", protocol="register")
    gw.downstream.set_available(False)
    assert gw.ingest(a.emit(0)).buffered
    assert gw.ingest(a.emit(1)).buffered
    overflow_a = gw.ingest(a.emit(2))
    assert overflow_a.reason is RejectReason.BUFFER_OVERFLOW
    assert "capacity=2" in overflow_a.detail
    # dev-b 的容量不被 dev-a 占用影响
    for s in range(3):
        assert gw.ingest(b.emit(s)).buffered
    assert gw.ingest(b.emit(3)).reason is RejectReason.BUFFER_OVERFLOW
    assert gw.spool.snapshot_stats().size == 5  # 2 + 3


def test_overflow_policy_per_device_drop_oldest():
    gw = _gw_with_policies(**{
        "dev-a": DevicePolicy(
            spool_capacity=2, overflow_policy="drop_oldest",
        ),
        "dev-b": DevicePolicy(spool_capacity=5),  # 默认 drop_newest
    })
    a = SimulatedDevice("dev-a", protocol="register")
    b = SimulatedDevice("dev-b", protocol="register")
    gw.downstream.set_available(False)
    for s in range(4):  # a: 0,1 被丢 -> 留 2,3
        assert gw.ingest(a.emit(s)).ok
    for s in range(5):
        assert gw.ingest(b.emit(s)).buffered
    # b 仍是默认 drop_newest：第 6 条溢出拒绝
    assert gw.ingest(b.emit(5)).reason is RejectReason.BUFFER_OVERFLOW
    gw.downstream.set_available(True)
    gw.on_downstream_recovered()
    # 全局 FIFO：a 的 2,3 先发，然后 b 的 0..4
    assert gw.downstream.delivered_seqs("dev-a") == [2, 3]
    assert gw.downstream.delivered_seqs("dev-b") == [0, 1, 2, 3, 4]
    assert gw.spool.stats.dropped_overflow == 3  # a 丢 2 条 + b 拒 1 条
