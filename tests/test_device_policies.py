"""按设备差异化策略：窗口/缓冲容量/溢出策略按设备生效，缺省回退默认。

覆盖：解析稳定性与缺省回退、设备间互不影响、同一设备跨接入/跨消息一致、
配置与环境变量解析及非法配置拒绝。
"""

import json

import pytest

from edge_gateway.buffer import OverflowPolicy
from edge_gateway.config import GatewayConfig, parse_device_policies
from edge_gateway.dedup import DeviceSequencer
from edge_gateway.devices import SimulatedDevice, auto_message
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policies import DevicePolicy, PolicyResolver


def _feed(gw, msg):
    r = gw.ingest(msg)
    reason = r.reason.value if r.reason else "-"
    print(
        f"  {msg.label:16s} seq={str(r.seq):>3s} wm={str(r.watermark):>3s} "
        f"{'ACCEPT' if r.ok else 'REJECT'} reason={reason:18s} "
        f"buffered={r.buffered}",
    )
    return r


# ---------- 解析器：稳定、回退、互不影响 ----------

def test_resolver_falls_back_to_defaults_and_is_stable():
    resolver = PolicyResolver(
        default_window_size=32, default_spool_capacity=100,
        default_overflow_policy=OverflowPolicy.DROP_NEWEST,
        device_policies={
            "tight": DevicePolicy(window_size=2),
            "small-old": DevicePolicy(
                spool_capacity=3, overflow_policy=OverflowPolicy.DROP_OLDEST,
            ),
        },
    )
    # 未配置设备：全部走默认
    assert resolver.resolve("any-other").window_size == 32
    assert resolver.resolve("any-other").spool_capacity == 100
    assert resolver.resolve("any-other").overflow_policy == "drop_newest"
    # 部分覆盖：未覆盖项仍回退默认
    got = resolver.resolve("tight")
    assert (got.window_size, got.spool_capacity, got.overflow_policy) == (
        2, 100, "drop_newest",
    )
    got = resolver.resolve("small-old")
    assert (got.window_size, got.spool_capacity, got.overflow_policy) == (
        32, 3, "drop_oldest",
    )
    # 稳定性：同一设备无论查多少次结论一致
    assert [resolver.resolve("tight").window_size for _ in range(5)] == [2] * 5


def test_invalid_policy_values_rejected():
    with pytest.raises(ValueError):
        DevicePolicy(window_size=0).validated()
    with pytest.raises(ValueError):
        DevicePolicy(spool_capacity=0).validated()
    with pytest.raises(ValueError):
        DevicePolicy(overflow_policy="nope").validated()
    with pytest.raises(ValueError):
        PolicyResolver(
            default_window_size=0, default_spool_capacity=1,
            default_overflow_policy="drop_newest",
        )
    with pytest.raises(ValueError):
        PolicyResolver(
            default_window_size=1, default_spool_capacity=1,
            default_overflow_policy="drop_newest",
            device_policies={"": DevicePolicy()},
        )


# ---------- 时序层：按设备窗口 ----------

def test_sequencer_uses_per_device_window_independently():
    resolver = PolicyResolver(
        default_window_size=10, default_spool_capacity=10,
        default_overflow_policy="drop_newest",
        device_policies={"tight": DevicePolicy(window_size=2)},
    )
    sq = DeviceSequencer(10, window_for=resolver.window_for)
    # tight：窗口 2，水位到 5 后窗内只剩 [4,5]
    for s in (3, 4, 5):
        assert sq.check("tight", s).accepted
    # 先制造一个更小的 last_seen（回退），随后 3 单调回升但仍超窗
    assert sq.check("tight", 0).reason is RejectReason.SEQ_ROLLBACK
    d = sq.check("tight", 3)
    print(f"  tight seq=3 -> {d.reason} window={d.window_size}")
    assert d.reason is RejectReason.OUT_OF_WINDOW
    assert d.window_size == 2
    # wide（默认窗口 10）：同样的序号仍在窗内 -> 重复而非超窗
    for s in (3, 4, 5):
        sq.check("wide", s)
    d2 = sq.check("wide", 3)
    print(f"  wide  seq=3 -> {d2.reason} window={d2.window_size}")
    assert d2.reason is RejectReason.DUPLICATE_SEQ
    assert d2.window_size == 10
    # tight 与 wide 水位互不影响
    assert sq.watermark("tight") == sq.watermark("wide") == 5


# ---------- 缓冲层：按设备容量与溢出策略 ----------

def test_spool_capacity_is_per_device_not_global():
    gw = Gateway(
        config=GatewayConfig(
            window_size=100, spool_capacity=2,
            device_policies={"big": DevicePolicy(spool_capacity=5)},
        ),
    )
    gw.downstream.set_available(False)
    big = SimulatedDevice("big", protocol="register")
    small = SimulatedDevice("small", protocol="register")
    for s in range(5):
        assert _feed(gw, big.emit(s)).buffered  # big 容量 5，全收
    # small 容量 2（默认）：第 3 条溢出，且不被 big 的积压挤占
    assert _feed(gw, small.emit(0)).buffered
    assert _feed(gw, small.emit(1)).buffered
    overflow = _feed(gw, small.emit(2))
    assert not overflow.ok
    assert overflow.reason is RejectReason.BUFFER_OVERFLOW
    assert "capacity=2" in overflow.detail
    assert gw.spool.snapshot_stats().size == 7


def test_spool_overflow_policy_is_per_device():
    gw = Gateway(
        config=GatewayConfig(
            window_size=100, spool_capacity=3,
            device_policies={
                "old": DevicePolicy(overflow_policy="drop_oldest"),
            },
        ),
    )
    gw.downstream.set_available(False)
    old = SimulatedDevice("old", protocol="register")
    new = SimulatedDevice("new", protocol="register")
    for s in range(5):
        gw.ingest(old.emit(s))  # drop_oldest：保留 2,3,4
    for s in range(4):
        gw.ingest(new.emit(s))  # drop_newest（默认）：保留 0,1,2，3 被拒
    gw.downstream.set_available(True)
    gw.on_downstream_recovered()
    # 全局 FIFO：old 保留的先补发，随后是 new 的 0,1,2
    assert gw.downstream.delivered_seqs("old") == [2, 3, 4]
    assert gw.downstream.delivered_seqs("new") == [0, 1, 2]
    # old 丢了 2 条（0,1），new 丢了 1 条（3）
    assert gw.spool.snapshot_stats().dropped_overflow == 3


# ---------- 网关整合：同一设备每次接入策略一致，自动识别也生效 ----------

def test_gateway_policy_stable_across_messages_and_auto_ingest():
    policy = {"tight": DevicePolicy(window_size=2, spool_capacity=2)}
    gw = Gateway(
        config=GatewayConfig(
            window_size=8, spool_capacity=8, device_policies=policy,
        ),
    )
    # 显式 register 与 auto 两条路径交错，同一设备拿到的窗口都是 2
    dev = SimulatedDevice("tight", protocol="register")
    r1 = gw.ingest(auto_message(dev.emit(0).payload, "auto#0"))
    r2 = gw.ingest(dev.emit(1))  # 显式
    assert r1.ok and r2.ok
    # 再用 auto 发一条超窗数据：水位 1，窗口 [0,1]，seq 之外
    late = gw.ingest(auto_message(
        SimulatedDevice("tight", protocol="structured").emit(5).payload,
        "auto-json#5",
    ))
    assert late.ok and late.watermark == 5
    rollback = gw.ingest(auto_message(
        SimulatedDevice("tight", protocol="structured").emit(2).payload,
        "auto-json#2",
    ))
    print(
        f"  effective window for tight: "
        f"{gw.policy_resolver.resolve('tight').window_size}",
    )
    # 窗口 2：low=5-2+1=4，seq=2 超窗
    assert not rollback.ok
    assert rollback.reason in (
        RejectReason.OUT_OF_WINDOW, RejectReason.SEQ_ROLLBACK,
    )


def test_unconfigured_device_uses_global_defaults():
    gw = Gateway(window_size=7, spool_capacity=9, overflow_policy="drop_oldest")
    resolved = gw.policy_resolver.resolve("never-seen")
    assert (resolved.window_size, resolved.spool_capacity,
            resolved.overflow_policy) == (7, 9, "drop_oldest")


# ---------- 配置/环境变量 ----------

def test_parse_device_policies_env_json():
    raw = json.dumps({
        "line-a": {"window_size": 4, "spool_capacity": 8,
                   "overflow_policy": "drop_oldest"},
        "line-b": {"window_size": 2},
    })
    policies = parse_device_policies(raw)
    assert policies["line-a"] == DevicePolicy(4, 8, "drop_oldest")
    assert policies["line-b"] == DevicePolicy(window_size=2)
    assert parse_device_policies(None) == {}
    assert parse_device_policies("  ") == {}


@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        "[]",
        '{"a": 1}',
        '{"a": {"window_size": 0}}',
        '{"a": {"bogus": 1}}',
        '{"a": {"overflow_policy": "nope"}}',
    ],
)
def test_parse_device_policies_rejects_bad_input(raw):
    with pytest.raises(ValueError):
        parse_device_policies(raw)


def test_config_from_env_with_device_policies(monkeypatch):
    monkeypatch.setenv(
        "GW_DEVICE_POLICIES",
        json.dumps({"d": {"spool_capacity": 1}}),
    )
    from edge_gateway.config import config_from_env

    cfg = config_from_env()
    assert cfg.device_policies["d"].spool_capacity == 1
    assert cfg.device_policies["d"].window_size is None  # 回退默认
