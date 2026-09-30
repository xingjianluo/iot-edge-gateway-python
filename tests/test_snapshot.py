"""重启后接着跑：状态快照落盘/恢复，水位/序号集/缓存/计数接得上。

快照只含纯运行状态（v2）：不含窗口/容量/溢出策略等策略设置，也绝不含
凭据类内容；恢复时做严格白名单 + 凭据键扫描，污染的快照必须被拒绝。
"""

import copy
import json

import pytest

from edge_gateway.config import GatewayConfig
from edge_gateway.devices import SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policy import DevicePolicy
from edge_gateway.snapshot import (
    SNAPSHOT_FORMAT,
    SNAPSHOT_VERSION,
    SnapshotError,
    load_snapshot,
    restore_gateway,
    save_snapshot,
    state_to_gateway,
)


def _build_prefailure_gateway():
    """构造一个"刚中断"的现场：部分已投递、部分缓存排队、含一次重复与拒绝。"""
    gw = Gateway(window_size=8, spool_capacity=10)
    dev = SimulatedDevice("line", protocol="register")
    gw.ingest(dev.emit(0))          # 已投递
    gw.ingest(dev.emit(1))
    gw.ingest(dev.emit(1))          # 重复（重启前已计数）
    gw.downstream.set_available(False)
    for s in (2, 3, 4):             # 排队中，尚未投递
        gw.ingest(dev.emit(s))
    gw.ingest(dev.emit(0))          # 缓存期间重复
    return gw, dev


def test_state_export_contains_only_runtime_state_without_credentials():
    gw, _ = _build_prefailure_gateway()
    state = gw.export_state()
    assert set(state) == {"stats", "sequencer", "spool"}
    seq_state = state["sequencer"]["line"]
    assert seq_state["watermark"] == 4
    assert seq_state["seen"] == [0, 1, 2, 3, 4]
    assert seq_state["last_seen_seq"] == 0
    queued = [(q["device_id"], q["seq"]) for q in state["spool"]["queued"]]
    assert queued == [("line", 2), ("line", 3), ("line", 4)]
    assert state["stats"]["accepted"] == 5
    assert state["stats"]["duplicate"] == 2
    # 白名单：不允许夹带凭据类字段
    blob = json.dumps(state, ensure_ascii=False)
    for forbidden in ("password", "passwd", "secret", "token", "api_key",
                      "credential", "SUPERSECRETVALUE"):
        assert forbidden not in blob.lower()
    print(f"  state keys={sorted(state)} queued={queued}")


def test_restore_in_new_process_picks_up_watermark_seen_spool_and_counts():
    gw, dev = _build_prefailure_gateway()
    state = gw.export_state()

    # 全新对象（等价全新进程），相同配置恢复
    gw2 = Gateway(window_size=8, spool_capacity=10)
    gw2.load_state(state)
    st = gw2.stats_snapshot()
    assert st.watermarks["line"] == 4
    assert st.accepted == 5 and st.duplicate == 2
    assert gw2.spool.snapshot_stats().size == 3

    # 重放：已投递的 0/1 与排队中的 2/3/4 全部被幂等拦截
    for s in range(5):
        r = gw2.ingest(dev.emit(s))
        assert r.reason is RejectReason.DUPLICATE_SEQ, (s, r.reason)
    st2 = gw2.stats_snapshot()
    assert st2.watermarks["line"] == 4          # 水位不倒退
    assert st2.delivered == 2                   # 不重复计投递
    assert gw2.downstream.count() == 0          # 下游仍中断，零新增投递

    # 恢复补发：排队读数按 FIFO 恰好投递一次（新进程下游此前为空，
    # 只看到恢复出的 2,3,4；0,1 在重启前已投递，不重复投递）
    gw2.downstream.set_available(True)
    n = gw2.on_downstream_recovered()
    assert n == 3
    assert gw2.downstream.delivered_seqs("line") == [2, 3, 4]
    # 再来一次重放+补发：下游集合不变、计数不变
    gw2.downstream.set_available(False)
    for s in range(5):
        gw2.ingest(dev.emit(s))
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("line") == [2, 3, 4]


def test_spool_delivered_keys_restored_as_idempotency_guard():
    gw, _dev = _build_prefailure_gateway()
    state = gw.export_state()
    gw2 = Gateway(window_size=8, spool_capacity=10)
    gw2.load_state(state)
    sp = gw2.spool.snapshot_stats()
    assert sp.enqueued == 3 and sp.size == 3
    # 排队队列里的读数字段完整还原
    first = gw2.spool._q[0]
    assert (first.device_id, first.seq, first.metric) == ("line", 2, "temperature")


def test_snapshot_file_roundtrip_and_atomic_restore(tmp_path):
    gw, _dev = _build_prefailure_gateway()
    path = tmp_path / "gw-state.json"
    save_snapshot(gw, path)
    on_disk = load_snapshot(path)
    assert on_disk["format"] == SNAPSHOT_FORMAT
    assert on_disk["version"] == SNAPSHOT_VERSION
    # 快照只含纯运行状态 + 外壳：策略设置（窗口/容量/溢出/设备覆盖）不得出现
    assert set(on_disk) == {
        "format", "version", "taken_at_ms", "stats", "sequencer", "spool",
    }
    assert "config" not in on_disk
    blob = json.dumps(on_disk)
    for forbidden in ("window_size", "spool_capacity", "overflow_policy",
                      "device_policies", "password", "token", "secret"):
        assert forbidden not in blob

    # 全新进程式恢复：策略由恢复进程自己的配置显式提供
    cfg = GatewayConfig(window_size=8, spool_capacity=10)
    gw2 = restore_gateway(on_disk, config=cfg)
    assert gw2.stats_snapshot().watermarks["line"] == 4
    gw2.downstream.set_available(True)
    assert gw2.on_downstream_recovered() == 3
    assert gw2.downstream.delivered_seqs("line") == [2, 3, 4]

    # 原子落盘：目标路径存在且是完整 JSON，同目录无残留临时文件
    json.loads(path.read_text(encoding="utf-8"))
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name.startswith(".gw-snapshot-")]
    assert leftovers == []


def test_restore_without_config_falls_back_to_process_env(tmp_path):
    """不给 config 时按恢复进程自己的环境配置建网关（策略不来自快照）。"""
    gw, _ = _build_prefailure_gateway()
    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(load_snapshot(tmp_path / "s.json"))
    # 默认环境配置（32/100）生效；运行状态照样恢复
    assert gw2.config.window_size == 32
    assert gw2.stats_snapshot().watermarks["line"] == 4


def test_snapshot_restores_runtime_with_per_device_policy_config(tmp_path):
    """按设备策略来自恢复进程配置：相同设备覆盖恢复后容量约束继续生效。"""
    cfg = GatewayConfig(
        window_size=100, spool_capacity=100,
        device_policies={"dev-a": DevicePolicy(spool_capacity=1)},
    )
    gw = Gateway(cfg)
    a = SimulatedDevice("dev-a", protocol="register")
    b = SimulatedDevice("dev-b", protocol="register")
    gw.downstream.set_available(False)
    gw.ingest(a.emit(0))
    gw.ingest(b.emit(0))
    save_snapshot(gw, tmp_path / "s.json")

    # 新进程拿着同一份策略配置恢复；快照里没有策略，覆盖不来自快照
    gw2 = restore_gateway(load_snapshot(tmp_path / "s.json"), config=cfg)
    gw2.downstream.set_available(False)
    assert gw2.policies.spool_capacity_for("dev-a") == 1
    assert gw2.policies.spool_capacity_for("dev-b") == 100
    assert gw2.ingest(a.emit(1)).reason is RejectReason.BUFFER_OVERFLOW
    assert gw2.ingest(b.emit(1)).buffered
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("dev-a") == [0]
    assert gw2.downstream.delivered_seqs("dev-b") == [0, 1]


def test_snapshot_without_device_policy_falls_back_to_global_default(tmp_path):
    """新进程未配置设备覆盖时，该设备回退全局缺省，运行状态仍正常恢复。"""
    gw = Gateway(
        GatewayConfig(
            window_size=100, spool_capacity=1,
            device_policies={"dev-a": DevicePolicy(spool_capacity=1)},
        ),
    )
    a = SimulatedDevice("dev-a", protocol="register")
    gw.downstream.set_available(False)
    gw.ingest(a.emit(0))
    save_snapshot(gw, tmp_path / "s.json")

    # 恢复进程没有 dev-a 覆盖：回退全局容量 100（策略完全以进程配置为准）
    gw2 = restore_gateway(
        load_snapshot(tmp_path / "s.json"),
        config=GatewayConfig(window_size=100, spool_capacity=100),
    )
    gw2.downstream.set_available(False)
    assert gw2.policies.spool_capacity_for("dev-a") == 100
    assert gw2.ingest(a.emit(1)).buffered


def test_restore_continues_after_crash_with_auto_identified_frames(tmp_path):
    """重启后用"不声明协议"的原始字节重放，照样被幂等拦截。"""
    from edge_gateway.devices import RawMessage

    gw = Gateway(window_size=8, spool_capacity=10)
    reg = SimulatedDevice("dev-x", protocol="register")
    gw.ingest(reg.emit_auto(0))
    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(
        load_snapshot(tmp_path / "s.json"),
        config=GatewayConfig(window_size=8, spool_capacity=10),
    )

    replay = RawMessage(
        protocol="auto", payload=reg.emit(0).payload, label="auto-replay",
    )
    r = gw2.ingest(replay)
    assert r.reason is RejectReason.DUPLICATE_SEQ
    assert gw2.stats_snapshot().watermarks["dev-x"] == 0


# ---- 严格校验：未知字段 / 凭据污染必须被明确拒绝 ---------------------------

def _valid_state(tmp_path) -> dict:
    gw, _ = _build_prefailure_gateway()
    save_snapshot(gw, tmp_path / "s.json")
    return load_snapshot(tmp_path / "s.json")


def test_restore_rejects_unknown_top_level_field(tmp_path):
    state = _valid_state(tmp_path)
    state["extra"] = {"whatever": 1}
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="unknown field"):
        state_to_gateway(gw, state)


def test_restore_rejects_legacy_policy_section(tmp_path):
    """旧版快照里的 config 段在 v2 下属于未知字段，必须拒绝而不是照单全收。"""
    state = _valid_state(tmp_path)
    state["config"] = {
        "window_size": 8, "spool_capacity": 10,
        "overflow_policy": "drop_newest", "device_policies": {},
    }
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="unknown field"):
        state_to_gateway(gw, state)


@pytest.mark.parametrize("cred_key", [
    "password", "passwd", "secret", "token", "api_key", "credential",
])
def test_restore_rejects_credential_key_at_top_level(tmp_path, cred_key):
    state = _valid_state(tmp_path)
    state[cred_key] = "SUPERSECRETVALUE"
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="credential-like key"):
        state_to_gateway(gw, state)


def test_restore_rejects_credential_key_nested_inside_queued(tmp_path):
    state = _valid_state(tmp_path)
    state["spool"]["queued"][0]["access_token"] = "abc"
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="credential-like key"):
        state_to_gateway(gw, state)


def test_restore_rejects_credential_key_nested_inside_stats(tmp_path):
    state = _valid_state(tmp_path)
    state["stats"]["watermarks"]["line"] = 4
    state["stats"]["db_secret"] = "x"
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="credential-like key"):
        state_to_gateway(gw, state)


def test_restore_rejects_unknown_nested_field(tmp_path):
    state = _valid_state(tmp_path)
    state["spool"]["stats"]["bogus"] = 1
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="unknown field"):
        state_to_gateway(gw, state)


def test_restore_rejects_unknown_field_in_direct_load_state(tmp_path):
    """不经信封、直接灌运行状态时同样严格（export_state 形态）。"""
    clean = _valid_state(tmp_path)
    runtime = {k: copy.deepcopy(clean[k]) for k in ("stats", "sequencer", "spool")}
    runtime["stats"]["token"] = 1
    gw2 = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="credential-like key"):
        gw2.load_state(runtime)

    runtime2 = {k: copy.deepcopy(clean[k]) for k in ("stats", "sequencer", "spool")}
    runtime2["stats2"] = copy.deepcopy(clean["stats"])
    gw3 = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="unknown field"):
        gw3.load_state(runtime2)


def test_restore_rejects_bad_format_and_version(tmp_path):
    state = _valid_state(tmp_path)
    gw = Gateway(window_size=8, spool_capacity=10)
    state["format"] = "something-else"
    with pytest.raises(SnapshotError, match="not an edge gateway snapshot"):
        state_to_gateway(gw, state)
    state2 = _valid_state(tmp_path)
    state2["version"] = 99
    with pytest.raises(SnapshotError, match="unsupported snapshot version"):
        state_to_gateway(gw, state2)


def test_restore_rejects_wrong_type(tmp_path):
    state = _valid_state(tmp_path)
    state["stats"]["accepted"] = "five"
    gw = Gateway(window_size=8, spool_capacity=10)
    with pytest.raises(SnapshotError, match="accepted"):
        state_to_gateway(gw, state)


def test_legal_snapshot_still_restores_after_strict_validation(tmp_path):
    """严格校验不能把本来合法的快照弄坏。"""
    state = _valid_state(tmp_path)
    gw2 = Gateway(window_size=8, spool_capacity=10)
    state_to_gateway(gw2, state)  # 不抛异常即通过
    assert gw2.stats_snapshot().accepted == 5


# ---- 溢出丢弃计数：落盘前后、恢复之后必须对得上 ----------------------------

def test_overflow_drops_survive_snapshot_and_restore_drop_newest(tmp_path):
    """DROP_NEWEST 溢出拒绝的丢弃条数必须进快照并原样恢复。"""
    gw = Gateway(window_size=64, spool_capacity=2)
    dev = SimulatedDevice("d", protocol="register")
    gw.downstream.set_available(False)
    gw.ingest(dev.emit(0))
    gw.ingest(dev.emit(1))
    r2 = gw.ingest(dev.emit(2))      # 满 -> 拒绝，真实丢弃 1 条
    r3 = gw.ingest(dev.emit(3))      # 又丢弃 1 条
    assert r2.reason is RejectReason.BUFFER_OVERFLOW
    assert r3.reason is RejectReason.BUFFER_OVERFLOW

    before = gw.stats_snapshot()
    assert before.overflow_dropped == 2
    assert gw.spool.snapshot_stats().dropped_overflow == 2

    save_snapshot(gw, tmp_path / "s.json")
    on_disk = load_snapshot(tmp_path / "s.json")
    # 快照里记录的丢弃条数不能是 0
    assert on_disk["stats"]["overflow_dropped"] == 2
    assert on_disk["spool"]["stats"]["dropped_overflow"] == 2

    gw2 = restore_gateway(
        on_disk, config=GatewayConfig(window_size=64, spool_capacity=2),
    )
    after = gw2.stats_snapshot()
    assert after.overflow_dropped == 2
    assert gw2.spool.snapshot_stats().dropped_overflow == 2
    # 其它计数也对得上
    assert after.delivered == before.delivered
    assert after.accepted == before.accepted
    assert after.rejected == before.rejected


def test_overflow_drops_survive_snapshot_and_restore_drop_oldest(tmp_path):
    """DROP_OLDEST 腾退旧读数也算丢弃，计数同样要进快照并恢复。"""
    gw = Gateway(
        window_size=64, spool_capacity=2, overflow_policy="drop_oldest",
    )
    dev = SimulatedDevice("d", protocol="register")
    gw.downstream.set_available(False)
    for s in range(5):
        gw.ingest(dev.emit(s))        # 容量 2：s=2,3,4 各腾退一条 -> 丢 3
    assert gw.stats_snapshot().overflow_dropped == 3

    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(
        load_snapshot(tmp_path / "s.json"),
        config=GatewayConfig(
            window_size=64, spool_capacity=2,
            overflow_policy="drop_oldest",
        ),
    )
    assert gw2.stats_snapshot().overflow_dropped == 3
    # 缓存里只剩 3,4：恢复后补发，0,1,2 不会复活、不会重复投递
    gw2.downstream.set_available(True)
    assert gw2.on_downstream_recovered() == 2
    assert gw2.downstream.delivered_seqs("d") == [3, 4]
    assert gw2.stats_snapshot().overflow_dropped == 3


def test_all_counter_classes_survive_snapshot_roundtrip(tmp_path):
    """投递/重复/超窗/丢弃/认不出/歧义各类计数重启前后一致。"""
    gw = Gateway(window_size=2, spool_capacity=2)
    reg = SimulatedDevice("d", protocol="register")
    from edge_gateway.devices import RawMessage
    from edge_gateway.protocols.identification import AUTO_PROTOCOL

    gw.ingest(reg.emit(0))           # delivered
    gw.ingest(reg.emit(0))           # duplicate
    gw.ingest(reg.emit(10))          # 推进水位到 10
    gw.ingest(reg.emit(0))           # 超窗（window=2，low=9）
    gw.downstream.set_available(False)
    gw.ingest(reg.emit(11))
    gw.ingest(reg.emit(12))
    gw.ingest(reg.emit(13))          # 溢出丢弃
    gw.ingest(reg.emit(1, corrupt="bad_crc"))
    gw.ingest(RawMessage(AUTO_PROTOCOL, reg.emit(2, corrupt="bad_crc").payload, "x"))
    gw.ingest(RawMessage(AUTO_PROTOCOL, b"\x00\x01not-any-protocol", "x"))

    before = gw.stats_snapshot()
    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(
        load_snapshot(tmp_path / "s.json"),
        config=GatewayConfig(window_size=2, spool_capacity=2),
    )
    after = gw2.stats_snapshot()
    for field_name in (
        "total_in", "accepted", "rejected", "decode_rejected", "duplicate",
        "out_of_window", "overflow_dropped", "delivered", "unidentified",
        "ambiguous",
    ):
        assert getattr(after, field_name) == getattr(before, field_name), field_name
    assert after.watermarks == before.watermarks

