"""重启后接着跑：状态快照落盘/恢复，水位/序号集/缓存/计数接得上。"""

import json

import pytest

from edge_gateway.devices import SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policy import DevicePolicy
from edge_gateway.snapshot import (
    SNAPSHOT_FORMAT,
    SNAPSHOT_VERSION,
    load_snapshot,
    restore_gateway,
    save_snapshot,
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
    assert "config" in on_disk

    # 全新进程式恢复：仅凭快照文件重建（含其自带配置）
    gw2 = restore_gateway(on_disk)
    assert gw2.stats_snapshot().watermarks["line"] == 4
    gw2.downstream.set_available(True)
    assert gw2.on_downstream_recovered() == 3
    assert gw2.downstream.delivered_seqs("line") == [2, 3, 4]

    # 原子落盘：目标路径存在且是完整 JSON，同目录无残留临时文件
    json.loads(path.read_text(encoding="utf-8"))
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name.startswith(".gw-snapshot-")]
    assert leftovers == []


def test_restore_rejects_config_mismatch(tmp_path):
    gw, _ = _build_prefailure_gateway()
    save_snapshot(gw, tmp_path / "s.json")
    state = load_snapshot(tmp_path / "s.json")
    # 用不同的窗口尺度新建网关：state_to_gateway 拒绝恢复，避免语义错配
    from edge_gateway.snapshot import state_to_gateway

    gw_other = Gateway(window_size=16, spool_capacity=10)
    with pytest.raises(ValueError, match="window_size"):
        state_to_gateway(gw_other, state)
    # 原配置网关恢复正常
    state_to_gateway(Gateway(window_size=8, spool_capacity=10), state)


def test_snapshot_restores_per_device_policies_and_independence(tmp_path):
    gw = Gateway(
        window_size=100, spool_capacity=100,
        device_policies={"dev-a": DevicePolicy(spool_capacity=1)},
    )
    a = SimulatedDevice("dev-a", protocol="register")
    b = SimulatedDevice("dev-b", protocol="register")
    gw.downstream.set_available(False)
    gw.ingest(a.emit(0))
    gw.ingest(b.emit(0))
    save_snapshot(gw, tmp_path / "s.json")

    gw2 = restore_gateway(load_snapshot(tmp_path / "s.json"))
    # 下游仍未恢复：容量约束继续生效
    gw2.downstream.set_available(False)
    # 设备级容量策略随快照恢复：dev-a 恢复出的 0 已占满其唯 1 容量
    assert gw2.policies.spool_capacity_for("dev-a") == 1
    assert gw2.policies.spool_capacity_for("dev-b") == 100
    assert gw2.ingest(a.emit(1)).reason is RejectReason.BUFFER_OVERFLOW
    assert gw2.ingest(b.emit(1)).buffered
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("dev-a") == [0]
    assert gw2.downstream.delivered_seqs("dev-b") == [0, 1]


def test_restore_continues_after_crash_with_auto_identified_frames(tmp_path):
    """重启后用"不声明协议"的原始字节重放，照样被幂等拦截。"""
    from edge_gateway.devices import RawMessage

    gw = Gateway(window_size=8, spool_capacity=10)
    reg = SimulatedDevice("dev-x", protocol="register")
    gw.ingest(reg.emit_auto(0))
    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(load_snapshot(tmp_path / "s.json"))

    replay = RawMessage(
        protocol="auto", payload=reg.emit(0).payload, label="auto-replay",
    )
    r = gw2.ingest(replay)
    assert r.reason is RejectReason.DUPLICATE_SEQ
    assert gw2.stats_snapshot().watermarks["dev-x"] == 0
