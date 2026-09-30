"""重启后接着跑：状态快照落盘/恢复，水位/序号集/缓存/计数接得上。

快照契约（本轮收敛）：
    * 只含纯运行状态（stats/sequencer/spool + 外层 format/version/
      taken_at_ms），不写任何策略设置（窗口/容量/溢出策略/设备覆盖），
      也不含凭据类内容；
    * 恢复时严格白名单校验：未知字段或凭据类键名一律明确拒绝；
    * 策略由恢复进程自己的配置决定（含按设备覆盖与缺省回退）。
"""

import copy
import json

import pytest

from edge_gateway.devices import RawMessage, SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policy import DevicePolicy
from edge_gateway.snapshot import (
    SNAPSHOT_FORMAT,
    SNAPSHOT_VERSION,
    load_snapshot,
    restore_gateway,
    save_snapshot,
    state_to_gateway,
)
from edge_gateway.state import SnapshotError


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


def _build_overflow_gateway():
    """构造一个发生过缓冲溢出丢弃的现场（drop_newest + drop_oldest 都覆盖）。"""
    gw = Gateway(window_size=64, spool_capacity=3)
    dev = SimulatedDevice("line", protocol="register")
    gw.downstream.set_available(False)
    for s in range(3):
        gw.ingest(dev.emit(s))
    gw.ingest(dev.emit(3))          # 容量满 -> 丢弃 1 条
    gw.ingest(dev.emit(1))          # 重复 1 条
    gw.ingest(dev.emit(4, corrupt="bad_crc"))  # 帧级拒绝 1 条
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

    # 快照只含运行状态：策略设置绝不落盘
    assert set(on_disk) == {
        "format", "version", "taken_at_ms",
        "stats", "sequencer", "spool",
    }
    assert "config" not in on_disk
    assert "policy" not in on_disk

    # 全新进程式恢复：策略由恢复进程的配置显式给出（与重启前一致）
    gw2 = restore_gateway(on_disk, window_size=8, spool_capacity=10)
    assert gw2.stats_snapshot().watermarks["line"] == 4
    gw2.downstream.set_available(True)
    assert gw2.on_downstream_recovered() == 3
    assert gw2.downstream.delivered_seqs("line") == [2, 3, 4]

    # 原子落盘：目标路径存在且是完整 JSON，同目录无残留临时文件
    json.loads(path.read_text(encoding="utf-8"))
    leftovers = [p.name for p in tmp_path.iterdir()
                 if p.name.startswith(".gw-snapshot-")]
    assert leftovers == []


def test_restore_rejects_unknown_fields_and_credentials(tmp_path):
    gw, _ = _build_prefailure_gateway()
    save_snapshot(gw, tmp_path / "s.json")
    state = load_snapshot(tmp_path / "s.json")
    gw_ok = Gateway(window_size=8, spool_capacity=10)

    def tamper(mutator):
        bad = copy.deepcopy(state)
        mutator(bad)
        return bad

    # 1) 外层夹"策略"类未知字段：明确拒绝（策略只能来自进程配置）
    with pytest.raises(SnapshotError, match="config"):
        state_to_gateway(gw_ok, tamper(lambda s: s.update(config={})))
    with pytest.raises(SnapshotError, match="policy"):
        state_to_gateway(gw_ok, tamper(lambda s: s.update(policy={})))

    # 2) 外层/嵌套塞凭据类键名：点名拒绝，绝不静默接受
    for key in ("password", "token", "secret", "api_key"):
        bad = tamper(lambda s, k=key: s.update({k: "leak"}))
        with pytest.raises(SnapshotError, match="credential"):
            state_to_gateway(gw_ok, bad)
    bad = tamper(lambda s: s["stats"].update(access_token="abc"))
    with pytest.raises(SnapshotError, match="credential"):
        state_to_gateway(gw_ok, bad)
    bad = tamper(lambda s: s["spool"].update(credentials=[]))
    with pytest.raises(SnapshotError, match="credential"):
        state_to_gateway(gw_ok, bad)

    # 3) 运行状态各层的普通未知字段：拒绝并指出位置
    with pytest.raises(SnapshotError, match="unknown field 'extra'"):
        state_to_gateway(gw_ok, tamper(lambda s: s["stats"].update(extra=1)))
    with pytest.raises(SnapshotError, match="unknown field"):
        state_to_gateway(
            gw_ok,
            tamper(lambda s: s["sequencer"]["line"].update(unknown=1)),
        )
    with pytest.raises(SnapshotError, match="unknown field"):
        state_to_gateway(
            gw_ok,
            tamper(lambda s: s["spool"]["queued"][0].update(extra="x")),
        )

    # 4) 类型/结构错误也拒绝
    bad = tamper(lambda s: s["stats"].__setitem__("delivered", "2"))
    with pytest.raises(SnapshotError, match="delivered"):
        state_to_gateway(gw_ok, bad)
    bad = tamper(lambda s: s.update(format="forged"))
    with pytest.raises(SnapshotError):
        state_to_gateway(gw_ok, bad)

    # 5) 合法快照本身不受影响：同样数据正常恢复
    state_to_gateway(Gateway(window_size=8, spool_capacity=10), state)


def test_restore_policy_comes_from_process_config_not_snapshot(tmp_path):
    """设备覆盖与缺省回退由恢复进程配置决定；快照改不动策略。"""
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
    on_disk = load_snapshot(tmp_path / "s.json")
    assert "device_policies" not in json.dumps(on_disk)

    # 恢复进程显式带上自己的按设备策略：dev-a 仍受其单独容量约束，
    # 未单独设定的 dev-b 回退全局缺省
    gw2 = restore_gateway(
        on_disk,
        window_size=100, spool_capacity=100,
        device_policies={"dev-a": DevicePolicy(spool_capacity=1)},
    )
    gw2.downstream.set_available(False)
    assert gw2.policies.spool_capacity_for("dev-a") == 1
    assert gw2.policies.spool_capacity_for("dev-b") == 100
    assert gw2.ingest(a.emit(1)).reason is RejectReason.BUFFER_OVERFLOW
    assert gw2.ingest(b.emit(1)).buffered
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("dev-a") == [0]
    assert gw2.downstream.delivered_seqs("dev-b") == [0, 1]

    # 恢复进程不给覆盖：两台设备都回退全局缺省（dev-a 不再有 1 容量限制）
    gw3 = restore_gateway(on_disk, window_size=100, spool_capacity=100)
    assert gw3.policies.spool_capacity_for("dev-a") == 100


def test_overflow_counts_survive_snapshot_and_replay(tmp_path):
    """重启前的溢出丢弃/投递/重复/超窗计数原样进快照，恢复后对得上。"""
    gw, dev = _build_overflow_gateway()
    before = gw.stats_snapshot()
    sp_before = gw.spool.snapshot_stats()
    assert before.overflow_dropped == 1          # 修复前这里在快照里会变 0
    assert sp_before.dropped_overflow == 1
    assert before.delivered == 0 and before.duplicate == 1
    assert before.decode_rejected == 1

    save_snapshot(gw, tmp_path / "s.json")
    on_disk = load_snapshot(tmp_path / "s.json")
    # 落盘内容里的计数与重启前完全一致
    assert on_disk["stats"]["overflow_dropped"] == 1
    assert on_disk["stats"]["duplicate"] == 1
    assert on_disk["stats"]["decode_rejected"] == 1
    assert on_disk["spool"]["stats"]["dropped_overflow"] == 1

    gw2 = restore_gateway(on_disk, window_size=64, spool_capacity=3)
    after = gw2.stats_snapshot()
    assert after.overflow_dropped == before.overflow_dropped == 1
    assert after.duplicate == 1 and after.delivered == 0
    assert after.watermarks["line"] == before.watermarks["line"] == 3
    assert gw2.spool.snapshot_stats().dropped_overflow == 1

    # 重放：重启前已接受（含曾被溢出丢掉）的序号不重复投递、不重复计丢弃，
    # 水位不倒退
    gw2.downstream.set_available(False)
    for s in range(4):  # 0,1,2 在排队；3 当时被溢出丢弃但序号已接受
        r = gw2.ingest(dev.emit(s))
        assert r.reason is RejectReason.DUPLICATE_SEQ, (s, r.reason)
    st2 = gw2.stats_snapshot()
    assert st2.watermarks["line"] == 3
    assert st2.overflow_dropped == 1
    assert gw2.spool.snapshot_stats().dropped_overflow == 1
    # 恢复后新发生的溢出在快照基线之上继续累加（不清零、不重算）
    r = gw2.ingest(dev.emit(9))  # 队列仍占满 0,1,2 -> 再丢一条
    assert r.reason is RejectReason.BUFFER_OVERFLOW
    assert gw2.stats_snapshot().overflow_dropped == 2


def test_drop_oldest_eviction_counts_survive_snapshot(tmp_path):
    """DROP_OLDEST 静默腾退同样要落进快照计数（这条路径最容易漏账）。"""
    gw = Gateway(
        window_size=64, spool_capacity=2, overflow_policy="drop_oldest",
    )
    dev = SimulatedDevice("d", protocol="register")
    gw.downstream.set_available(False)
    for s in range(5):      # 容量 2：0,1,2 先后被腾退，最终留下 3,4
        assert gw.ingest(dev.emit(s)).ok
    assert gw.stats_snapshot().overflow_dropped == 3
    assert gw.spool.snapshot_stats().dropped_overflow == 3

    save_snapshot(gw, tmp_path / "s.json")
    on_disk = load_snapshot(tmp_path / "s.json")
    assert on_disk["stats"]["overflow_dropped"] == 3

    gw2 = restore_gateway(
        on_disk, window_size=64, spool_capacity=2,
        overflow_policy="drop_oldest",
    )
    assert gw2.stats_snapshot().overflow_dropped == 3
    assert gw2.spool.snapshot_stats().dropped_overflow == 3
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    assert gw2.downstream.delivered_seqs("d") == [3, 4]
    assert gw2.stats_snapshot().overflow_dropped == 3  # 补发不改变丢弃账


def test_restore_continues_after_crash_with_auto_identified_frames(tmp_path):
    """重启后用"不声明协议"的原始字节重放，照样被幂等拦截。"""
    gw = Gateway(window_size=8, spool_capacity=10)
    reg = SimulatedDevice("dev-x", protocol="register")
    gw.ingest(reg.emit_auto(0))
    save_snapshot(gw, tmp_path / "s.json")
    gw2 = restore_gateway(
        load_snapshot(tmp_path / "s.json"),
        window_size=8, spool_capacity=10,
    )

    replay = RawMessage(
        protocol="auto", payload=reg.emit(0).payload, label="auto-replay",
    )
    r = gw2.ingest(replay)
    assert r.reason is RejectReason.DUPLICATE_SEQ
    assert gw2.stats_snapshot().watermarks["dev-x"] == 0
