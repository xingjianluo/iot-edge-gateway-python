"""重启恢复测试：快照落盘 -> 全新进程恢复 -> 水位/序号/缓存/计数接得上。

关键不变量：
    * 恢复后每设备水位、窗口内已见序号、待补发缓存、各类计数与重启前一致；
    * 重启前已交付/已排队的读数再喂一遍：水位不倒退、不重复投递、不重复计数；
    * 快照只含白名单运行状态，不夹带任何凭据类内容。
"""

import json
import os

import pytest

from edge_gateway.config import GatewayConfig
from edge_gateway.devices import SimulatedDevice
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.policies import DevicePolicy
from edge_gateway.snapshot import (
    SNAPSHOT_VERSION,
    SnapshotError,
    load_snapshot_file,
    save_snapshot,
    validate_snapshot,
)


def _drive_pregate():
    """构造一个含：已交付读数、缓冲积压、重复、溢出的重启前状态。"""
    gw = Gateway(window_size=8, spool_capacity=3,
                 overflow_policy="drop_oldest")
    dev = SimulatedDevice("line", protocol="register")
    other = SimulatedDevice("other", protocol="structured")
    gw.ingest(dev.emit(0))
    gw.ingest(other.emit(0))
    gw.ingest(dev.emit(1))
    gw.ingest(dev.emit(1))  # 重复
    gw.downstream.set_available(False)
    for s in range(2, 7):
        gw.ingest(dev.emit(s))  # 容量 3 + drop_oldest -> 保留 4,5,6
    return gw, dev, other


def test_snapshot_roundtrip_restores_watermark_seen_queue_and_counts(tmp_path):
    gw, dev, _other = _drive_pregate()
    path = tmp_path / "snap.json"
    save_snapshot(gw, str(path))
    snap = load_snapshot_file(str(path))

    gw2 = Gateway.restore_from_snapshot(
        snap, window_size=8, spool_capacity=3,
        overflow_policy="drop_oldest",
    )

    before = gw.stats_snapshot()
    after = gw2.stats_snapshot()
    # 计数全部接得上
    for field_name in (
        "total_in", "accepted", "rejected", "duplicate", "delivered",
        "overflow_dropped",
    ):
        assert getattr(after, field_name) == getattr(before, field_name), field_name
    # 水位表一致
    assert after.watermarks == before.watermarks == {"line": 6, "other": 0}

    # 待补发缓存：drop_oldest 后保留 4,5,6
    assert [r.seq for r in gw2.spool._q] == [4, 5, 6]
    sp_before = gw.spool.snapshot_stats()
    sp_after = gw2.spool.snapshot_stats()
    assert (sp_after.size, sp_after.enqueued, sp_after.delivered,
            sp_after.dropped_overflow) == (
        sp_before.size, sp_before.enqueued, sp_before.delivered,
        sp_before.dropped_overflow,
    )

    # 已见序号：恢复后立即重复提交窗口内序号仍被识别
    assert gw2.ingest(dev.emit(6)).reason is RejectReason.DUPLICATE_SEQ
    assert gw2.sequencer.watermark("line") == 6


def test_replay_after_restart_does_not_regress_or_double_deliver(tmp_path):
    gw, dev, _other = _drive_pregate()
    snap = gw.export_snapshot()

    # 全新下游（模拟新进程），恢复后只应补发缓存里的 4,5,6
    gw2 = Gateway.restore_from_snapshot(
        snap, window_size=8, spool_capacity=3,
        overflow_policy="drop_oldest",
    )
    # 重放：重启前已交付的 0,1 与已排队的 4,5,6 再喂一遍
    assert gw2.ingest(dev.emit(0)).reason is RejectReason.DUPLICATE_SEQ
    assert gw2.ingest(dev.emit(1)).reason is RejectReason.DUPLICATE_SEQ
    for s in (4, 5, 6):
        assert gw2.ingest(dev.emit(s)).reason is RejectReason.DUPLICATE_SEQ
    # 水位不倒退、计数不新增接受/投递
    st = gw2.stats_snapshot()
    assert st.watermarks["line"] == 6
    assert st.accepted == gw.stats_snapshot().accepted
    assert gw2.downstream.count() == 0  # 重放一条都没进下游

    # 恢复补发：只有缓存中的 4,5,6 被投递（0,1 不重复投递）
    gw2.downstream.set_available(True)
    n = gw2.on_downstream_recovered()
    assert n == 3
    assert gw2.downstream.delivered_seqs("line") == [4, 5, 6]
    # 再补发一次：幂等，零新增
    assert gw2.on_downstream_recovered() == 0


def test_new_forward_data_after_recovery_continues_correctly(tmp_path):
    gw, dev, _other = _drive_pregate()
    gw2 = Gateway.restore_from_snapshot(
        gw.export_snapshot(), window_size=8, spool_capacity=3,
        overflow_policy="drop_oldest",
    )
    # 下游仍未恢复：新数据 7 在容量 3 + drop_oldest 下腾退最旧 4
    gw2.downstream.set_available(False)
    r = gw2.ingest(dev.emit(7))
    assert r.ok and r.watermark == 7
    assert [x.seq for x in gw2.spool._q] == [5, 6, 7]
    gw2.downstream.set_available(True)
    gw2.on_downstream_recovered()
    seqs = gw2.downstream.delivered_seqs("line")
    assert seqs == [5, 6, 7]  # 积压保序补发；4 已在重启前被确定腾退


def test_snapshot_file_is_json_and_atomic(tmp_path):
    gw, _, _ = _drive_pregate()
    path = tmp_path / "nested" / "snap.json"
    save_snapshot(gw, str(path))
    # 标准 JSON、可解析
    obj = json.loads(path.read_text(encoding="utf-8"))
    assert obj["version"] == SNAPSHOT_VERSION
    assert isinstance(obj["taken_at_ms"], int)
    # 原子写：目录中不应残留临时文件
    leftovers = [
        n for n in os.listdir(tmp_path / "nested") if n.startswith(".gw-")
    ]
    assert leftovers == []


def test_snapshot_contains_only_whitelisted_runtime_state(tmp_path):
    gw, _, _ = _drive_pregate()
    snap = gw.export_snapshot()
    text = json.dumps(snap, ensure_ascii=False)

    # 顶层严格白名单
    assert set(snap) == {
        "version", "taken_at_ms", "sequencer", "spool", "stats",
    }
    # 绝不出现任何凭据类键名（递归检查每个键）
    forbidden = (
        "password", "passwd", "secret", "token", "credential",
        "api_key", "apikey", "authorization",
    )

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                assert not any(f in k.lower() for f in forbidden), k
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(snap)
    # 配置不进快照（按设备策略属于配置）
    assert "config" not in text and "device_policies" not in text


def test_snapshot_validation_rejects_tampered_and_foreign_keys():
    good = Gateway().export_snapshot()
    # 注入凭据风格的键 -> 拒绝
    bad = json.loads(json.dumps(good))
    bad["password"] = "leak"
    with pytest.raises(SnapshotError):
        validate_snapshot(bad)
    # 未知顶层键 -> 拒绝
    bad2 = json.loads(json.dumps(good))
    bad2["extra"] = 1
    with pytest.raises(SnapshotError):
        validate_snapshot(bad2)
    # 版本不符 -> 拒绝
    bad3 = json.loads(json.dumps(good))
    bad3["version"] = 999
    with pytest.raises(SnapshotError):
        validate_snapshot(bad3)
    # 队列项夹带额外字段 -> 拒绝
    bad4 = json.loads(json.dumps(good))
    bad4["spool"]["queue"].append({"device_id": "x", "token": "t"})
    with pytest.raises(SnapshotError):
        validate_snapshot(bad4)


def test_load_corrupt_or_nonjson_file_raises(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    with pytest.raises(SnapshotError):
        load_snapshot_file(str(p))
    with pytest.raises(FileNotFoundError):
        load_snapshot_file(str(tmp_path / "missing.json"))


def test_snapshot_works_with_per_device_policies():
    cfg = GatewayConfig(
        window_size=8, spool_capacity=10,
        device_policies={
            "tight": DevicePolicy(window_size=2, spool_capacity=2,
                                  overflow_policy="drop_oldest"),
        },
    )
    gw = Gateway(config=cfg)
    dev = SimulatedDevice("tight", protocol="register")
    gw.downstream.set_available(False)
    gw.ingest(dev.emit(0))
    gw.ingest(dev.emit(1))
    gw.ingest(dev.emit(2))  # drop_oldest 腾退 0

    # 新进程以相同配置恢复（策略来自配置而非快照），下游仍中断
    gw2 = Gateway.restore_from_snapshot(gw.export_snapshot(), config=cfg)
    gw2.downstream.set_available(False)
    assert gw2.policy_resolver.resolve("tight").window_size == 2
    assert [r.seq for r in gw2.spool._q] == [1, 2]
    # 恢复后该设备仍按自己的容量 2 生效：再来一条会腾退最旧
    gw2.ingest(dev.emit(3))
    assert [r.seq for r in gw2.spool._q] == [2, 3]
