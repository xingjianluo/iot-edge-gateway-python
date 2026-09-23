"""端到端：模拟设备 -> 两种协议 -> 去重/窗口 -> 中断缓冲 -> 恢复补发。

该用例是需求场景的完整复现脚本，断言所有计数可区分且确定。
"""
from gateway.buffer import OverflowPolicy
from gateway.device import SimulatedDevice
from gateway.downstream import MockDownstream
from gateway.gateway import EdgeGateway
from gateway.model import RejectReason
from gateway.protocols import JsonFrameDecoder, RegisterFrameDecoder


def test_full_pipeline_deterministic():
    downstream = MockDownstream(True)
    gw = EdgeGateway(
        decoders=[RegisterFrameDecoder(), JsonFrameDecoder()],
        window=3,
        capacity=4,
        overflow_policy=OverflowPolicy.DROP_OLDEST,
        downstream=downstream,
    )
    a = SimulatedDevice("dev-A")
    b = SimulatedDevice("dev-B")

    # --- 阶段1：链路正常，乱序 + 两种协议 + 畸形帧 ---
    assert gw.ingest(a.emit_json(1, "temp", 21.0, 100.0)).accepted
    assert gw.ingest(a.emit_register(3, "temp", 23.0, 102.0)).accepted  # 乱序
    bad_crc = gw.ingest(a.emit_register_bad_checksum(99, "x", 1, 1))
    assert bad_crc.reason is RejectReason.CHECKSUM_FAILED
    trunc = gw.ingest(a.emit_register_truncated(98, "x", 1, 1))
    assert trunc.reason is RejectReason.LENGTH_MISMATCH
    miss = gw.ingest(b.emit_json_missing_field(1, "m", 1, 1))
    assert miss.reason is RejectReason.MISSING_FIELD
    malformed = gw.ingest(b.emit_json_malformed())
    assert malformed.reason is RejectReason.DECODE_ERROR

    # --- 阶段2：下游中断 ---
    gw.downstream.set_available(False)
    # 补齐 A:2，触发 A:2、A:3 就绪并入缓冲
    assert gw.ingest(a.emit_register(2, "temp", 22.0, 101.0)).accepted
    # B 设备在中断期间发 1..6，容量4，DROP_OLDEST：保留 3,4,5,6
    for s in range(1, 7):
        assert gw.ingest(b.emit_json(s, "hum", 0.1 * s, 200.0 + s)).accepted
    buf_stats = gw.stats()["buffer"]
    assert buf_stats["capacity"] == 4
    # A2,A3 先入缓冲；B1..B6 填满后全局最旧的 A2,A3,B1,B2 依次被逐出
    assert buf_stats["dropped_oldest"] == 4
    assert buf_stats["last_dropped"] == ("dev-B", 2)
    assert gw.downstream.received_count == 1  # 中断前仅 A:1 已下发

    # --- 阶段3：重复与回退在中断期间依旧被拒绝 ---
    dup = gw.ingest(b.emit_json(4, "hum", 0.4, 204.0))
    assert dup.reason in (RejectReason.DUPLICATE_SEQ, RejectReason.SEQ_ROLLBACK)
    assert dup.accepted is False

    # --- 阶段4：恢复补发，严格按序 ---
    gw.downstream.set_available(True)
    sent = gw.on_downstream_restored()
    # 缓冲容量4，DROP_OLDEST 全局 FIFO：
    # [A2,A3] -> B1,B2 填满 -> B3 逐出A2 -> B4 逐出A3 -> B5 逐出B1 -> B6 逐出B2
    assert sent == 4

    per = {}
    for r in gw.downstream.received:
        per.setdefault(r.device_id, []).append(r.seq)
    assert per["dev-A"] == [1]                 # A2,A3 在溢出中被确定丢弃
    assert per["dev-B"] == [3, 4, 5, 6]        # 补发严格有序

    # --- 阶段5：恢复后新数据直发，重放不改变任何结果 ---
    before = gw.downstream.received_count
    assert gw.ingest(b.emit_json(7, "hum", 0.7, 207.0)).accepted
    assert gw.downstream.received_count == before + 1
    replay = gw.ingest(b.emit_json(7, "hum", 0.7, 207.0))
    assert replay.accepted is False
    assert gw.downstream.duplicate_delivery == 0

    stats = gw.stats()
    # 畸形帧：checksum_failed / length_mismatch / missing_field / decode_error 各1
    assert stats["rejected_decode"] == 4
    assert stats["watermarks"]["dev-A"] == 3
    assert stats["watermarks"]["dev-B"] == 7
    assert stats["buffer"]["dropped_oldest"] == 4
    assert stats["buffer"]["last_dropped"] == ("dev-B", 2)
