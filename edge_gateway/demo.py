"""本地端到端演示：异常链路下的接入、去重、缓冲、恢复与重启快照。

运行：uv run python main.py
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from .config import config_from_env
from .devices import SimulatedDevice
from .gateway import Gateway
from .policy import DevicePolicy
from .snapshot import load_snapshot, restore_gateway, save_snapshot


def _print_result(result, label: str) -> None:
    """打印序号、水位与判定依据（与单测日志格式保持一致）。"""
    reason = result.reason.value if result.reason else "-"
    verdict = "ACCEPT" if result.ok else "REJECT"
    dest = ""
    if result.delivered:
        dest = " ->downstream"
    elif result.buffered:
        dest = " ->spool"
    proto = f" proto={result.protocol}" if result.protocol else ""
    print(
        f"  [{verdict:6s}] {label:34s} seq={str(result.seq):>4s} "
        f"wm={str(result.watermark):>4s} stage={result.stage:9s} "
        f"reason={reason:18s}{dest}{proto}"
    )
    if not result.ok and result.detail:
        print(f"             └─ {result.detail}")


def run_demo() -> None:
    """跑一遍确定性剧本并打印结论与最终统计。"""
    config = config_from_env()
    policy_overrides = {
        # line-A 用更窄窗口；line-B 下线缓冲更短且满了丢最旧
        "line-A/temp": DevicePolicy(window_size=3),
        "line-B/vibe": DevicePolicy(
            spool_capacity=2, overflow_policy="drop_oldest",
        ),
    }
    gw = Gateway(
        config=config, window_size=8, spool_capacity=4,
        device_policies=policy_overrides,
    )
    reg_dev = SimulatedDevice("line-A/temp", protocol="register")
    json_dev = SimulatedDevice("line-B/vibe", protocol="structured")

    print(
        f"=== config: window_size={gw.config.window_size} "
        f"spool_capacity={gw.config.spool_capacity} "
        f"overflow_policy={gw.config.overflow_policy} ==="
    )
    print(f"  device overrides: {gw.policies.known_devices()}")

    print("=== 1) 协议自行判别（设备不声明协议，只给原始字节）===")
    for msg in (reg_dev.emit_auto(1), json_dev.emit_auto(0),
                reg_dev.emit_auto(0)):
        _print_result(gw.ingest(msg), msg.label)

    print("=== 1b) 自动判别：认得出但帧坏 / 认不出 / 歧义 三种结论 ===")
    from edge_gateway.devices import RawMessage
    from edge_gateway.models import DecodeResult, RejectReason
    from edge_gateway.protocols.identification import (
        AUTO_PROTOCOL,
        ProtocolIdentifier,
    )

    # 认得出是 register、但 CRC 坏了：给 register 自己的结论，并带设备/序号
    msg = reg_dev.emit_auto(2, corrupt="bad_crc")
    _print_result(gw.ingest(msg), msg.label)
    # JSON 对象缺字段：给 structured 自己的 FIELD_MISSING，而不是"认不出"
    msg = json_dev.emit_auto(1, corrupt="missing_field")
    _print_result(gw.ingest(msg), msg.label)
    # 真正认不出：两种编码都对不上
    _print_result(
        gw.ingest(RawMessage(AUTO_PROTOCOL, b"\x00\x01not-anything", "?")),
        "unidentified bytes",
    )
    # 歧义：同一段字节同时像两种编码（用桩解码器稳定构造）
    real_identifier = gw.identifier

    class _Stub:
        def __init__(self, name, result):
            self.name = name
            self._result = result

        def decode(self, raw):
            return self._result

    gw.identifier = ProtocolIdentifier([
        _Stub("register", DecodeResult(
            ok=False, reason=RejectReason.CHECKSUM_MISMATCH, detail="crc",
            attributable=True, protocol="register",
            evidence={"device_id": "line-A/temp", "seq": 99})),
        _Stub("structured", DecodeResult(
            ok=False, reason=RejectReason.FIELD_MISSING, detail="missing",
            attributable=True, protocol="structured",
            evidence={"device_id": "line-A/temp", "seq": 99})),
    ])
    _print_result(
        gw.ingest(RawMessage(AUTO_PROTOCOL, b"ambiguous-bytes", "!?")),
        "ambiguous corrupt bytes",
    )
    gw.identifier = real_identifier

    print("=== 1c) 指定协议老路仍然可用 ===")
    _print_result(gw.ingest(reg_dev.emit(2)), reg_dev.emit(2).label)

    print("=== 2) 重复序号（只计一次）===")
    msg = reg_dev.emit(1)  # 重复
    _print_result(gw.ingest(msg), msg.label + " replay")

    print("=== 3) 畸形帧矩阵（绝不静默丢弃）===")
    for corrupt in ("truncate", "bad_crc", "bad_magic",
                    "missing_field", "bad_value", "malformed"):
        msg = reg_dev.emit(5, corrupt=corrupt)
        _print_result(gw.ingest(msg), msg.label)
    msg = json_dev.emit(2, corrupt="missing_field")
    _print_result(gw.ingest(msg), msg.label)

    print("=== 4) 下游中断：按设备容量缓冲/溢出（B 容量2丢最旧，A 容量4）===")
    gw.downstream.set_available(False)
    for s in (3, 4, 5, 6):
        msg = reg_dev.emit(s)
        _print_result(gw.ingest(msg), msg.label)
    for s in (1, 2, 3, 4):
        msg = json_dev.emit(s)  # B：保留最新的 3,4，1,2 被 drop_oldest
        _print_result(gw.ingest(msg), msg.label)

    print("=== 5) 迟到超窗与序号回退（A 的窗口=3）===")
    msg = reg_dev.emit(0)
    _print_result(gw.ingest(msg), msg.label + " late")

    print("=== 6) 落快照 -> 全新进程恢复 -> 接着补发 ===")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "gw-state.json"
        save_snapshot(gw, path)
        disk = load_snapshot(path)
        print(
            f"  snapshot: format={disk['format']} v{disk['version']} "
            f"watermarks={disk['stats']['watermarks']} "
            f"queued={len(disk['spool']['queued'])} "
            f"overflow_dropped={disk['stats']['overflow_dropped']}"
        )
        # 快照只含纯运行状态；策略由新进程自己的配置决定（含按设备覆盖）
        gw2 = restore_gateway(
            disk, config=config,
            window_size=8, spool_capacity=4,
            device_policies=policy_overrides,
        )
        # 重放重启前数据：水位不倒退；窗口内序号 DUPLICATE_SEQ，
        # 过窄窗口外的更旧序号依旧 OUT_OF_WINDOW/ROLLBACK，绝不重新投递
        for s in (0, 1, 3, 4, 5, 6):
            r = gw2.ingest(reg_dev.emit(s))
            assert not r.delivered and not r.buffered, r.detail
        flushed = gw2.on_downstream_recovered()  # 新下游可用，直接补发
        print(f"  restored + flushed={flushed}")
        print(f"  new downstream line-A/temp received: "
              f"{gw2.downstream.delivered_seqs('line-A/temp')}")
        print(f"  new downstream line-B/vibe received: "
              f"{gw2.downstream.delivered_seqs('line-B/vibe')}")

    print("=== 7) 最终统计（重启前进程）===")
    st = gw.stats_snapshot()
    sp = gw.spool.snapshot_stats()
    print(f"  total_in={st.total_in} accepted={st.accepted} "
          f"rejected={st.rejected}")
    print(f"  decode_rejected={st.decode_rejected} duplicate={st.duplicate} "
          f"out_of_window={st.out_of_window} rollback={st.rollback}")
    print(f"  unidentified={st.unidentified} ambiguous={st.ambiguous}")
    print(f"  overflow_dropped={st.overflow_dropped} delivered={st.delivered}")
    print(f"  watermarks={st.watermarks}")
    print(f"  spool: size={sp.size} enqueued={sp.enqueued} "
          f"delivered={sp.delivered} failures={sp.downstream_failures}")


if __name__ == "__main__":
    run_demo()
