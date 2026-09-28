"""本地端到端演示：自动识别、按设备策略、异常链路与重启恢复。

运行：uv run python main.py
"""

from __future__ import annotations

from .config import config_from_env
from .devices import SimulatedDevice, auto_message
from .gateway import Gateway
from .policies import DevicePolicy
from .snapshot import load_snapshot_file, save_snapshot


def _print_result(result, label: str) -> None:
    """打印序号、水位与判定依据（与单测日志格式保持一致）。"""
    reason = result.reason.value if result.reason else "-"
    verdict = "ACCEPT" if result.ok else "REJECT"
    dest = ""
    if result.delivered:
        dest = " ->downstream"
    elif result.buffered:
        dest = " ->spool"
    detected = f" [{result.detected_protocol}]" if result.detected_protocol else ""
    print(
        f"  [{verdict:6s}] {label:32s} seq={str(result.seq):>4s} "
        f"wm={str(result.watermark):>4s} stage={result.stage:9s} "
        f"reason={reason:22s}{dest}{detected}"
    )
    if not result.ok and result.detail:
        print(f"             └─ {result.detail}")


def run_demo(snapshot_path: str = "/tmp/gw-snapshot.json") -> None:
    """跑一遍确定性剧本并打印结论、快照恢复与最终统计。"""
    env_config = config_from_env()
    # 合并演示用的按设备覆盖（环境变量给的同设备覆盖优先）
    device_policies = {
        "tight-line": DevicePolicy(
            window_size=2, spool_capacity=2, overflow_policy="drop_oldest",
        ),
    }
    device_policies.update(env_config.device_policies)
    gw = Gateway(
        config=type(env_config)(
            window_size=env_config.window_size,
            spool_capacity=env_config.spool_capacity,
            overflow_policy=env_config.overflow_policy,
            device_policies=device_policies,
        ),
    )
    reg_dev = SimulatedDevice("line-A/temp", protocol="register")
    json_dev = SimulatedDevice("line-B/vibe", protocol="structured")
    tight_dev = SimulatedDevice("tight-line", protocol="register")

    print(
        f"=== config: window_size={gw.config.window_size} "
        f"spool_capacity={gw.config.spool_capacity} "
        f"overflow_policy={gw.config.overflow_policy} "
        f"device_policies={sorted(gw.config.device_policies)} ==="
    )

    print("=== 1) 协议自行判别（auto：只给原始字节）===")
    for msg in (
        auto_message(reg_dev.emit(0).payload, "auto register#0"),
        auto_message(json_dev.emit(0).payload, "auto structured#0"),
        auto_message(b"\x00\x01mystery-bytes", "auto unknown"),
        auto_message(reg_dev.emit(9, corrupt="bad_crc").payload,
                     "auto register#9 badcrc"),
    ):
        _print_result(gw.ingest(msg), msg.label)

    print("=== 2) 老路径：显式指定协议仍然可用 ===")
    msg = reg_dev.emit(1)
    _print_result(gw.ingest(msg), msg.label)

    print("=== 3) 按设备策略：tight-line 窗口=2 容量=2 drop_oldest ===")
    gw.downstream.set_available(False)
    for s in (0, 1, 2, 3):
        _print_result(gw.ingest(tight_dev.emit(s)), tight_dev.emit(s).label)
    resolved = gw.policy_resolver.resolve("tight-line")
    print(
        f"  effective policy for tight-line: window={resolved.window_size} "
        f"capacity={resolved.spool_capacity} overflow={resolved.overflow_policy}",
    )

    print("=== 4) 落快照并在全新网关中恢复 ===")
    save_snapshot(gw, snapshot_path)
    snap = load_snapshot_file(snapshot_path)
    gw2 = Gateway.restore_from_snapshot(snap, config=gw.config)
    gw2.downstream.set_available(False)
    before = gw.stats_snapshot()
    after = gw2.stats_snapshot()
    counts_match = (
        (before.total_in, before.accepted, before.delivered)
        == (after.total_in, after.accepted, after.delivered)
    )
    print(f"  restored counts match: {counts_match}")
    print(f"  restored watermarks={after.watermarks}")
    print(f"  restored spool seqs={[r.seq for r in gw2.spool._q]}")

    print("=== 5) 重放已交付/已排队读数：不回退、不重复 ===")
    for s in (0, 1):
        _print_result(gw2.ingest(reg_dev.emit(s)), f"replay line-A#{s}")
    for s in (2, 3):
        _print_result(gw2.ingest(tight_dev.emit(s)), f"replay tight#{s}")

    print("=== 6) 下游恢复：按序补发缓存 ===")
    gw2.downstream.set_available(True)
    n = gw2.on_downstream_recovered()
    print(f"  flushed={n}")

    print("=== 7) 最终统计（恢复后的进程）===")
    st = gw2.stats_snapshot()
    sp = gw2.spool.snapshot_stats()
    print(f"  total_in={st.total_in} accepted={st.accepted} "
          f"rejected={st.rejected}")
    print(f"  decode_rejected={st.decode_rejected} duplicate={st.duplicate} "
          f"out_of_window={st.out_of_window} rollback={st.rollback}")
    print(f"  overflow_dropped={st.overflow_dropped} delivered={st.delivered}")
    print(f"  watermarks={st.watermarks}")
    print(f"  spool: size={sp.size} enqueued={sp.enqueued} "
          f"delivered={sp.delivered} failures={sp.downstream_failures}")


if __name__ == "__main__":
    run_demo()
