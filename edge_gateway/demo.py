"""本地端到端演示：异常链路下的接入、去重、缓冲与恢复。

运行：uv run python main.py
"""

from __future__ import annotations

from .devices import SimulatedDevice
from .gateway import Gateway


def _print_result(result, label: str) -> None:
    """打印序号、水位与判定依据（与单测日志格式保持一致）。"""
    reason = result.reason.value if result.reason else "-"
    verdict = "ACCEPT" if result.ok else "REJECT"
    dest = ""
    if result.delivered:
        dest = " ->downstream"
    elif result.buffered:
        dest = " ->spool"
    print(
        f"  [{verdict:6s}] {label:34s} seq={str(result.seq):>4s} "
        f"wm={str(result.watermark):>4s} stage={result.stage:9s} "
        f"reason={reason:18s}{dest}"
    )
    if not result.ok and result.detail:
        print(f"             └─ {result.detail}")


def run_demo() -> None:
    """跑一遍确定性剧本并打印结论与最终统计。"""
    gw = Gateway(window_size=8, spool_capacity=4)
    reg_dev = SimulatedDevice("line-A/temp", protocol="register")
    json_dev = SimulatedDevice("line-B/vibe", protocol="structured")

    print("=== 1) 双协议正常接入（乱序：先 1 后 0）===")
    for msg in (reg_dev.emit(1), json_dev.emit(0), reg_dev.emit(0)):
        _print_result(gw.ingest(msg), msg.label)

    print("=== 2) 重复序号（只计一次）===")
    msg = reg_dev.emit(1)  # 重复
    _print_result(gw.ingest(msg), msg.label + " replay")

    print("=== 3) 畸形帧矩阵（绝不静默丢弃）===")
    for corrupt in ("truncate", "bad_crc", "bad_magic",
                    "missing_field", "bad_value", "malformed"):
        msg = reg_dev.emit(2, corrupt=corrupt)
        _print_result(gw.ingest(msg), msg.label)
    msg = json_dev.emit(2, corrupt="missing_field")
    _print_result(gw.ingest(msg), msg.label)

    print("=== 4) 下游中断：3..6 进入边界缓冲（容量 4），7/8 溢出 ===")
    gw.downstream.set_available(False)
    for s in (3, 4, 5, 6):
        msg = reg_dev.emit(s)
        _print_result(gw.ingest(msg), msg.label)
    for s in (7, 8):
        msg = reg_dev.emit(s)
        _print_result(gw.ingest(msg), msg.label)

    print("=== 5) 迟到超窗与序号回退 ===")
    # 水位已到 8，窗口 [1,8]：seq=0 超窗且相对上一条(8)回退
    msg = reg_dev.emit(0)
    _print_result(gw.ingest(msg), msg.label + " late")

    print("=== 6) 下游恢复：按序补发 3,4,5,6 ===")
    gw.downstream.set_available(True)
    n = gw.on_downstream_recovered()
    print(f"  flushed={n}")
    print(f"  downstream line-A/temp received seqs: "
          f"{gw.downstream.delivered_seqs('line-A/temp')}")

    print("=== 7) 最终统计 ===")
    st = gw.stats_snapshot()
    sp = gw.spool.snapshot_stats()
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
