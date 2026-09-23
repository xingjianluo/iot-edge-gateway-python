"""本地端到端演示（进程内）：模拟设备 -> 边缘网关 -> 模拟下游汇聚端。

运行：uv run python demo.py
场景：正常接入两种协议与乱序/重复/畸形帧 -> 制造下游中断与缓冲溢出
-> 恢复后按序补发 -> 打印全部统计。
"""
from __future__ import annotations

import logging

from gateway.buffer import OverflowPolicy
from gateway.device import SimulatedDevice
from gateway.downstream import MockDownstream
from gateway.gateway import EdgeGateway
from gateway.protocols import JsonFrameDecoder, RegisterFrameDecoder


def banner(title: str) -> None:
    print("\n" + "=" * 20, title, "=" * 20)


def show(title: str, result) -> None:
    if result.accepted:
        print(f"  [接受] seq={result.reading.seq if result.reading else '-'} "
              f"水位={result.watermark} {result.detail}")
    else:
        print(f"  [拒绝] 原因={result.reason.value if result.reason else '-'} "
              f"水位={result.watermark} | {result.detail}")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="  %(levelname)s %(message)s")

    downstream = MockDownstream(available=True)
    gw = EdgeGateway(
        decoders=[RegisterFrameDecoder(), JsonFrameDecoder()],
        window=3,                 # 乱序容忍窗口 W
        capacity=4,               # 缓冲硬上限
        overflow_policy=OverflowPolicy.DROP_OLDEST,
        downstream=downstream,
    )
    dev = SimulatedDevice("line-1")

    banner("阶段1：正常链路，乱序 + 两协议 + 畸形帧")
    show("json 序号1", gw.ingest(dev.emit_json(1, "temp", 21.0, 100.0)))
    show("寄存器 序号3(乱序)", gw.ingest(dev.emit_register(3, "temp", 23.0, 102.0)))
    show("寄存器 序号2(补洞)", gw.ingest(dev.emit_register(2, "temp", 22.0, 101.0)))
    show("重复序号2", gw.ingest(dev.emit_register(2, "temp", 22.0, 101.0)))
    show("校验失败帧", gw.ingest(dev.emit_register_bad_checksum(9, "x", 1, 1)))
    show("长度不符帧", gw.ingest(dev.emit_register_truncated(8, "x", 1, 1)))
    show("缺字段JSON", gw.ingest(dev.emit_json_missing_field(7, "x", 1, 1)))
    show("超窗序号(上界7)", gw.ingest(dev.emit_register(10, "temp", 30.0, 110.0)))

    banner("阶段2：下游中断，继续接入（触发有界缓冲与溢出）")
    downstream.set_available(False)
    for s in range(4, 11):
        show(f"中断期序号{s}", gw.ingest(dev.emit_json(s, "vib", 0.1 * s, 100.0 + s)))

    banner("阶段3：下游恢复，FIFO 按序补发")
    downstream.set_available(True)
    sent = gw.on_downstream_restored()
    print(f"  本次补发 {sent} 条")

    banner("最终状态")
    print("  下游收到序号:", [r.seq for r in downstream.received])
    print("  下游收到来源:", sorted({r.source for r in downstream.received}))
    print("  统计:")
    for k, v in gw.stats().items():
        print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
