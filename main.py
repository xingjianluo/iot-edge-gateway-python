"""本地可运行入口。

用法：
    uv run python main.py            # 直接运行端到端演示剧本
    uv run uvicorn main:app          # 启动 HTTP 服务，查询统计/触发恢复

HTTP：
    GET  /            服务信息
    GET  /stats       网关与边界缓冲统计（含水位、溢出计数）
    POST /downstream/available  体 {"available": true|false} 注入/解除故障
    POST /recover     下游恢复后触发按序补发，返回补发条数
    POST /snapshot    体 {"path": "..."} 落一份本地状态快照
    POST /restore     体 {"path": "..."} 从快照在当前进程恢复运行状态
"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from edge_gateway.demo import run_demo
from edge_gateway.gateway import Gateway
from edge_gateway.models import RejectReason
from edge_gateway.snapshot import SnapshotError, load_snapshot_file, save_snapshot

# 进程内单例网关 + 模拟设备，供 HTTP 演示使用
gateway = Gateway()
app = FastAPI(title="iot-edge-gateway-python")


class AvailabilityBody(BaseModel):
    available: bool


class SnapshotBody(BaseModel):
    path: str


@app.get("/")
def read_root() -> dict[str, str]:
    return {
        "service": "iot-edge-gateway-python",
        "demo": "uv run python main.py",
        "endpoints": "/stats /recover /downstream/available",
    }


@app.get("/stats")
def get_stats() -> dict:
    stats = gateway.stats_snapshot()
    spool = gateway.spool.snapshot_stats()
    return {
        "gateway": {
            "total_in": stats.total_in,
            "accepted": stats.accepted,
            "rejected": stats.rejected,
            "rejected_breakdown": {
                "decode_rejected": stats.decode_rejected,
                "duplicate": stats.duplicate,
                "out_of_window": stats.out_of_window,
                "rollback": stats.rollback,
                "overflow_dropped": stats.overflow_dropped,
            },
            "delivered": stats.delivered,
            "watermarks": stats.watermarks,
        },
        "spool": spool.__dict__,
        "downstream": {
            "available": gateway.downstream.available,
            "received": gateway.downstream.count(),
            "duplicates": gateway.downstream.duplicates,
        },
        "reject_reasons": [r.value for r in RejectReason],
    }


@app.post("/downstream/available")
def set_downstream(body: AvailabilityBody) -> dict[str, bool]:
    gateway.downstream.set_available(body.available)
    return {"available": gateway.downstream.available}


@app.post("/recover")
def recover() -> dict[str, int]:
    return {"flushed": gateway.on_downstream_recovered()}


@app.post("/snapshot")
def create_snapshot(body: SnapshotBody) -> dict:
    """把当前运行状态落成一份本地快照（原子写，仅运行状态白名单）。"""
    path = save_snapshot(gateway, body.path)
    return {"saved": path}


@app.post("/restore")
def restore_snapshot(body: SnapshotBody) -> dict:
    """从本地快照恢复水位/已见序号/待发缓存/计数到当前网关。"""
    try:
        snap = load_snapshot_file(body.path)
    except SnapshotError as exc:
        return {"restored": False, "error": str(exc)}
    gateway.import_snapshot(snap)
    st = gateway.stats_snapshot()
    return {
        "restored": True,
        "watermarks": st.watermarks,
        "spool_size": gateway.spool.snapshot_stats().size,
    }


if __name__ == "__main__":
    run_demo()
