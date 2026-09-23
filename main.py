"""可选的 HTTP 观测入口（核心能力均为进程内实现，不依赖 Web 服务）。

启动：uv run fastapi dev main.py
- GET /health  服务存活
- GET /stats   进程内网关单例统计（演示/调试用）
核心链路的本地验证请使用 ``uv run python demo.py`` 与 pytest。
"""
from __future__ import annotations

from fastapi import FastAPI

from gateway.buffer import OverflowPolicy
from gateway.downstream import MockDownstream
from gateway.gateway import EdgeGateway
from gateway.protocols import JsonFrameDecoder, RegisterFrameDecoder


def _build_gateway() -> EdgeGateway:
    return EdgeGateway(
        decoders=[RegisterFrameDecoder(), JsonFrameDecoder()],
        window=3,
        capacity=1000,
        overflow_policy=OverflowPolicy.DROP_NEWEST,
        downstream=MockDownstream(available=True),
        log_decisions=True,
    )


app = FastAPI(title="iot-edge-gateway-python")
gateway = _build_gateway()


@app.get("/")
def read_root():
    return {"service": "iot-edge-gateway-python", "status": "ok"}


@app.get("/health")
def health():
    return {"status": "up", "downstream_available": gateway.downstream.available}


@app.get("/stats")
def stats():
    return gateway.stats()
