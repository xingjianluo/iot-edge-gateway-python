"""本地可运行入口：默认跑端到端演示；也可通过 uvicorn 起 HTTP 查看统计。"""

from __future__ import annotations

from fastapi import FastAPI

from edge_gateway.demo import run_demo

app = FastAPI(title="iot-edge-gateway-python")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"service": "iot-edge-gateway-python", "run": "python main.py --demo"}


if __name__ == "__main__":
    run_demo()
