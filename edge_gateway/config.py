"""网关配置项（可代码构造，也可从环境变量覆盖）。

环境变量（均有默认值，未设置时取默认）：
    GW_WINDOW_SIZE       每设备乱序可接受窗口 W（序号数，>=1）
    GW_SPOOL_CAPACITY    下游中断时边界缓冲硬上限（条数，>=1）
    GW_OVERFLOW_POLICY   drop_newest（默认） | drop_oldest
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .buffer import OverflowPolicy


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    window_size: int = 32
    spool_capacity: int = 100
    overflow_policy: str = OverflowPolicy.DROP_NEWEST

    def validated(self) -> "GatewayConfig":
        if self.window_size < 1:
            raise ValueError("GW_WINDOW_SIZE must be >= 1")
        if self.spool_capacity < 1:
            raise ValueError("GW_SPOOL_CAPACITY must be >= 1")
        if self.overflow_policy not in (
            OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST,
        ):
            raise ValueError(
                "GW_OVERFLOW_POLICY must be drop_newest or drop_oldest",
            )
        return self


def config_from_env() -> GatewayConfig:
    """从环境变量读取配置，缺省使用内置默认值。"""
    return GatewayConfig(
        window_size=int(os.environ.get("GW_WINDOW_SIZE", "32")),
        spool_capacity=int(os.environ.get("GW_SPOOL_CAPACITY", "100")),
        overflow_policy=os.environ.get(
            "GW_OVERFLOW_POLICY", OverflowPolicy.DROP_NEWEST,
        ),
    ).validated()
