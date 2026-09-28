"""网关配置项（可代码构造，也可从环境变量覆盖）。

环境变量（均有默认值，未设置时取默认）：
    GW_WINDOW_SIZE         每设备乱序可接受窗口 W（序号数，>=1）
    GW_SPOOL_CAPACITY      下游中断时边界缓冲硬上限（条数，>=1）
    GW_OVERFLOW_POLICY     drop_newest（默认） | drop_oldest
    GW_DEVICE_POLICIES     按设备覆盖，JSON 对象，键为 device_id，
                           值为 {"window_size":?,"spool_capacity":?,
                           "overflow_policy":?}（字段均可省略，省略走默认）
                           例：{"line-a":{"window_size":4,"spool_capacity":8,
                               "overflow_policy":"drop_oldest"}}
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .buffer import OverflowPolicy
from .policies import DevicePolicy


@dataclass(frozen=True)
class GatewayConfig:
    window_size: int = 32
    spool_capacity: int = 100
    overflow_policy: str = OverflowPolicy.DROP_NEWEST
    device_policies: dict[str, DevicePolicy] = field(default_factory=dict)

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
        for device_id, policy in self.device_policies.items():
            if not isinstance(device_id, str) or not device_id:
                raise ValueError("device policy key must be non-empty string")
            policy.validated()
        return self


def parse_device_policies(raw: str | None) -> dict[str, DevicePolicy]:
    """解析 GW_DEVICE_POLICIES（JSON 对象，缺省/空串视为无覆盖）。"""
    if raw is None or not raw.strip():
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GW_DEVICE_POLICIES must be valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("GW_DEVICE_POLICIES must be a JSON object")  # noqa: TRY004

    result: dict[str, DevicePolicy] = {}
    for device_id, body in obj.items():
        if not isinstance(device_id, str) or not device_id:
            raise ValueError("device policy key must be non-empty string")
        if not isinstance(body, dict):
            raise ValueError(f"policy for {device_id!r} must be a JSON object")  # noqa: TRY004
        unknown = set(body) - {"window_size", "spool_capacity", "overflow_policy"}
        if unknown:
            raise ValueError(
                f"policy for {device_id!r} has unknown fields: {sorted(unknown)}",
            )
        result[device_id] = DevicePolicy(
            window_size=body.get("window_size"),
            spool_capacity=body.get("spool_capacity"),
            overflow_policy=body.get("overflow_policy"),
        ).validated()
    return result


def config_from_env() -> GatewayConfig:
    """从环境变量读取配置，缺省使用内置默认值。"""
    return GatewayConfig(
        window_size=int(os.environ.get("GW_WINDOW_SIZE", "32")),
        spool_capacity=int(os.environ.get("GW_SPOOL_CAPACITY", "100")),
        overflow_policy=os.environ.get(
            "GW_OVERFLOW_POLICY", OverflowPolicy.DROP_NEWEST,
        ),
        device_policies=parse_device_policies(
            os.environ.get("GW_DEVICE_POLICIES"),
        ),
    ).validated()
