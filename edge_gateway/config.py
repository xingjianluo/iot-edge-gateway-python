"""网关配置项（可代码构造，也可从环境变量覆盖）。

环境变量（均有默认值，未设置时取默认）：
    GW_WINDOW_SIZE       全局默认：每设备乱序可接受窗口 W（序号数，>=1）
    GW_SPOOL_CAPACITY    全局默认：下线期间边界缓冲硬上限（条数，>=1）
    GW_OVERFLOW_POLICY   全局默认：drop_newest（默认） | drop_oldest
    GW_DEVICE_POLICIES   按设备覆盖的 JSON 对象（可选），形如
        {"dev-a":{"window_size":4},
         "dev-b":{"spool_capacity":8,"overflow_policy":"drop_oldest"}}
        每个子字段都可省略，省略项回退全局默认；字段非法立即报错。
        注意：这是策略配置，不是运行状态，不会进入状态快照。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .buffer import OverflowPolicy
from .policy import DevicePolicy


@dataclass(frozen=True, slots=True)
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
        if not isinstance(self.device_policies, dict):
            raise ValueError("device_policies must be a dict")  # noqa: TRY004
        # 委托解析器做逐项校验（构造即验证，坏配置提前暴露）
        from .policy import DevicePolicyResolver

        DevicePolicyResolver(
            default_window_size=self.window_size,
            default_spool_capacity=self.spool_capacity,
            default_overflow_policy=self.overflow_policy,
            overrides=self.device_policies,
        )
        return self


def _parse_device_policies(raw: str) -> dict[str, DevicePolicy]:
    """解析 GW_DEVICE_POLICIES 环境变量；结构非法带上下文报错。"""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"GW_DEVICE_POLICIES is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("GW_DEVICE_POLICIES must be a JSON object")  # noqa: TRY004
    out: dict[str, DevicePolicy] = {}
    valid_keys = {"window_size", "spool_capacity", "overflow_policy"}
    for device_id, spec in obj.items():
        if not isinstance(device_id, str) or not device_id:
            raise ValueError(
                f"device id in GW_DEVICE_POLICIES must be non-empty string, "
                f"got {device_id!r}",
            )
        if not isinstance(spec, dict):
            raise ValueError(  # noqa: TRY004
                f"policy for {device_id!r} must be a JSON object",
            )
        unknown = set(spec) - valid_keys
        if unknown:
            raise ValueError(
                f"unknown policy keys for {device_id!r}: {sorted(unknown)}",
            )
        out[device_id] = DevicePolicy(
            window_size=spec.get("window_size"),
            spool_capacity=spec.get("spool_capacity"),
            overflow_policy=spec.get("overflow_policy"),
        )
    return out


def config_from_env() -> GatewayConfig:
    """从环境变量读取配置，缺省使用内置默认值。"""
    raw = os.environ.get("GW_DEVICE_POLICIES")
    device_policies = _parse_device_policies(raw) if raw else {}
    return GatewayConfig(
        window_size=int(os.environ.get("GW_WINDOW_SIZE", "32")),
        spool_capacity=int(os.environ.get("GW_SPOOL_CAPACITY", "100")),
        overflow_policy=os.environ.get(
            "GW_OVERFLOW_POLICY", OverflowPolicy.DROP_NEWEST,
        ),
        device_policies=device_policies,
    ).validated()
