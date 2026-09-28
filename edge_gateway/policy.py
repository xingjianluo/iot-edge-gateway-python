"""按设备差异化策略：窗口/缓冲容量/溢出策略的注册表与缺省回退。

语义：
    * 每台设备以 device_id 为唯一、稳定的键；同一台设备第几条报文、
      哪一次接入，解析出的生效策略完全相同（注册表只随显式配置变化）。
    * 单台设备可分别覆盖乱序窗口、下线缓冲容量、缓冲溢出策略三项；
      覆盖值为 None（或缺省）的项回退到全局默认。
    * 设备之间互不影响：A 的窗口/容量与 B 完全独立。
    * 覆盖值在注册时即完成校验，坏配置在接入前暴露，而不是在数据流中
      随机失败。
"""

from __future__ import annotations

from dataclasses import dataclass

from .buffer import OverflowPolicy


@dataclass(frozen=True, slots=True)
class DevicePolicy:
    """单台设备的策略覆盖项；None 表示该项回退全局默认。"""

    window_size: int | None = None
    spool_capacity: int | None = None
    overflow_policy: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedPolicy:
    """某台设备在默认值上叠加自身覆盖后的生效策略（全部为具体值）。"""

    window_size: int
    spool_capacity: int
    overflow_policy: str


_VALID_OVERFLOW = (OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST)


class DevicePolicyResolver:
    """以 device_id 为稳定键解析生效策略；未注册设备走全局默认。"""

    def __init__(
        self,
        *,
        default_window_size: int,
        default_spool_capacity: int,
        default_overflow_policy: str,
        overrides: dict[str, DevicePolicy] | None = None,
    ) -> None:
        if default_window_size < 1:
            raise ValueError("default window_size must be >= 1")
        if default_spool_capacity < 1:
            raise ValueError("default spool_capacity must be >= 1")
        if default_overflow_policy not in _VALID_OVERFLOW:
            raise ValueError(
                f"unknown overflow_policy: {default_overflow_policy!r}",
            )
        self._default = ResolvedPolicy(
            window_size=default_window_size,
            spool_capacity=default_spool_capacity,
            overflow_policy=default_overflow_policy,
        )
        self._overrides: dict[str, DevicePolicy] = {}
        for device_id, pol in (overrides or {}).items():
            self.set_policy(device_id, pol)

    def set_policy(self, device_id: str, policy: DevicePolicy) -> None:
        """注册/替换一台设备的覆盖项，并立即校验。"""
        if not isinstance(device_id, str) or not device_id:
            raise ValueError("device_id must be non-empty string")
        if not isinstance(policy, DevicePolicy):
            raise TypeError("policy must be a DevicePolicy")
        if policy.window_size is not None and policy.window_size < 1:
            raise ValueError(
                f"window_size for {device_id!r} must be >= 1",
            )
        if policy.spool_capacity is not None and policy.spool_capacity < 1:
            raise ValueError(
                f"spool_capacity for {device_id!r} must be >= 1",
            )
        if (
            policy.overflow_policy is not None
            and policy.overflow_policy not in _VALID_OVERFLOW
        ):
            raise ValueError(
                f"overflow_policy for {device_id!r} must be one of "
                f"{_VALID_OVERFLOW}, got {policy.overflow_policy!r}",
            )
        self._overrides[device_id] = policy

    def resolve(self, device_id: str) -> ResolvedPolicy:
        """返回该设备叠加默认值后的完整生效策略（结果稳定、可缓存）。"""
        pol = self._overrides.get(device_id)
        if pol is None:
            return self._default
        return ResolvedPolicy(
            window_size=(
                pol.window_size
                if pol.window_size is not None else self._default.window_size
            ),
            spool_capacity=(
                pol.spool_capacity
                if pol.spool_capacity is not None
                else self._default.spool_capacity
            ),
            overflow_policy=(
                pol.overflow_policy
                if pol.overflow_policy is not None
                else self._default.overflow_policy
            ),
        )

    def window_for(self, device_id: str) -> int:
        return self.resolve(device_id).window_size

    def spool_capacity_for(self, device_id: str) -> int:
        return self.resolve(device_id).spool_capacity

    def overflow_policy_for(self, device_id: str) -> str:
        return self.resolve(device_id).overflow_policy

    def known_devices(self) -> tuple[str, ...]:
        """已注册覆盖策略的设备（快照/诊断用）。"""
        return tuple(sorted(self._overrides))
