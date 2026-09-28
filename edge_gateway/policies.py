"""按设备差异化策略：窗口大小、缓冲容量、溢出策略。

每台设备可单独设定上述三项；未设定（None）的项回退到全局默认值。
解析以 device_id 为唯一依据，确定性、无状态：同一台设备无论第几条、
哪一次接入，拿到的策略都相同；设备之间互不影响。
"""

from __future__ import annotations

from dataclasses import dataclass

from .buffer import OverflowPolicy

_VALID_POLICIES = (OverflowPolicy.DROP_NEWEST, OverflowPolicy.DROP_OLDEST)


@dataclass(frozen=True, slots=True)
class DevicePolicy:
    """单设备策略覆盖；None 表示该项沿用全局默认。"""

    window_size: int | None = None
    spool_capacity: int | None = None
    overflow_policy: str | None = None

    def validated(self) -> "DevicePolicy":
        if self.window_size is not None and self.window_size < 1:
            raise ValueError("device window_size must be >= 1")
        if self.spool_capacity is not None and self.spool_capacity < 1:
            raise ValueError("device spool_capacity must be >= 1")
        if (
            self.overflow_policy is not None
            and self.overflow_policy not in _VALID_POLICIES
        ):
            raise ValueError(
                "device overflow_policy must be drop_newest or drop_oldest",
            )
        return self


@dataclass(frozen=True, slots=True)
class ResolvedPolicy:
    """应用默认回退后的具体策略（无 None）。"""

    window_size: int
    spool_capacity: int
    overflow_policy: str


class PolicyResolver:
    """默认值 + 每设备覆盖表；device_id 是唯一解析依据。"""

    def __init__(
        self,
        *,
        default_window_size: int,
        default_spool_capacity: int,
        default_overflow_policy: str,
        device_policies: dict[str, DevicePolicy] | None = None,
    ) -> None:
        if default_window_size < 1:
            raise ValueError("default_window_size must be >= 1")
        if default_spool_capacity < 1:
            raise ValueError("default_spool_capacity must be >= 1")
        if default_overflow_policy not in _VALID_POLICIES:
            raise ValueError(
                "default_overflow_policy must be drop_newest or drop_oldest",
            )
        for dev_id, pol in (device_policies or {}).items():
            if not isinstance(dev_id, str) or not dev_id:
                raise ValueError("device id must be non-empty string")
            pol.validated()
        self._default_window = default_window_size
        self._default_capacity = default_spool_capacity
        self._default_policy = default_overflow_policy
        self._devices = dict(device_policies or {})

    def resolve(self, device_id: str) -> ResolvedPolicy:
        """返回该设备实际生效的具体策略（缺省项回退全局默认）。"""
        pol = self._devices.get(device_id)
        return ResolvedPolicy(
            window_size=(
                pol.window_size if pol and pol.window_size is not None
                else self._default_window
            ),
            spool_capacity=(
                pol.spool_capacity if pol and pol.spool_capacity is not None
                else self._default_capacity
            ),
            overflow_policy=(
                pol.overflow_policy if pol and pol.overflow_policy is not None
                else self._default_policy
            ),
        )

    def window_for(self, device_id: str) -> int:
        return self.resolve(device_id).window_size

    def capacity_for(self, device_id: str) -> int:
        return self.resolve(device_id).spool_capacity

    def policy_for(self, device_id: str) -> str:
        return self.resolve(device_id).overflow_policy

    def device_ids(self) -> list[str]:
        """已配置覆盖的设备（快照需要包含它们，即使尚未出现流量）。"""
        return list(self._devices)
