"""配置项默认值、环境变量覆盖与校验。"""

import pytest

from edge_gateway.buffer import OverflowPolicy
from edge_gateway.config import GatewayConfig, config_from_env


def test_defaults(monkeypatch):
    for key in ("GW_WINDOW_SIZE", "GW_SPOOL_CAPACITY", "GW_OVERFLOW_POLICY"):
        monkeypatch.delenv(key, raising=False)
    cfg = config_from_env()
    assert cfg.window_size == 32
    assert cfg.spool_capacity == 100
    assert cfg.overflow_policy == OverflowPolicy.DROP_NEWEST


def test_env_override(monkeypatch):
    monkeypatch.setenv("GW_WINDOW_SIZE", "8")
    monkeypatch.setenv("GW_SPOOL_CAPACITY", "4")
    monkeypatch.setenv("GW_OVERFLOW_POLICY", "drop_oldest")
    cfg = config_from_env()
    assert cfg == GatewayConfig(8, 4, "drop_oldest")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"window_size": 0},
        {"spool_capacity": 0},
        {"overflow_policy": "nope"},
    ],
)
def test_invalid_config_rejected(kwargs):
    with pytest.raises(ValueError):
        GatewayConfig(**kwargs).validated()
