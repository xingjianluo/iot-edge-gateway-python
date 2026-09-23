"""测试夹具：统一构造网关与模拟设备。"""
import logging

import pytest

from gateway.buffer import OverflowPolicy
from gateway.dedup import DeviceStateRegistry, DeviceWatermarkState
from gateway.downstream import MockDownstream
from gateway.gateway import EdgeGateway
from gateway.protocols import JsonFrameDecoder, RegisterFrameDecoder
from gateway.device import SimulatedDevice


@pytest.fixture
def decoders():
    return [RegisterFrameDecoder(), JsonFrameDecoder()]


@pytest.fixture
def downstream():
    return MockDownstream(available=True)


@pytest.fixture
def make_gateway(decoders, downstream):
    def _make(window=5, capacity=100, policy=OverflowPolicy.DROP_NEWEST):
        return EdgeGateway(
            decoders=decoders,
            window=window,
            capacity=capacity,
            overflow_policy=policy,
            downstream=downstream,
            log_decisions=True,
        )
    return _make


@pytest.fixture
def device():
    return SimulatedDevice("dev-1")


@pytest.fixture(autouse=True)
def _gateway_logs(caplog):
    """保证每个用例都能采集并打印网关判定日志（序号/水位/依据）。"""
    caplog.set_level(logging.INFO, logger="gateway")
    yield
