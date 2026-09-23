"""骨架冒烟测试（随实现推进逐步替换为真实用例）。"""

from edge_gateway.models import RejectReason


def test_reason_values_distinct():
    values = [r.value for r in RejectReason]
    assert len(values) == len(set(values))
