"""模拟设备端：产生寄存器帧 / JSON 报文（含畸形帧能力）。"""
from __future__ import annotations

import json
import struct

REGISTER_MAGIC = b"\xa5\x5a"
REGISTER_VERSION = 0x01


def _xor_checksum(body: bytes) -> int:
    acc = 0
    for b in body:
        acc ^= b
    return acc


class SimulatedDevice:
    """一个进程内模拟设备，可发出两种协议编码及可控的畸形帧。"""

    def __init__(self, device_id: str) -> None:
        if len(device_id) > 16:
            raise ValueError("device_id 最长 16 字节（寄存器帧限制）")
        self.device_id = device_id

    # ---- 合法帧 -------------------------------------------------------

    def emit_register(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        """组装寄存器风格帧并计算异或校验。"""
        dev = self.device_id.encode("ascii")
        dev_field = dev.ljust(16, b"\x00")
        metric_b = metric.encode("ascii")
        payload_len = 40 + len(metric_b)
        body = (
            REGISTER_MAGIC
            + bytes([REGISTER_VERSION, payload_len])
            + dev_field
            + struct.pack("<I", seq)
            + struct.pack("<I", len(metric_b))
            + metric_b
            + struct.pack("<d", float(value))
            + struct.pack("<d", float(ts))
        )
        return body + bytes([_xor_checksum(body)])

    def emit_json(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        obj = {
            "device_id": self.device_id,
            "seq": seq,
            "metric": metric,
            "value": float(value),
            "timestamp": float(ts),
        }
        return json.dumps(obj).encode("utf-8")

    # ---- 畸形帧 -------------------------------------------------------

    def emit_register_bad_checksum(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        frame = bytearray(self.emit_register(seq, metric, value, ts))
        frame[-1] ^= 0xFF  # 翻转校验字节
        return bytes(frame)

    def emit_register_truncated(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        frame = self.emit_register(seq, metric, value, ts)
        return frame[:-3]  # 截断 3 字节

    def emit_register_bad_length(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        frame = bytearray(self.emit_register(seq, metric, value, ts))
        frame[3] = (frame[3] + 5) & 0xFF  # 篡改载荷长度字段
        # 重新计算校验以隔离“长度不符”与“校验失败”
        frame[-1] = _xor_checksum(bytes(frame[:-1]))
        return bytes(frame)

    def emit_json_missing_field(
        self, seq: int, metric: str, value: float, ts: float
    ) -> bytes:
        obj = {
            "device_id": self.device_id,
            "seq": seq,
            "metric": metric,
            # 故意缺 value / timestamp
        }
        return json.dumps(obj).encode("utf-8")

    def emit_json_malformed(self) -> bytes:
        return b'{"device_id": "' + self.device_id.encode("ascii") + b'", seq: }'

    def emit_json_bad_type(self, seq: int) -> bytes:
        obj = {
            "device_id": self.device_id,
            "seq": seq,
            "metric": "temp",
            "value": "not-a-number",
            "timestamp": 1.0,
        }
        return json.dumps(obj).encode("utf-8")
