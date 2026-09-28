"""进程内模拟设备端：按脚本产生寄存器帧 / 结构化报文（含异常帧）。

corrupt 支持的注入类型（跨协议对齐到同一组名字）：
    truncate       截断帧/长度不符（register 去尾字节；structured 去尾部）
    bad_crc        CRC 校验失败（仅 register；structured 回退为 malformed）
    bad_magic      帧头魔数错误（仅 register；structured 回退为 malformed）
    missing_field  删除必填字段
    bad_value      把 value 改成非法类型
    malformed      整体不可解析（register 改版本；structured 非 JSON）
"""

from __future__ import annotations

from dataclasses import dataclass

from .protocols.register import encode_register_frame
from .protocols.structured import encode_structured


@dataclass(frozen=True, slots=True)
class RawMessage:
    """设备侧发出的原始报文及其协议与可读标签。"""

    protocol: str  # "register" | "structured" | "auto"（自行判别）
    payload: bytes
    label: str


def auto_message(payload: bytes, label: str = "auto") -> RawMessage:
    """构造"协议自行判别"报文：只给原始字节，不告诉网关编码。"""
    return RawMessage(protocol="auto", payload=payload, label=label)


class SimulatedDevice:
    """按 (seq, corrupt) 脚本产生原始报文。"""

    def __init__(
        self,
        device_id: str,
        protocol: str = "register",
        metric: str = "temperature",
        base_ts: int = 1_700_000_000_000,
        interval_ms: int = 100,
    ) -> None:
        if protocol not in ("register", "structured"):
            raise ValueError(f"unknown protocol: {protocol!r}")
        self.device_id = device_id
        self.protocol = protocol
        self.metric = metric
        self.base_ts = base_ts
        self.interval_ms = interval_ms

    def timestamp_of(self, seq: int) -> int:
        return self.base_ts + seq * self.interval_ms

    def emit(
        self,
        seq: int,
        *,
        value: float | None = None,
        corrupt: str | None = None,
    ) -> RawMessage:
        """产生第 seq 条报文；value 默认随 seq 线性变化。"""
        if value is None:
            value = 20.0 + (seq % 10) + seq / 100.0
        ts = self.timestamp_of(seq)
        label = f"{self.device_id}/{self.protocol}#{seq}"
        if corrupt:
            label += f"[{corrupt}]"

        if self.protocol == "register":
            payload = self._emit_register(seq, ts, value, corrupt)
        else:
            payload = self._emit_structured(seq, ts, value, corrupt)
        return RawMessage(protocol=self.protocol, payload=payload, label=label)

    def _emit_register(
        self, seq: int, ts: int, value: float, corrupt: str | None,
    ) -> bytes:
        if corrupt in (None, "truncate", "bad_crc"):
            frame = encode_register_frame(
                device_id=self.device_id, seq=seq, timestamp_ms=ts,
                metric=self.metric, value=value, bad_crc=(corrupt == "bad_crc"),
            )
            if corrupt == "truncate":
                return frame[:-3]  # 连同 CRC 与值尾字节一起截断
            return frame

        # 以合法帧为基底做确定性破坏
        frame = bytearray(encode_register_frame(
            device_id=self.device_id, seq=seq, timestamp_ms=ts,
            metric=self.metric, value=value,
        ))
        if corrupt == "bad_magic":
            frame[0] ^= 0xFF
        elif corrupt == "malformed":
            frame[2] = 0x09  # 不支持的版本
        elif corrupt == "missing_field":
            # 把 metric_len 改为 0（空 metric -> FIELD_MISSING），
            # 为保持简单直接重编码
            return encode_register_frame(
                device_id=self.device_id, seq=seq, timestamp_ms=ts,
                metric="", value=value,
            )
        elif corrupt == "bad_value":
            # 校验仍然通过，但 device_id 为非 ASCII 字节 -> BAD_VALUE
            return self._raw_register_frame(
                seq=seq, ts=ts, value=value, device_id_bytes=b"dev\xff",
                metric=self.metric,
            )
        else:
            raise ValueError(f"unknown corrupt type: {corrupt!r}")
        return bytes(frame)

    @staticmethod
    def _raw_register_frame(
        *, seq: int, ts: int, value: float, device_id_bytes: bytes,
        metric: str,
    ) -> bytes:
        """以原始 device_id 字节（可非 ASCII）构造 CRC 正确的寄存器帧。"""
        import struct

        from .protocols.register import (
            MAGIC, VERSION, VALUE_SCALE, crc16_modbus,
        )

        met = metric.encode("ascii")
        body = struct.pack("<HBBH", MAGIC, VERSION, 0, len(device_id_bytes))
        body += device_id_bytes
        body += struct.pack("<IQH", seq, ts, len(met))
        body += met
        body += struct.pack("<I", int(round(value * VALUE_SCALE)))
        return body + struct.pack("<H", crc16_modbus(body))

    def _emit_structured(
        self, seq: int, ts: int, value: float, corrupt: str | None,
    ) -> bytes:
        if corrupt is None:
            return encode_structured(
                device_id=self.device_id, seq=seq, timestamp_ms=ts,
                metric=self.metric, value=value,
            )
        if corrupt == "malformed" or corrupt in ("bad_crc", "bad_magic"):
            return b"{oops,not-json"
        if corrupt == "truncate":
            return encode_structured(
                device_id=self.device_id, seq=seq, timestamp_ms=ts,
                metric=self.metric, value=value,
            )[:-3]
        if corrupt == "missing_field":
            return (
                b'{"device":' + _json_bytes(self.device_id)
                + b',"seq":' + str(seq).encode()
                + b',"ts":' + str(ts).encode()
                + b',"value":1}'
            )  # 故意缺 metric
        if corrupt == "bad_value":
            return (
                b'{"device":' + _json_bytes(self.device_id)
                + b',"seq":' + str(seq).encode()
                + b',"ts":' + str(ts).encode()
                + b',"metric":' + _json_bytes(self.metric)
                + b',"value":"not-a-number"}'
            )
        raise ValueError(f"unknown corrupt type: {corrupt!r}")


def _json_bytes(s: str) -> bytes:
    import json

    return json.dumps(s, ensure_ascii=False).encode("utf-8")
