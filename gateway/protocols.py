"""两种本地协议编码的解码：寄存器风格帧与结构化 JSON 报文。

寄存器风格帧（小端，定长 22 字节）::

    偏移  长度  字段
    0     2     魔数 0xA5 0x5A
    2     1     版本，固定 0x01
    3     1     载荷长度 N（= 设备ID 16 + 序号 4 + 指标 4 + 值 8 + 时间戳 8 => 40）
    4..19 16    设备 ID，ASCII，NUL 右填充
    20    4     序号 seq，uint32 LE
    24    4     指标名长度 M，uint32 LE
    28..  M     指标名 ASCII
    ...   8     值 value，double LE
    ...   8     时间戳 timestamp，double LE
    末1   1     校验：前述全部字节按字节异或

帧总长 = 头4 + 16 + 4 + 4 + M + 8 + 8 + 1 = 45 + M。
"""
from __future__ import annotations

import json
import struct

from gateway.model import Reading, RejectReason

REGISTER_MAGIC = b"\xa5\x5a"
REGISTER_VERSION = 0x01
DEVICE_ID_FIELD = 16
# 不含可变指标名与帧尾校验的固定部分：魔数2+版本1+长度1+设备16+序号4+指标长度4+值8+时间戳8+校验1
REGISTER_FIXED_OVERHEAD = 45


class DecodeError(Exception):
    """解码失败，携带可区分原因。"""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class FrameDecoder:
    """把原始字节解码为统一 Reading；失败时返回 (None, 原因, 说明)。"""

    source: str = "unknown"

    def decode(self, raw: bytes) -> tuple[Reading | None, RejectReason | None, str]:
        raise NotImplementedError

    def match(self, raw: bytes) -> bool:
        """该解码器是否认领此帧（用于多协议复用）。"""
        return False

    def _fail(self, reason: RejectReason, detail: str):
        return None, reason, detail


class RegisterFrameDecoder(FrameDecoder):
    """寄存器风格二进制帧。"""

    source = "register"

    def match(self, raw: bytes) -> bool:
        return len(raw) >= 2 and raw[:2] == REGISTER_MAGIC

    def decode(self, raw: bytes) -> tuple[Reading | None, RejectReason | None, str]:
        if not isinstance(raw, (bytes, bytearray)):
            return self._fail(RejectReason.DECODE_ERROR, "输入不是字节序列")
        if len(raw) < 2 or raw[:2] != REGISTER_MAGIC:
            return self._fail(RejectReason.DECODE_ERROR, "魔数不匹配，非寄存器帧")
        if len(raw) < REGISTER_FIXED_OVERHEAD:
            return self._fail(
                RejectReason.LENGTH_MISMATCH,
                f"帧长 {len(raw)} 小于最小长度 {REGISTER_FIXED_OVERHEAD}",
            )
        if raw[2] != REGISTER_VERSION:
            return self._fail(
                RejectReason.DECODE_ERROR, f"不支持的协议版本 0x{raw[2]:02x}"
            )
        payload_len = raw[3]
        # 载荷 = 设备16 + 序号4 + 指标长度4 + 指标M + 值8 + 时间戳8 = 40 + M
        metric_len = payload_len - 40
        expect_total = 4 + payload_len + 1  # 头4 + 载荷 + 校验1
        if metric_len <= 0 or len(raw) != expect_total:
            return self._fail(
                RejectReason.LENGTH_MISMATCH,
                f"长度不符：声明载荷 {payload_len}，实际帧长 {len(raw)}，"
                f"期望 {expect_total}",
            )

        # 校验：除末字节外逐字节异或
        body, checksum = raw[:-1], raw[-1]
        calc = 0
        for b in body:
            calc ^= b
        if calc != checksum:
            return self._fail(
                RejectReason.CHECKSUM_FAILED,
                f"校验失败：帧校验=0x{checksum:02x}，计算值=0x{calc:02x}",
            )

        try:
            offset = 4
            device_raw = raw[offset : offset + DEVICE_ID_FIELD]
            offset += DEVICE_ID_FIELD
            (seq,) = struct.unpack_from("<I", raw, offset)
            offset += 4
            (m_len,) = struct.unpack_from("<I", raw, offset)
            offset += 4
            if m_len != metric_len:
                return self._fail(
                    RejectReason.LENGTH_MISMATCH,
                    f"指标长度字段 {m_len} 与载荷声明 {metric_len} 不一致",
                )
            metric_bytes = raw[offset : offset + m_len]
            offset += m_len
            (value,) = struct.unpack_from("<d", raw, offset)
            offset += 8
            (timestamp,) = struct.unpack_from("<d", raw, offset)
        except struct.error as exc:
            return self._fail(RejectReason.LENGTH_MISMATCH, f"字段越界：{exc}")

        try:
            device_id = device_raw.split(b"\x00", 1)[0].decode("ascii")
            metric = metric_bytes.decode("ascii")
        except UnicodeDecodeError as exc:
            return self._fail(RejectReason.BAD_FIELD_TYPE, f"非 ASCII 字段：{exc}")

        if not device_id:
            return self._fail(RejectReason.MISSING_FIELD, "设备 ID 为空")
        if not metric:
            return self._fail(RejectReason.MISSING_FIELD, "指标名为空")

        reading = Reading(
            device_id=device_id,
            seq=int(seq),
            metric=metric,
            value=float(value),
            timestamp=float(timestamp),
            source=self.source,
        )
        return reading, None, ""


class JsonFrameDecoder(FrameDecoder):
    """结构化 JSON 报文。

    期望 JSON 对象::

        {"device_id": "dev-1", "seq": 3, "metric": "temp",
         "value": 21.5, "timestamp": 1727000000.0}
    """

    source = "json"

    REQUIRED = ("device_id", "seq", "metric", "value", "timestamp")

    def match(self, raw: bytes) -> bool:
        if not raw:
            return False
        head = raw.lstrip()[:1]
        return head == b"{" or head == b"["

    def decode(self, raw: bytes) -> tuple[Reading | None, RejectReason | None, str]:
        if not isinstance(raw, (bytes, bytearray)):
            return self._fail(RejectReason.DECODE_ERROR, "输入不是字节序列")
        try:
            obj = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return self._fail(RejectReason.DECODE_ERROR, f"JSON 解析失败：{exc}")
        if not isinstance(obj, dict):
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "报文根节点必须是 JSON 对象"
            )

        missing = [k for k in self.REQUIRED if k not in obj]
        if missing:
            return self._fail(
                RejectReason.MISSING_FIELD, f"缺失必填字段：{', '.join(missing)}"
            )

        device_id = obj["device_id"]
        seq = obj["seq"]
        metric = obj["metric"]
        value = obj["value"]
        timestamp = obj["timestamp"]

        if not isinstance(device_id, str) or not device_id:
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "device_id 必须为非空字符串"
            )
        if not isinstance(metric, str) or not metric:
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "metric 必须为非空字符串"
            )
        # bool 是 int 的子类，显式排除
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "seq 必须为非负整数"
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "value 必须为数值"
            )
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            return self._fail(
                RejectReason.BAD_FIELD_TYPE, "timestamp 必须为数值"
            )

        reading = Reading(
            device_id=device_id,
            seq=int(seq),
            metric=metric,
            value=float(value),
            timestamp=float(timestamp),
            source=self.source,
        )
        return reading, None, ""
