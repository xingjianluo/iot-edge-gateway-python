"""网关运行状态快照：落盘与全新进程恢复。

快照内容（严格白名单，只含真实运行状态，不写任何配置与凭据）：
    version                    快照格式版本
    taken_at_ms                生成时刻（毫秒）
    sequencer                  每设备 watermark / last_seen_seq / seen
    spool.queue                尚未补发的读数（FIFO 顺序）
    spool.delivered            已成功投递的 (device_id, seq) 键
    spool.stats                缓冲各类计数
    stats                      网关级计数与水位表

恢复语义：恢复后水位、已见序号、待发缓存、计数与重启前一致；
重启前已交付或已排队的读数再喂一遍，不会倒退水位、不会重复投递/计数。
注意：按设备策略属于配置，不在快照内；恢复时由新进程的配置重新提供。
"""

from __future__ import annotations

import json
import os
import tempfile
import time

SNAPSHOT_VERSION = 1

# 允许出现的顶层/嵌套键白名单（凭据类内容无路径可入）
_ALLOWED_TOP = {"version", "taken_at_ms", "sequencer", "spool", "stats"}
_ALLOWED_SEQ = {"watermark", "last_seen_seq", "seen"}
_ALLOWED_SPOOL = {"queue", "delivered", "stats"}
_ALLOWED_READING = {
    "device_id", "seq", "timestamp_ms", "metric", "value", "source",
}
_ALLOWED_KEY = {"device_id", "seq"}
_ALLOWED_SPOOL_STATS = {
    "enqueued", "dropped_overflow", "delivered", "duplicate_delivery",
    "downstream_failures", "flushes",
}
_ALLOWED_GW_STATS = {
    "total_in", "accepted", "rejected", "decode_rejected", "duplicate",
    "out_of_window", "rollback", "overflow_dropped", "delivered",
    "watermarks",
}

_FORBIDDEN_NAME_PARTS = (
    "password", "passwd", "secret", "token", "credential", "apikey",
    "api_key", "authorization", "auth",
)


class SnapshotError(ValueError):
    """快照格式不合法或被篡改时抛出。"""


def build_snapshot(gateway, *, taken_at_ms: int | None = None) -> dict:
    """在网关锁内收集一致快照并校验白名单。"""
    with gateway._lock:
        snap = {
            "version": SNAPSHOT_VERSION,
            "taken_at_ms": (
                taken_at_ms if taken_at_ms is not None
                else time.time_ns() // 1_000_000
            ),
            "sequencer": gateway.sequencer.export_state(),
            "spool": gateway.spool.export_state(),
            "stats": {
                "total_in": gateway.stats.total_in,
                "accepted": gateway.stats.accepted,
                "rejected": gateway.stats.rejected,
                "decode_rejected": gateway.stats.decode_rejected,
                "duplicate": gateway.stats.duplicate,
                "out_of_window": gateway.stats.out_of_window,
                "rollback": gateway.stats.rollback,
                "overflow_dropped": gateway.spool.stats.dropped_overflow,
                "delivered": gateway.stats.delivered,
                "watermarks": dict(gateway.stats.watermarks),
            },
        }
    _assert_whitelist(snap)
    return snap


def save_snapshot(gateway, path: str) -> str:
    """原子写入快照文件（临时文件 + os.replace），返回写入路径。"""
    snap = build_snapshot(gateway)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".gw-snapshot-", suffix=".tmp", dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(snap, fh, ensure_ascii=False, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise
    return path


def load_snapshot_file(path: str) -> dict:
    """读取并严格校验快照文件（白名单 + 基础类型）。"""
    with open(path, encoding="utf-8") as fh:
        try:
            snap = json.load(fh)
        except json.JSONDecodeError as exc:
            raise SnapshotError(f"snapshot is not valid JSON: {exc}") from exc
    validate_snapshot(snap)
    return snap


def validate_snapshot(snap: dict) -> None:
    """对快照做版本与白名单校验。"""
    if not isinstance(snap, dict):
        raise SnapshotError("snapshot must be a JSON object")
    _assert_whitelist(snap)
    if snap.get("version") != SNAPSHOT_VERSION:
        raise SnapshotError(
            f"unsupported snapshot version: {snap.get('version')!r}",
        )
    if not isinstance(snap.get("taken_at_ms"), int):
        raise SnapshotError("taken_at_ms must be int")
    if not isinstance(snap.get("sequencer"), dict):
        raise SnapshotError("sequencer must be object")
    for device_id, body in snap["sequencer"].items():
        if not isinstance(device_id, str) or not device_id:
            raise SnapshotError("sequencer device id must be non-empty str")
        if not isinstance(body, dict) or set(body) != _ALLOWED_SEQ:
            raise SnapshotError(f"sequencer entry malformed for {device_id!r}")
        if not isinstance(body["watermark"], int) or body["watermark"] < -1:
            raise SnapshotError("watermark must be int >= -1")
        if not isinstance(body["last_seen_seq"], int) \
                or body["last_seen_seq"] < -1:
            raise SnapshotError("last_seen_seq must be int >= -1")
        if not all(isinstance(s, int) for s in body["seen"]):
            raise SnapshotError("seen must be int list")

    spool = snap.get("spool")
    if not isinstance(spool, dict) or set(spool) != _ALLOWED_SPOOL:
        raise SnapshotError("spool malformed")
    for item in spool["queue"]:
        if not isinstance(item, dict) or set(item) != _ALLOWED_READING:
            raise SnapshotError("spool queue item malformed")
    for item in spool["delivered"]:
        if not isinstance(item, dict) or set(item) != _ALLOWED_KEY:
            raise SnapshotError("spool delivered key malformed")
    stats = spool["stats"]
    if not isinstance(stats, dict) or set(stats) != _ALLOWED_SPOOL_STATS:
        raise SnapshotError("spool stats malformed")
    if not all(isinstance(v, int) for v in stats.values()):
        raise SnapshotError("spool stats must be ints")

    gw = snap.get("stats")
    if not isinstance(gw, dict) or set(gw) != _ALLOWED_GW_STATS:
        raise SnapshotError("gateway stats malformed")
    for key in _ALLOWED_GW_STATS - {"watermarks"}:
        if not isinstance(gw[key], int):
            raise SnapshotError(f"gateway stats {key} must be int")
    watermarks = gw["watermarks"]
    if not isinstance(watermarks, dict) or not all(
        isinstance(k, str) and isinstance(v, int)
        for k, v in watermarks.items()
    ):
        raise SnapshotError("watermarks must be str->int object")


def _assert_whitelist(snap: dict) -> None:
    """递归确认：键集合全部在白名单内，且无凭据风格的键名。"""

    def check(node, allowed: set[str], path: str) -> None:
        if isinstance(node, dict):
            extra = set(node) - allowed
            if extra:
                raise SnapshotError(
                    f"unexpected key(s) at {path}: {sorted(extra)}",
                )
            for key in node:
                lowered = key.lower()
                if any(part in lowered for part in _FORBIDDEN_NAME_PARTS):
                    raise SnapshotError(
                        f"forbidden credential-like key at {path}: {key!r}",
                    )

    check(snap, _ALLOWED_TOP, "/")
    sequencer = snap.get("sequencer", {})
    for device_id, body in sequencer.items():
        check(body, _ALLOWED_SEQ, f"/sequencer/{device_id}")
    spool = snap.get("spool", {})
    check(spool, _ALLOWED_SPOOL, "/spool")
    for i, item in enumerate(spool.get("queue", [])):
        check(item, _ALLOWED_READING, f"/spool/queue/{i}")
    for i, item in enumerate(spool.get("delivered", [])):
        check(item, _ALLOWED_KEY, f"/spool/delivered/{i}")
    check(spool.get("stats", {}), _ALLOWED_SPOOL_STATS, "/spool/stats")
    gw = snap.get("stats", {})
    check(gw, _ALLOWED_GW_STATS, "/stats")
