"""网关运行状态的本地快照与在全新进程中的恢复。

快照内容（只有真实运行状态，绝不含策略设置，也绝不含任何凭据/密钥/
连接串/令牌）：
    format/version   格式标识，供恢复侧校验
    taken_at_ms      写入时刻（诊断用）
    sequencer        每设备 watermark / last_seen_seq / seen（窗口内序号集）
    spool            尚未补发的缓存读数（全局 FIFO 顺序）、已投递键、计数
    stats            网关各类计数（含水位表、丢弃/投递/重复/超窗/歧义等）

策略（窗口大小、缓冲容量、溢出策略、按设备覆盖）不属于快照：它们由
进程自己的配置决定（环境变量 / 构造参数），按设备单独覆盖、没有覆盖的
设备回退全局缺省值。恢复时由调用方把目标网关按目标配置建好，再灌入
纯运行状态。

恢复语义：
    * 严格白名单校验：出现运行状态之外的任何未知字段（任何层级）一律
      拒绝，绝不"多出来也照单全收"；
    * 凭据类内容一律拒绝：任意层级出现 password/token/secret/api_key 等
      凭据样式的键名，立即报错并指出位置，绝不静默接受；
    * 水位、已见序号集合原样恢复：重启前已接受的序号重放 -> DUPLICATE_SEQ，
      水位不倒退、不重复投递、不重复计数；
    * 缓存中排队的读数原样恢复，恢复后再次 drain 才会投递；重启前已投递
      的键同时恢复，作为补发幂等防线；
    * 快照采用"临时文件 + os.replace"原子落盘，读到的永远是完整快照。

注意：快照文件含设备读数数据，应按现场数据治理要求存放；本模块本身不
写入任何凭据类内容。
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import time

SNAPSHOT_FORMAT = "edge-gateway-state"
SNAPSHOT_VERSION = 2

# 快照各层级允许出现的键（白名单；任何多余键都视为污染，拒绝恢复）
_ENVELOPE_KEYS = {
    "format", "version", "taken_at_ms", "stats", "sequencer", "spool",
}
_STATS_KEYS = {
    "total_in", "accepted", "rejected", "decode_rejected", "duplicate",
    "out_of_window", "rollback", "overflow_dropped", "delivered",
    "unidentified", "ambiguous", "watermarks",
}
_SEQUENCER_DEVICE_KEYS = {"watermark", "last_seen_seq", "seen"}
_SPOOL_KEYS = {"queued", "delivered", "stats"}
_SPOOL_STATS_KEYS = {
    "enqueued", "dropped_overflow", "delivered", "duplicate_delivery",
    "downstream_failures", "flushes",
}
_READING_KEYS = {"device_id", "seq", "timestamp_ms", "metric", "value", "source"}

# 凭据类键名片段（小写子串匹配）。快照本来就不该出现这类内容；
# 一旦在任意层级出现，立即拒绝恢复并说明位置。
_CREDENTIAL_MARKERS = (
    "password", "passwd", "secret", "token", "api_key", "apikey",
    "credential", "private_key", "access_key",
)


class SnapshotError(ValueError):
    """快照内容不合法（结构、类型、未知字段或凭据污染）。"""


def _is_uint(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _check_credential_keys(obj, where: str) -> None:
    """递归扫描所有字典键：凭据样式键名一律拒绝。"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            lk = str(key).lower()
            for marker in _CREDENTIAL_MARKERS:
                if marker in lk:
                    raise SnapshotError(
                        f"credential-like key {key!r} is not allowed in "
                        f"snapshot at {where}; snapshots must contain only "
                        f"runtime state, never credentials",
                    )
            _check_credential_keys(value, f"{where}.{key}")
    elif isinstance(obj, (list, tuple)):
        for index, item in enumerate(obj):
            _check_credential_keys(item, f"{where}[{index}]")


def _check_whitelist(obj, allowed: set[str], where: str) -> None:
    unknown = set(obj) - allowed
    if unknown:
        raise SnapshotError(
            f"unknown field(s) {sorted(unknown)} in snapshot at {where}; "
            f"snapshots must contain only runtime state, refusing to restore",
        )


def validate_runtime_state(state: dict, *, envelope: bool) -> None:
    """严格校验快照/运行状态：白名单字段 + 类型 + 凭据扫描。

    envelope=True 时额外校验文件快照外壳（format/version/taken_at_ms）；
    直接调用 Gateway.load_state 时传 False（只校验纯运行状态三段）。
    """
    if not isinstance(state, dict):
        raise SnapshotError(
            f"snapshot must be a JSON object, got {type(state).__name__}",
        )

    allowed = set(_ENVELOPE_KEYS) if envelope else {"stats", "sequencer", "spool"}
    # 凭据扫描优先于白名单：即便键名同时是"未知字段"，也要明确报成凭据污染
    _check_credential_keys(state, "<root>")
    _check_whitelist(state, allowed, "<root>")

    if envelope:
        if state.get("format") != SNAPSHOT_FORMAT:
            raise SnapshotError(
                f"not an edge gateway snapshot: format={state.get('format')!r}",
            )
        if state.get("version") != SNAPSHOT_VERSION:
            raise SnapshotError(
                f"unsupported snapshot version: {state.get('version')!r} "
                f"(supported {SNAPSHOT_VERSION})",
            )
        if "taken_at_ms" in state and not _is_uint(state["taken_at_ms"]):
            raise SnapshotError(
                "taken_at_ms must be a non-negative integer",
            )

    for section in ("stats", "sequencer", "spool"):
        if section not in state:
            raise SnapshotError(f"missing required section {section!r}")
        if not isinstance(state[section], dict):
            raise SnapshotError(f"section {section!r} must be an object")

    _validate_stats(state["stats"])
    _validate_sequencer(state["sequencer"])
    _validate_spool(state["spool"])


def _validate_stats(stats: dict) -> None:
    _check_whitelist(stats, _STATS_KEYS, "stats")
    for key in _STATS_KEYS - {"watermarks"}:
        if not _is_uint(stats.get(key)):
            raise SnapshotError(f"stats.{key} must be a non-negative integer")
    watermarks = stats["watermarks"]
    if not isinstance(watermarks, dict):
        raise SnapshotError("stats.watermarks must be an object")
    for device_id, wm in watermarks.items():
        if not isinstance(device_id, str) or not device_id:
            raise SnapshotError("stats.watermarks keys must be non-empty strings")
        if not isinstance(wm, int) or isinstance(wm, bool) or wm < -1:
            raise SnapshotError(
                f"stats.watermarks[{device_id!r}] must be an int >= -1",
            )


def _validate_sequencer(sequencer: dict) -> None:
    for device_id, item in sequencer.items():
        if not isinstance(device_id, str) or not device_id:
            raise SnapshotError("sequencer keys must be non-empty strings")
        if not isinstance(item, dict):
            raise SnapshotError(f"sequencer[{device_id!r}] must be an object")
        _check_whitelist(item, _SEQUENCER_DEVICE_KEYS, f"sequencer.{device_id}")
        for key in ("watermark", "last_seen_seq"):
            value = item.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < -1:
                raise SnapshotError(
                    f"sequencer.{device_id}.{key} must be an int >= -1",
                )
        seen = item.get("seen")
        if not isinstance(seen, list) or not all(_is_uint(s) for s in seen):
            raise SnapshotError(
                f"sequencer.{device_id}.seen must be a list of uint",
            )


def _validate_spool(spool: dict) -> None:
    _check_whitelist(spool, _SPOOL_KEYS, "spool")
    queued = spool["queued"]
    if not isinstance(queued, list):
        raise SnapshotError("spool.queued must be a list")
    for index, item in enumerate(queued):
        where = f"spool.queued[{index}]"
        if not isinstance(item, dict):
            raise SnapshotError(f"{where} must be an object")
        _check_whitelist(item, _READING_KEYS, where)
        device_id = item.get("device_id")
        if not isinstance(device_id, str) or not device_id:
            raise SnapshotError(f"{where}.device_id must be a non-empty string")
        if not _is_uint(item.get("seq")):
            raise SnapshotError(f"{where}.seq must be a uint")
        if not _is_uint(item.get("timestamp_ms")):
            raise SnapshotError(f"{where}.timestamp_ms must be a uint")
        if not isinstance(item.get("metric"), str):
            raise SnapshotError(f"{where}.metric must be a string")
        value = item.get("value")
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
        ):
            raise SnapshotError(f"{where}.value must be a finite number")
        if item.get("source") not in ("register", "structured"):
            raise SnapshotError(
                f"{where}.source must be 'register' or 'structured'",
            )

    delivered = spool["delivered"]
    if not isinstance(delivered, list):
        raise SnapshotError("spool.delivered must be a list")
    for index, pair in enumerate(delivered):
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) != 2
            or not isinstance(pair[0], str)
            or not _is_uint(pair[1])
        ):
            raise SnapshotError(
                f"spool.delivered[{index}] must be a [device_id, seq] pair",
            )

    spool_stats = spool["stats"]
    if not isinstance(spool_stats, dict):
        raise SnapshotError("spool.stats must be an object")
    _check_whitelist(spool_stats, _SPOOL_STATS_KEYS, "spool.stats")
    for key in _SPOOL_STATS_KEYS:
        if not _is_uint(spool_stats.get(key)):
            raise SnapshotError(f"spool.stats.{key} must be a non-negative integer")


def gateway_to_state(gateway, *, taken_at_ms: int | None = None) -> dict:
    """导出门户运行状态为可 JSON 化的纯字典（不含任何策略/凭据）。"""
    state = gateway.export_state()
    state["format"] = SNAPSHOT_FORMAT
    state["version"] = SNAPSHOT_VERSION
    state["taken_at_ms"] = (
        time.time_ns() // 1_000_000 if taken_at_ms is None else taken_at_ms
    )
    return state


def state_to_gateway(gateway, state: dict) -> None:
    """把状态字典恢复进网关。

    快照只含纯运行状态：策略（窗口/容量/溢出/设备覆盖）以目标网关自身
    的生效配置为准，调用方应按目标进程配置先把网关建好。任何未知字段或
    凭据类内容都会被严格拒绝。
    """
    validate_runtime_state(state, envelope=True)
    gateway.load_state(_runtime_only(state))


def _runtime_only(state: dict) -> dict:
    """剥掉文件快照外壳，只留 stats/sequencer/spool 三段运行状态。"""
    return {key: state[key] for key in ("stats", "sequencer", "spool")}


def restore_gateway(
    state: dict,
    *,
    config=None,
    downstream=None,
    window_size: int | None = None,
    spool_capacity: int | None = None,
    overflow_policy: str | None = None,
    device_policies: dict | None = None,
):
    """在全新进程里重建网关并恢复纯运行状态。

    策略必须由恢复进程显式提供（环境变量/构造参数），快照不携带策略：
        config          GatewayConfig；省略时取进程环境配置；
        window_size / spool_capacity / overflow_policy / device_policies
                        与 Gateway 构造参数同义，用来在 config 之上覆盖
                        （包括按设备单独覆盖；未覆盖设备回退全局缺省值）。
    """
    from .config import GatewayConfig, config_from_env
    from .gateway import Gateway

    validate_runtime_state(state, envelope=True)
    if config is None:
        config = config_from_env()
    elif not isinstance(config, GatewayConfig):
        raise SnapshotError(
            "config must be a GatewayConfig; policies come from the "
            "restarting process's own configuration, never the snapshot",
        )
    gateway = Gateway(
        config,
        downstream=downstream,
        window_size=window_size,
        spool_capacity=spool_capacity,
        overflow_policy=overflow_policy,
        device_policies=device_policies,
    )
    gateway.load_state(_runtime_only(state))
    return gateway


def save_snapshot(gateway, path, *, taken_at_ms: int | None = None) -> None:
    """原子写入快照文件（同目录临时文件 + os.replace）。"""
    state = gateway_to_state(gateway, taken_at_ms=taken_at_ms)
    directory = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".gw-snapshot-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False, sort_keys=True)
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


def load_snapshot(path: str) -> dict:
    """读取并解析快照文件。"""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
