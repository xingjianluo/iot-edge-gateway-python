"""运行状态快照的结构定义与严格校验（恢复侧唯一信任边界）。

快照只允许描述"纯运行状态"：每设备水位/已见序号、还没补发的缓存读数、
各类计数。策略设置（窗口/容量/溢出策略/按设备覆盖）不属于快照，由进程
自身配置决定；凭据类内容（password/token/secret/key 等）在快照里没有
任何合法位置。

校验原则：白名单 + 凭据嗅探。
    * 任何层级出现白名单之外的键 -> 直接拒绝（哪怕只是多一个 config）；
    * 键名疑似凭据（password/token/secret/apikey 等）-> 拒绝并点名；
    * 值类型/结构不符 -> 拒绝并指出路径。
"""

from __future__ import annotations

# 各层级允许出现的键（严格白名单；新增运行状态字段必须显式登记到这里）
TOP_LEVEL_KEYS = frozenset({"stats", "sequencer", "spool"})
SNAPSHOT_KEYS = frozenset(
    {"format", "version", "taken_at_ms", "stats", "sequencer", "spool"},
)
STATS_KEYS = frozenset({
    "total_in", "accepted", "rejected", "decode_rejected", "duplicate",
    "out_of_window", "rollback", "overflow_dropped", "delivered",
    "unidentified", "ambiguous", "watermarks",
})
STATS_INT_KEYS = STATS_KEYS - {"watermarks"}
SEQUENCER_DEVICE_KEYS = frozenset({"watermark", "last_seen_seq", "seen"})
SPOOL_KEYS = frozenset({"queued", "delivered", "stats"})
SPOOL_STATS_KEYS = frozenset({
    "enqueued", "dropped_overflow", "delivered", "duplicate_delivery",
    "downstream_failures", "flushes",
})
READING_KEYS = frozenset(
    {"device_id", "seq", "timestamp_ms", "metric", "value", "source"},
)

# 凭据类键名嗅探：命中精确名、敏感结尾或敏感子串都算
_CREDENTIAL_NAMES = frozenset({
    "password", "passwd", "pwd", "secret", "token", "credential",
    "credentials", "apikey", "api_key", "access_key", "secret_key",
    "private_key", "auth", "authorization",
})
_CREDENTIAL_SUFFIXES = (
    "password", "passwd", "secret", "token", "credential", "credentials",
    "apikey", "api_key", "access_key", "secret_key", "private_key",
)
_CREDENTIAL_SUBSTRINGS = ("passwd", "secret", "token", "credential")


class SnapshotError(ValueError):
    """快照内容不合法：含未知字段、凭据内容或结构/类型错误。"""


def _is_credential_key(key: str) -> bool:
    k = key.lower()
    if k in _CREDENTIAL_NAMES:
        return True
    if any(k.endswith(suffix) for suffix in _CREDENTIAL_SUFFIXES):
        return True
    return any(part in k for part in _CREDENTIAL_SUBSTRINGS)


def _reject(path: str, message: str) -> None:
    where = f" at {path}" if path else ""
    raise SnapshotError(f"illegal snapshot content{where}: {message}")


def _check_no_unknown(obj, allowed: frozenset, path: str) -> None:
    for key in obj:
        if not isinstance(key, str):
            _reject(path, f"key must be string, got {type(key).__name__}")
        if _is_credential_key(key):
            _reject(
                path,
                f"credential-like key {key!r} is not allowed in a runtime "
                f"state snapshot",
            )
        if key not in allowed:
            _reject(
                path,
                f"unknown field {key!r}; snapshots may contain runtime state "
                f"only (allowed: {sorted(allowed)})",
            )


def _require(obj, expected_type: type, path: str, what: str) -> None:
    if not isinstance(obj, expected_type):
        _reject(
            path,
            f"{what} must be {expected_type.__name__}, "
            f"got {type(obj).__name__}",
        )


def _nonneg_int(value, path: str, what: str) -> int:
    # bool 是 int 子类，显式排除
    if isinstance(value, bool) or not isinstance(value, int):
        _reject(path, f"{what} must be a non-negative integer, got {value!r}")
    if value < 0:
        _reject(path, f"{what} must be >= 0, got {value}")
    return value


def _validate_stats(stats, path: str) -> None:
    _require(stats, dict, path, "stats")
    _check_no_unknown(stats, STATS_KEYS, path)
    for key in STATS_INT_KEYS:
        if key not in stats:
            _reject(path, f"missing stats field {key!r}")
        _nonneg_int(stats[key], f"{path}.{key}", key)
    if "watermarks" not in stats:
        _reject(path, "missing stats field 'watermarks'")
    watermarks = stats["watermarks"]
    _require(watermarks, dict, f"{path}.watermarks", "watermarks")
    for device_id, wm in watermarks.items():
        _require(
            device_id, str, f"{path}.watermarks", "watermark device id",
        )
        if _is_credential_key(device_id):
            _reject(
                f"{path}.watermarks",
                f"credential-like device id {device_id!r} is not allowed",
            )
        _nonneg_int_or_minus_one(wm, f"{path}.watermarks[{device_id!r}]")


def _nonneg_int_or_minus_one(value, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        _reject(path, f"watermark must be int, got {value!r}")
    if value < -1:
        _reject(path, f"watermark must be >= -1, got {value}")


def _validate_sequencer(sequencer, path: str) -> None:
    _require(sequencer, dict, path, "sequencer")
    for device_id, item in sequencer.items():
        sub = f"{path}[{device_id!r}]"
        _require(device_id, str, path, "sequencer device id")
        _require(item, dict, sub, "device sequencer state")
        _check_no_unknown(item, SEQUENCER_DEVICE_KEYS, sub)
        for key in SEQUENCER_DEVICE_KEYS:
            if key not in item:
                _reject(sub, f"missing sequencer field {key!r}")
        _nonneg_int_or_minus_one(item["watermark"], f"{sub}.watermark")
        _nonneg_int_or_minus_one(
            item["last_seen_seq"], f"{sub}.last_seen_seq",
        )
        seen = item["seen"]
        _require(seen, list, f"{sub}.seen", "seen seq set")
        for s in seen:
            _nonneg_int(s, f"{sub}.seen", "seen seq")


def _validate_reading(item, path: str) -> None:
    _require(item, dict, path, "queued reading")
    _check_no_unknown(item, READING_KEYS, path)
    for key in READING_KEYS:
        if key not in item:
            _reject(path, f"missing reading field {key!r}")
    _require(item["device_id"], str, f"{path}.device_id", "device_id")
    _require(item["metric"], str, f"{path}.metric", "metric")
    _require(item["source"], str, f"{path}.source", "source")
    if item["source"] not in ("register", "structured"):
        _reject(
            f"{path}.source",
            f"source must be 'register' or 'structured', "
            f"got {item['source']!r}",
        )
    _nonneg_int(item["seq"], f"{path}.seq", "seq")
    _nonneg_int(
        item["timestamp_ms"], f"{path}.timestamp_ms", "timestamp_ms",
    )
    value = item["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _reject(f"{path}.value", f"value must be number, got {value!r}")


def _validate_spool(spool, path: str) -> None:
    _require(spool, dict, path, "spool")
    _check_no_unknown(spool, SPOOL_KEYS, path)
    for key in SPOOL_KEYS:
        if key not in spool:
            _reject(path, f"missing spool field {key!r}")
    queued = spool["queued"]
    _require(queued, list, f"{path}.queued", "queued")
    for i, item in enumerate(queued):
        _validate_reading(item, f"{path}.queued[{i}]")
    delivered = spool["delivered"]
    _require(delivered, list, f"{path}.delivered", "delivered")
    for i, pair in enumerate(delivered):
        sub = f"{path}.delivered[{i}]"
        _require(pair, (list, tuple), sub, "delivered key")
        if len(pair) != 2:
            _reject(sub, f"delivered key must be [device_id, seq], got {pair!r}")
        device_id, seq = pair
        _require(device_id, str, sub, "delivered device id")
        _nonneg_int(seq, sub, "delivered seq")
    spool_stats = spool["stats"]
    _require(spool_stats, dict, f"{path}.stats", "spool stats")
    _check_no_unknown(spool_stats, SPOOL_STATS_KEYS, f"{path}.stats")
    for key in SPOOL_STATS_KEYS:
        if key not in spool_stats:
            _reject(f"{path}.stats", f"missing spool stats field {key!r}")
        _nonneg_int(spool_stats[key], f"{path}.stats.{key}", key)


def validate_runtime_state(state, *, allow_snapshot_envelope: bool | None = None):
    """严格校验运行状态；非法时抛 SnapshotError 并说明路径与原因。

    接受两种形态（allow_snapshot_envelope）：
        False（默认） 纯运行状态：仅 stats/sequencer/spool；
        True          快照文件外层：额外允许 format/version/taken_at_ms
                      且必须通过格式校验；
        None          自动：外层键缺省按纯状态，出现则必须合法。
    """
    _require(state, dict, "", "snapshot state")
    if allow_snapshot_envelope is None:
        allowed = TOP_LEVEL_KEYS | SNAPSHOT_KEYS
        has_envelope = "format" in state or "version" in state
    else:
        allowed = SNAPSHOT_KEYS if allow_snapshot_envelope else TOP_LEVEL_KEYS
        has_envelope = bool(allow_snapshot_envelope)
    _check_no_unknown(state, allowed, "<root>")
    if has_envelope:
        from .snapshot import SNAPSHOT_FORMAT, SNAPSHOT_VERSION

        if state.get("format") != SNAPSHOT_FORMAT:
            _reject(
                "<root>",
                f"format must be {SNAPSHOT_FORMAT!r}, "
                f"got {state.get('format')!r}",
            )
        if state.get("version") != SNAPSHOT_VERSION:
            _reject(
                "<root>",
                f"version must be {SNAPSHOT_VERSION}, "
                f"got {state.get('version')!r}",
            )
        if "taken_at_ms" in state and state["taken_at_ms"] is not None:
            _nonneg_int(state["taken_at_ms"], "<root>.taken_at_ms",
                        "taken_at_ms")
    else:
        for key in ("format", "version", "taken_at_ms"):
            if key in state:
                _reject(
                    "<root>",
                    f"{key!r} is only valid inside a snapshot envelope",
                )
    for section in ("stats", "sequencer", "spool"):
        if section not in state:
            _reject("<root>", f"missing section {section!r}")
    _validate_stats(state["stats"], "stats")
    _validate_sequencer(state["sequencer"], "sequencer")
    _validate_spool(state["spool"], "spool")
    return state
