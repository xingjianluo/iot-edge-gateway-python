"""网关运行状态的本地快照与在全新进程中的恢复。

快照内容（只有真实运行状态，绝不含任何凭据/密钥/连接串）：
    format/version   格式标识，供恢复侧校验
    taken_at_ms      写入时刻（诊断用）
    config           生效的策略性参数（窗口/容量/溢出策略/设备覆盖）。
                     这不是凭据；恢复后判定必须与重启前同一尺度，故随快照
                     一并带上，保证"拿着快照文件在全新进程直接恢复"语义自洽。
    sequencer        每设备 watermark / last_seen_seq / seen（窗口内序号集）
    spool            尚未补发的缓存读数（全局 FIFO 顺序）、已投递键、计数
    stats            网关各类计数（含水位表）

恢复语义：
    * 水位、已见序号集合原样恢复：重启前已接受的序号重放 -> DUPLICATE_SEQ，
      水位不倒退、不重复投递、不重复计数；
    * 缓存中排队的读数原样恢复，恢复后再次 drain 才会投递；重启前已投递的
      键同时恢复，作为补发幂等防线；
    * 快照采用"临时文件 + os.replace"原子落盘，读到的永远是完整快照。

注意：快照文件含设备读数数据，应按现场数据治理要求存放；本模块本身不
写入任何凭据类内容。
"""

from __future__ import annotations

import json
import os
import tempfile
import time

SNAPSHOT_FORMAT = "edge-gateway-state"
SNAPSHOT_VERSION = 1


def gateway_to_state(gateway, *, taken_at_ms: int | None = None) -> dict:
    """导出门户运行状态为可 JSON 化的纯字典。"""
    state = gateway.export_state()
    cfg = gateway.config
    state["format"] = SNAPSHOT_FORMAT
    state["version"] = SNAPSHOT_VERSION
    state["taken_at_ms"] = (
        time.time_ns() // 1_000_000 if taken_at_ms is None else taken_at_ms
    )
    state["config"] = {
        "window_size": cfg.window_size,
        "spool_capacity": cfg.spool_capacity,
        "overflow_policy": cfg.overflow_policy,
        "device_policies": {
            device_id: {
                "window_size": pol.window_size,
                "spool_capacity": pol.spool_capacity,
                "overflow_policy": pol.overflow_policy,
            }
            for device_id, pol in cfg.device_policies.items()
        },
    }
    return state


def state_to_gateway(gateway, state: dict) -> None:
    """把状态字典恢复进网关。

    若 state 自带 config，将与目标网关当前生效配置比对：必须完全一致，
    否则拒绝恢复（避免换了策略尺度后窗口/容量语义错配）。
    """
    if state.get("format") != SNAPSHOT_FORMAT:
        raise ValueError(
            f"not an edge gateway snapshot: format={state.get('format')!r}",
        )
    if state.get("version") != SNAPSHOT_VERSION:
        raise ValueError(
            f"unsupported snapshot version: {state.get('version')!r} "
            f"(supported {SNAPSHOT_VERSION})",
        )
    snap_cfg = state.get("config")
    if snap_cfg is not None:
        _assert_same_config(gateway, snap_cfg)
    gateway.load_state(state)


def _assert_same_config(gateway, snap_cfg: dict) -> None:
    cfg = gateway.config
    if cfg.window_size != snap_cfg.get("window_size"):
        raise ValueError("snapshot config mismatch: window_size")
    if cfg.spool_capacity != snap_cfg.get("spool_capacity"):
        raise ValueError("snapshot config mismatch: spool_capacity")
    if cfg.overflow_policy != snap_cfg.get("overflow_policy"):
        raise ValueError("snapshot config mismatch: overflow_policy")
    current = {
        device_id: {
            "window_size": pol.window_size,
            "spool_capacity": pol.spool_capacity,
            "overflow_policy": pol.overflow_policy,
        }
        for device_id, pol in cfg.device_policies.items()
    }
    if current != snap_cfg.get("device_policies", {}):
        raise ValueError("snapshot config mismatch: device_policies")


def restore_gateway(state: dict, *, downstream=None):
    """便捷入口：依据快照自带配置在全新进程里重建网关并恢复状态。"""
    from .config import GatewayConfig
    from .gateway import Gateway
    from .policy import DevicePolicy

    snap_cfg = state.get("config")
    if snap_cfg is None:
        raise ValueError("snapshot has no config; construct Gateway first")
    overrides = {
        device_id: DevicePolicy(
            window_size=spec.get("window_size"),
            spool_capacity=spec.get("spool_capacity"),
            overflow_policy=spec.get("overflow_policy"),
        )
        for device_id, spec in snap_cfg.get("device_policies", {}).items()
    }
    gateway = Gateway(
        GatewayConfig(
            window_size=snap_cfg["window_size"],
            spool_capacity=snap_cfg["spool_capacity"],
            overflow_policy=snap_cfg["overflow_policy"],
            device_policies=overrides,
        ).validated(),
        downstream=downstream,
    )
    state_to_gateway(gateway, state)
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
