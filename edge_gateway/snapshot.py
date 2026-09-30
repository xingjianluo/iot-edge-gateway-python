"""网关运行状态的本地快照与在全新进程中的恢复。

快照内容（严格收敛为纯运行状态，绝不含策略设置，也不含任何
凭据/密钥/令牌/连接串）：
    format/version   格式标识，供恢复侧校验
    taken_at_ms      写入时刻（诊断用）
    sequencer        每设备 watermark / last_seen_seq / seen（窗口内序号集）
    spool            尚未补发的缓存读数（全局 FIFO 顺序）、已投递键、计数
    stats            网关各类计数（含水位表）

策略（窗口大小/缓冲容量/溢出策略/按设备覆盖）**不进快照**：恢复后用什么
尺度判定，完全由恢复进程自己的配置决定——代码参数或环境变量，包括按
设备单独覆盖、未单独设定的设备回退全局缺省值。

恢复校验（state.validate_runtime_state，白名单 + 凭据嗅探）：
    * 任何层级多出未知字段（哪怕塞的是 "config" 或 "policy"）->
      SnapshotError 明确拒绝；
    * 出现 password/token/secret/api_key 等凭据类键名 -> 点名拒绝；
    * 值类型/结构不符 -> 带路径拒绝；
    * 合法旧快照的恢复行为保持不变。

恢复语义：
    * 水位、已见序号集合原样恢复：重启前已接受的序号重放 -> DUPLICATE_SEQ，
      水位不倒退、不重复投递、不重复计数；
    * 缓存中排队的读数原样恢复，恢复后再次 drain 才会投递；重启前已投递的
      键同时恢复，作为补发幂等防线；
    * 快照采用"临时文件 + os.replace"原子落盘，读到的永远是完整快照。

注意：快照文件含设备读数数据，应按现场数据治理要求存放。
"""

from __future__ import annotations

import json
import os
import tempfile
import time

from .state import validate_runtime_state

SNAPSHOT_FORMAT = "edge-gateway-state"
SNAPSHOT_VERSION = 1


def gateway_to_state(gateway, *, taken_at_ms: int | None = None) -> dict:
    """导出门户运行状态为可 JSON 化的纯字典（只含运行状态）。"""
    state = gateway.export_state()
    state["format"] = SNAPSHOT_FORMAT
    state["version"] = SNAPSHOT_VERSION
    state["taken_at_ms"] = (
        time.time_ns() // 1_000_000 if taken_at_ms is None else taken_at_ms
    )
    return state


def state_to_gateway(gateway, state: dict) -> None:
    """把状态字典恢复进已按本进程配置构建好的网关。

    严格校验通过后才恢复：未知字段/凭据内容/结构错误一律 SnapshotError，
    不会半恢复。策略以目标网关自己的生效配置为准（含按设备覆盖与缺省
    回退），快照不携带、也不能影响策略。
    """
    validate_runtime_state(state, allow_snapshot_envelope=True)
    gateway.load_state(state)


def restore_gateway(state, *, config=None, downstream=None, **config_kwargs):
    """在全新进程中重建网关并恢复状态。

    config       目标进程的 GatewayConfig；为 None 时用 config_kwargs 构造，
                 两者都不给则使用 GatewayConfig 默认（等价 config_from_env
                 之外的内置缺省值）。策略完全由调用方进程决定，不从快照读。
    downstream   可选的模拟下游。
    """
    from .config import GatewayConfig
    from .gateway import Gateway

    validate_runtime_state(state, allow_snapshot_envelope=True)
    if config is None:
        config = GatewayConfig(**config_kwargs).validated()
    gateway = Gateway(config, downstream=downstream)
    gateway.load_state(state)
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
    """读取并解析快照文件（只解析；是否合法在恢复时严格校验）。"""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)
