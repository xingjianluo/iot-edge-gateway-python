# IoT 边缘网关（进程内模拟）

一套**本地可运行**的工业边缘网关能力演示，使用进程内的模拟设备端与模拟下游汇聚端，
在同一进程内复现多协议数据的接入归一（含**协议自行判别**）、面向设备时序的
幂等/乱序处理（含**按设备差异化策略**）、下游不可用时的有界缓冲与恢复补发，
以及**进程重启后的本地快照恢复**，使异常链路下行为确定、可解释、可复现。

## 目录结构

```text.
├── main.py                       # 演示入口 + HTTP（统计查询/故障注入/恢复）
├── edge_gateway/
│   ├── config.py                 # 配置项及环境变量（全局默认 + 按设备覆盖）
│   ├── models.py                 # 统一读数模型与拒绝原因枚举
│   ├── gateway.py                # 编排：识别/解码→判定→直投/缓冲→恢复/快照
│   ├── policy.py                 # 按设备策略注册表（窗口/容量/溢出，缺省回退）
│   ├── snapshot.py               # 运行状态快照落盘/恢复（原子写，无凭据）
│   ├── buffer.py                 # 全局 FIFO 缓冲 + 每设备容量与两种溢出策略
│   ├── dedup.py                  # 每设备幂等/乱序窗口/单调水位（可导出恢复）
│   ├── downstream.py             # 进程内模拟下游汇聚端（可注入故障）
│   ├── devices.py                # 进程内模拟设备端（含畸形帧/不声明协议）
│   ├── demo.py                   # 逐条打印序号/水位/判定依据的剧本
│   └── protocols/
│       ├── base.py               # 解码器协议接口
│       ├── identification.py     # 协议自行判别（唯一命中/认不出/歧义）
│       ├── register.py           # 寄存器风格二进制帧（CRC16/MODBUS）
│       └── structured.py         # 结构化报文（JSON）
└── tests/                        # 69 个单测/集成测（识别/设备策略/快照/并发）
```

## 1. 支持的协议编码与归一

两路输入最终都还原为统一读数模型 `Reading(device_id, seq, timestamp_ms,
metric, value, source)`，其中 `source` 标记来自 `register` 或 `structured`。

### 1.1 寄存器风格帧（binary, little-endian）

| 偏移 | 长度 | 字段 |
|---|---|---|
| 0 | 2 | magic `0xA55A`（线上 `5A A5`） |
| 2 | 1 | version = `0x01` |
| 3 | 1 | flags（bit0=1 表示值按有符号解析） |
| 4 | 2 | device_id 长度 N |
| 6 | N | device_id（ASCII） |
| 6+N | 4 | seq（uint32） |
| 10+N | 8 | timestamp_ms（uint64） |
| 18+N | 2 | metric 长度 M |
| 20+N | M | metric（ASCII） |
| 20+N+M | 4 | int32 寄存器值，÷100 还原为 float |
| 24+N+M | 2 | **CRC16/MODBUS**，覆盖 CRC 前全部字节 |

合法帧总长 = `26 + N + M`。拒绝原因**不静默丢弃**，按固定顺序判定：
`TRUNCATED_FRAME → BAD_MAGIC → MALFORMED(版本) → CHECKSUM_MISMATCH →
FIELD_MISSING / BAD_VALUE`，每条都带 detail 文本（如实际长度、计算 CRC）。

### 1.2 结构化报文（JSON）

单行 JSON：`{"device":"d","seq":3,"ts":1700000000123,"metric":"temperature","value":21.5}`

- 非 JSON / 顶层非对象 → `MALFORMED`
- 缺少任一必填字段（device/seq/ts/metric/value）→ `FIELD_MISSING`
- 空字符串、负序号、负时间戳、类型不符、bool 冒充数字、NaN/Infinity → `BAD_VALUE`

### 1.3 协议自行判别（设备不声明协议）

接入侧设备只给一段原始字节时，把 `RawMessage.protocol` 置为 `"auto"`
（模拟器可用 `device.emit_auto(seq)`），网关 `ProtocolIdentifier` 依次尝试
全部已注册解码器，按**唯一命中**原则归一：

| 判别结果 | 结论 | 拒绝原因 |
|---|---|---|
| 恰好 1 种协议解码成功 | 接受，`Reading.source` 标明命中协议；`IngestResult.protocol` 回填协议名 | — |
| 没有任何协议成功 | 明确拒绝，detail 逐个列出各解码器的失败原因 | **`UNKNOWN_PROTOCOL`** |
| 2 种及以上同时成功 | 明确拒绝（歧义，绝不二选一硬塞），detail 列出全部命中者 | **`AMBIGUOUS_PROTOCOL`** |

要点：

- 认不出 ≠ 某种协议的坏帧：例如魔数错误的字节在 register 看来是
  `BAD_MAGIC`、在 structured 看来是 `MALFORMED`，自动路径给的是
  `UNKNOWN_PROTOCOL`——"我们认不出这是什么"，不会瞎猜一种硬塞。
- **指定协议的老路保持不变**：`protocol="register"` 时严格按 register
  解析，把 JSON 字节误标成 register 只会得到 register 的帧级拒绝，
  不会偷偷回退到 JSON 解析。
- 非字节输入（如 str）按 `MALFORMED` 拒绝；全部拒绝结论计入
  `decode_rejected`，并分别计入 `unidentified` / `ambiguous` 统计，
  绝不静默丢弃。

## 2. 去重与窗口语义（每设备独立）

状态：`watermark`（已接受最大序号，**只增不减**）、`seen`（窗口内已接受序号集合，
随水位推进修剪，大小有界）、`last_seen_seq`（最近到达序号）。
设窗口大小为 W，窗口下界 `low = watermark - W + 1`：

| 情形 | 结论 |
|---|---|
| `seq > watermark` | 接受，水位推进到 seq（允许跳号） |
| `low ≤ seq ≤ watermark` 且 seq ∈ seen | **拒绝 DUPLICATE_SEQ**（重复只计一次） |
| `low ≤ seq ≤ watermark` 且 seq ∉ seen | 接受（窗口内乱序/迟到补缺，**水位不动**） |
| `seq < low` 且 seq 小于上一条到达序号 | **拒绝 SEQ_ROLLBACK**（观察到序号回退） |
| `seq < low` 且不倒退 | **拒绝 OUT_OF_WINDOW**（迟到超出可接受窗口） |

- **单调水位**：补缺接受不回退水位，聚合结果不随重放改变（有专门测试
  `test_replay_does_not_change_aggregate` 验证）。
- 每个设备一把锁，设备间互不阻塞；4 线程并发提交同一组 1000 序号，
  恰好 1000 次接受（`test_concurrent_same_device_exactly_once`）。
- 边界：seq 通过时序层后若在缓冲阶段溢出，水位仍已推进；该序号重放会被
  `DUPLICATE_SEQ` 拦截，下游不会重复计数，但其数据不会自动补发（见第 3 节）。

## 3. 缓冲与溢出策略（容量/策略可按设备）

下游不可用时，读数进入 `BoundedSpool`：内部是一条**全局 FIFO**（恢复补发严格
按全局到达序），同时为每台设备维护各自的排队键，**容量按设备独立计数
（硬上限，绝不无限增长）**，设备 A 占满不影响设备 B：

- **`drop_newest`（默认）**：该设备满则拒绝新读数，返回 `BUFFER_OVERFLOW`
  （detail 标明是哪台设备、它的容量与策略）。语义：保证已缓存数据按序、
  不丢旧地补发。
- **`drop_oldest`**：该设备满时腾退**该设备**最旧读数再放入新读数（腾退同时
  移出全局 FIFO，不影响别的设备的顺序）。语义：优先实时性。

每台设备的容量与策略由 `DevicePolicyResolver` 按 `device_id` 解析：有覆盖
用覆盖值，未设置的项回退全局默认，未注册的设备全部走默认。同一台设备无论
第几条报文、哪一次接入，解析结果都稳定一致；设备之间互不影响。

两种策略的丢弃都累加 `dropped_overflow`，可通过 `spool.snapshot_stats()`
或 `GET /stats` 查询（另含 enqueued/delivered/duplicate_delivery/
downstream_failures/flushes/size）。

**恢复补发**：`drain` / `POST /recover` 严格全局 FIFO 逐条投递；投递抛异常
立即中断，失败项保留在队首，下次恢复重试，保证顺序。已成功投递的
`(device_id, seq)` 记录在案，再次出现跳过并计入 `duplicate_delivery`；
下游 `MockDownstream` 自身也按 `(device_id, seq)` 做最终幂等。
全部缓冲操作同一把锁串行，并发接入/补发计数不错乱。

## 4. 配置项（全局默认 + 按设备覆盖）

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `GW_WINDOW_SIZE` | 32 | **全局默认**：每设备乱序/迟到可接受窗口 W（≥1） |
| `GW_SPOOL_CAPACITY` | 100 | **全局默认**：单设备下线缓冲硬上限（条数，≥1） |
| `GW_OVERFLOW_POLICY` | drop_newest | **全局默认**：`drop_newest` / `drop_oldest` |
| `GW_DEVICE_POLICIES` | 无 | 按设备覆盖的 JSON 对象（见下） |

按设备覆盖（每个子字段都可省略，省略即回退全局默认，构造时立即校验）：

```bash
export GW_DEVICE_POLICIES='{
  "line-A/temp": {"window_size": 3},
  "line-B/vibe": {"spool_capacity": 8, "overflow_policy": "drop_oldest"}
}'
```

代码方式：

```python
from edge_gateway.gateway import Gateway
from edge_gateway.policy import DevicePolicy

gw = Gateway(
    window_size=32, spool_capacity=100,
    device_policies={
        "line-A/temp": DevicePolicy(window_size=3),
        "line-B/vibe": DevicePolicy(spool_capacity=8,
                                    overflow_policy="drop_oldest"),
    },
)
gw.policies.resolve("line-A/temp")  # -> ResolvedPolicy(3, 100, 'drop_newest')
gw.policies.resolve("unknown-dev")  # -> ResolvedPolicy(32, 100, 'drop_newest')
```

覆盖值非法（窗口 0、容量 0、拼错策略名、JSON 非法、键名未知等）在配置构造
阶段即抛 `ValueError`，不会拖到数据流里才暴露。

## 5. 拒绝原因一览

`TRUNCATED_FRAME`、`BAD_MAGIC`、`CHECKSUM_MISMATCH`、`FIELD_MISSING`、
`BAD_VALUE`、`MALFORMED`、`UNKNOWN_PROTOCOL`（自动识别：没有协议匹配）、
`AMBIGUOUS_PROTOCOL`（自动识别：多种协议同时匹配）、`DUPLICATE_SEQ`、
`OUT_OF_WINDOW`、`SEQ_ROLLBACK`、`BUFFER_OVERFLOW`——取值互不相同，
日志与统计中可区分。

## 6. 重启后接着跑：本地状态快照

`edge_gateway/snapshot.py` 把网关**真实运行状态**落成一份本地 JSON 快照，
并支持在全新进程中仅凭快照文件恢复：

```python
from edge_gateway.snapshot import save_snapshot, load_snapshot, restore_gateway

save_snapshot(gw, "/var/lib/gw/state.json")          # 临时文件 + os.replace 原子写
state = load_snapshot("/var/lib/gw/state.json")      # 全新进程
gw = restore_gateway(state)                          # 用快照自带配置重建并恢复
gw.on_downstream_recovered()                         # 继续补发缓存
```

快照内容（白名单，**不含任何凭据/密钥/令牌/连接串**）：

- `format` / `version` / `taken_at_ms`；
- `config`：生效的窗口/容量/溢出策略与**按设备覆盖**——它是让恢复后判定
  尺度与重启前一致所必需的策略参数，不是凭据；
- `sequencer`：每设备 `watermark`、`last_seen_seq`、窗口内已见序号集 `seen`；
- `spool`：尚未补发的排队读数（保持全局 FIFO 顺序与字段）、已成功投递键、
  缓冲计数；
- `stats`：各类计数与水位表（含 `unidentified` / `ambiguous`）。

恢复语义（有专门测试逐条断言）：

1. **水位/已见序号接得上**：恢复后重放重启前已交给下游或仍在缓存排队的
   读数，窗口内的一律 `DUPLICATE_SEQ`，水位**只增不减、不倒退**；
2. **不重复投递/计数**：重启前已投递的键随快照恢复，作为补发幂等防线；
   新进程下游为空，恢复后 `drain` 只会收到缓存里尚未投递的读数，恰好一次；
3. **缓存接着补发**：排队读数、顺序与字段完整恢复；恢复后下游仍不可用时
   容量/溢出策略继续按各设备生效；
4. **配置一致性校验**：`state_to_gateway(existing_gateway, state)` 会比对
   窗口/容量/溢出/设备覆盖，尺度不一致直接 `ValueError` 拒绝恢复，避免
   换了策略后窗口语义错配；`restore_gateway(state)` 则直接用快照配置重建。

## 7. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 端到端剧本：自动识别/指定协议、按设备策略、六类畸形帧、
# 中断→按设备缓冲/溢出→落快照→全新对象恢复→补发
uv run python main.py

# 全部 69 个测试；加 -s 可查看每条序号/水位/判定依据日志
uv run pytest -s

# 仅看本轮三类新增行为
uv run pytest tests/test_identification.py -s   # 协议自行判别（7）
uv run pytest tests/test_device_policy.py -s    # 按设备差异化策略（5）
uv run pytest tests/test_snapshot.py -s         # 快照落盘/重启恢复（7）

# 既有用例不回归
uv run pytest tests/test_pipeline.py tests/test_sequencer.py \
  tests/test_spool.py tests/test_protocols.py tests/test_config.py

# 自定义全局配置运行
GW_WINDOW_SIZE=8 GW_SPOOL_CAPACITY=4 GW_OVERFLOW_POLICY=drop_oldest \
  uv run python main.py

# 按设备差异化策略（A 窄窗口；B 小缓冲且丢最旧）
GW_WINDOW_SIZE=32 GW_SPOOL_CAPACITY=100 \
GW_DEVICE_POLICIES='{"line-A/temp":{"window_size":3},"line-B/vibe":{"spool_capacity":2,"overflow_policy":"drop_oldest"}}' \
  uv run python main.py

# HTTP：统计查询（含 unidentified/ambiguous）/ 故障注入 / 恢复
uv run uvicorn main:app
curl localhost:8000/stats
curl -XPOST localhost:8000/downstream/available -d '{"available":false}' \
  -H 'content-type: application/json'
curl -XPOST localhost:8000/recover
```

## 8. 测试覆盖结论（可复现）

| 测试文件 | 覆盖点 |
|---|---|
| `tests/test_protocols.py` | CRC 标准向量、两种协议往返归一、各类畸形帧原因矩阵（23） |
| `tests/test_sequencer.py` | 单调水位、乱序补缺、重复只计一次、回退 vs 超窗、剪枝、并发幂等（8） |
| `tests/test_spool.py` | 容量上限、两种溢出策略、失败保留队首、FIFO 补发、并发一致（6） |
| `tests/test_pipeline.py` | 双协议、乱序重复、9 类畸形帧、中断→恢复保序、重放不变、并发、抖动恢复、未知协议（8） |
| `tests/test_config.py` | 默认值、环境覆盖、非法配置（5） |
| `tests/test_identification.py` | 两类字节自动归一、老路不回退、认不出、坏帧不硬塞、歧义可区分且计数分离（7） |
| `tests/test_device_policy.py` | 缺省回退/稳定解析、窗口与容量按设备独立、重连一致、每设备 drop_oldest（5） |
| `tests/test_snapshot.py` | 状态白名单无凭据、水位/序号集/缓存/计数恢复、重放不重复不回退、文件原子往返、配置错配拒绝、设备策略恢复、自动识别帧重放（7） |
