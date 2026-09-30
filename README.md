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
│   ├── models.py                 # 统一读数模型与拒绝原因枚举/归属判别类型
│   ├── gateway.py                # 编排：识别/解码→判定→直投/缓冲→恢复/快照
│   ├── policy.py                 # 按设备策略注册表（窗口/容量/溢出，缺省回退）
│   ├── state.py                  # 快照严格白名单校验（拒未知字段/凭据内容）
│   ├── snapshot.py               # 运行状态快照落盘/恢复（原子写，无策略无凭据）
│   ├── buffer.py                 # 全局 FIFO 缓冲 + 每设备容量与两种溢出策略
│   ├── dedup.py                  # 每设备幂等/乱序窗口/单调水位（可导出恢复）
│   ├── downstream.py             # 进程内模拟下游汇聚端（可注入故障）
│   ├── devices.py                # 进程内模拟设备端（含畸形帧/不声明协议）
│   ├── demo.py                   # 逐条打印序号/水位/判定依据的剧本
│   └── protocols/
│       ├── base.py               # 解码器协议接口（decode + recognize）
│       ├── identification.py     # 协议自行判别（认得出但帧坏/认不出/歧义）
│       ├── register.py           # 寄存器风格二进制帧（CRC16/MODBUS）
│       └── structured.py         # 结构化报文（JSON）
└── tests/                        # 76 个单测/集成测（识别/设备策略/快照/并发）
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
（模拟器可用 `device.emit_auto(seq)`），网关让每台已注册解码器先对字节做
**归属判断**（`recognize`），再按**唯一认领**原则归一。归属判断有三档：

- `MATCH`：属于本协议且完整解码成功；
- `CORRUPT`：**能确认属于本协议**（register 看帧头魔数 `5A A5`；
  structured 看"能否解析成一个 JSON 对象"），但帧本身坏了——校验失败、
  被截断、必填字段缺失、取值非法等；
- `UNRELATED`：字节不属于本协议（register 魔数不对/头不足；
  structured 不是 JSON 对象）。

汇总后的最终结论：

| 情形 | 结论 | 拒绝原因 |
|---|---|---|
| 恰好 1 个 `MATCH`，无其它认领 | 接受，`Reading.source` 标明命中协议；`IngestResult.protocol` 回填协议名 | — |
| 恰好 1 个 `CORRUPT` 认领 | 按**该协议自己的、可区分的**帧级原因拒绝，并回填协议名与从字节里读到的 `device_id`/`seq` 依据 | `CHECKSUM_MISMATCH` / `TRUNCATED_FRAME` / `FIELD_MISSING` / `BAD_VALUE` / `MALFORMED` |
| 没有任何解码器认领 | 明确拒绝（"认不出是什么"），detail 逐个列出各解码器"为什么不像" | **`UNKNOWN_PROTOCOL`** |
| 2 个及以上认领（含双方都 `CORRUPT`、或一 `MATCH` 一 `CORRUPT`） | 明确拒绝（歧义，绝不二选一硬塞），detail 列出全部认领方 | **`AMBIGUOUS_PROTOCOL`** |

三类结论在统计与日志里**分开**：认得出的坏帧只计入 `decode_rejected`，
原因就是具体帧级原因（与设备提前声明协议时逐字节一致）；认不出计入
`unidentified`；歧义计入 `ambiguous`——后两者也都计入 `decode_rejected`。
现场排查时可以一眼分清"设备发了我们不认识的东西"还是"我们支持的编码发坏了"。

要点与边界：

- **认得出但帧坏 ≠ 认不出**：魔数正确但 CRC 错误、长度不符、版本不对、
  metric 为空、device_id 非 ASCII 的 register 字节，自动路径直接给
  `CHECKSUM_MISMATCH` / `TRUNCATED_FRAME` / `MALFORMED` /
  `FIELD_MISSING` / `BAD_VALUE`；能从帧里读出的 `device_id`、`seq` 会
  放在 `IngestResult.evidence` 与日志 detail 里（读不出的字段不臆造）。
- **真正认不出仍报认不出**：魔数错误的 register 字节（连帧头都不像）、
  截断到已不是合法 JSON 的文本，归不到任何已支持编码，仍是
  `UNKNOWN_PROTOCOL`。
- **歧义单独报**：真实编码里 register 帧头与 JSON 对象互斥，歧义主要由
  桩/未来新编码触发；只要两方都认领（哪怕认领的是坏帧），一律
  `AMBIGUOUS_PROTOCOL`，绝不硬挑一种。
- **指定协议的老路保持不变**：`protocol="register"` 时严格按 register
  解析，把 JSON 字节误标成 register 只会得到 register 的帧级拒绝，
  不会偷偷回退到 JSON 解析。
- 非字节输入（如 str）按 `MALFORMED` 拒绝；任何情况下都不静默丢弃、
  不硬挑一种塞进去。

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
`BAD_VALUE`、`MALFORMED`（这六项在指定协议路径与自动识别的"认得出但
帧坏"路径下结论一致）、`UNKNOWN_PROTOCOL`（自动识别：归不到任何已支持
编码）、`AMBIGUOUS_PROTOCOL`（自动识别：多种编码同时认领）、
`DUPLICATE_SEQ`、`OUT_OF_WINDOW`、`SEQ_ROLLBACK`、`BUFFER_OVERFLOW`
——取值互不相同，日志与统计中可区分。

## 6. 重启后接着跑：本地状态快照

`edge_gateway/snapshot.py` 把网关**纯运行状态**落成一份本地 JSON 快照。
**策略设置不进快照**：窗口大小、缓冲容量、溢出策略以及按设备覆盖，全部
由恢复进程自己的配置决定（构造参数或环境变量，设备有单独覆盖用覆盖、
没有的回退全局缺省值）。

```python
from edge_gateway.gateway import Gateway
from edge_gateway.policy import DevicePolicy
from edge_gateway.snapshot import save_snapshot, load_snapshot, restore_gateway

save_snapshot(gw, "/var/lib/gw/state.json")          # 临时文件 + os.replace 原子写
state = load_snapshot("/var/lib/gw/state.json")      # 全新进程

# 恢复时由本进程显式给出生效策略（含按设备覆盖）；快照里没有、也改不动策略
gw = restore_gateway(
    state,
    window_size=8, spool_capacity=100,
    device_policies={"line-A/temp": DevicePolicy(window_size=3)},
)
gw.on_downstream_recovered()                         # 继续补发缓存

# 也可以先按本进程配置构造网关，再把状态恢复进去（state_to_gateway /
# gateway.load_state），两条路径都走同一套严格校验
gw2 = Gateway(window_size=8, spool_capacity=100)
gw2.load_state(state)
```

快照内容（**严格白名单，只有运行状态**）：

- `format` / `version` / `taken_at_ms`：格式标识与写入时刻（诊断用）；
- `sequencer`：每设备 `watermark`、`last_seen_seq`、窗口内已见序号集 `seen`；
- `spool`：尚未补发的排队读数（保持全局 FIFO 顺序与完整字段）、已成功
  投递键 `delivered`、缓冲计数（enqueued/dropped_overflow/delivered/
  duplicate_delivery/downstream_failures/flushes）；
- `stats`：网关各类计数与水位表（含 `unidentified` / `ambiguous` /
  `overflow_dropped` 等）。

**不含**任何策略字段，也**不含**凭据/密钥/令牌/连接串。

### 6.1 恢复时的严格校验与拒绝

恢复入口（`restore_gateway` / `state_to_gateway` / `Gateway.load_state`）
统一调用 `edge_gateway/state.py` 的白名单校验，任何层级：

- 出现白名单之外的字段（外层、`stats`、`sequencer`、`spool`、排队读数
  各层都查）——哪怕只是多一个 `config`、`policy`——抛 `SnapshotError`
  并指出字段名与所在位置，**绝不照单全收**；
- 键名疑似凭据（精确或后缀匹配 `password`/`passwd`/`secret`/`token`/
  `credential`/`api_key`/`access_key`/`secret_key`/`private_key` 等）
  ——点名拒绝；
- 类型/结构不符（计数不是非负整数、reading 缺字段、source 非法、
  format/version 不符等）——带路径拒绝；
- 校验失败时不会发生半恢复。此前能正常恢复的合法快照字段与类型保持
  不变，恢复行为不回归。

### 6.2 恢复语义（有专门测试逐条断言）

1. **计数原样接得上**：重启前的投递 `delivered`、重复 `duplicate`、
   超窗/回退、**缓冲溢出丢弃 `overflow_dropped`（drop_newest 拒绝与
   drop_oldest 静默腾退两条路径都包含）**、识别阶段的
   `unidentified`/`ambiguous` 等计数原样进快照、恢复后逐一相等
   （旧实现里网关层的丢弃数从不累加，落盘恒为 0，已修复）；
2. **水位/已见序号接得上**：恢复后重放重启前已交给下游、曾被溢出丢掉、
   或仍在缓存排队的读数，窗口内一律 `DUPLICATE_SEQ`，水位**只增不减、
   不倒退**，不重复投递、不重复计数；
3. **缓存接着补发**：排队读数、顺序与字段完整恢复；重启前已投递的键
   随快照恢复作为补发幂等防线，新进程 `drain` 只会收到尚未投递的读数，
   恰好一次；恢复后下游仍不可用时，容量/溢出策略按**恢复进程配置**
   继续生效，新发生的溢出在快照计数基线之上累加；
4. **策略来自进程配置**：恢复进程不给某设备的覆盖，它就回退全局缺省，
   快照无法夹带或改变任何策略。

## 7. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 端到端剧本：自动识别（含"认得出但帧坏"）、指定协议、按设备策略、
# 六类畸形帧、中断→按设备缓冲/溢出→落快照（无策略字段）→显式配置恢复→补发
uv run python main.py

# 全部 76 个测试；加 -s 可查看每条序号/水位/判定依据日志
uv run pytest -s

# 仅看本轮三类自动判别结论与快照新契约
uv run pytest tests/test_identification.py -s   # 协议自行判别（12）
uv run pytest tests/test_device_policy.py -s    # 按设备差异化策略（5）
uv run pytest tests/test_snapshot.py -s         # 快照运行状态白名单/严格恢复（10）

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
```

快速手工验证三类自动判别结论与快照拒绝行为：

```python
import json
from edge_gateway.devices import SimulatedDevice, RawMessage
from edge_gateway.gateway import Gateway
from edge_gateway.protocols.identification import AUTO_PROTOCOL
from edge_gateway.snapshot import save_snapshot, load_snapshot, restore_gateway
from edge_gateway.state import SnapshotError

gw = Gateway()
reg = SimulatedDevice("line-A", protocol="register")
# 1) 认得出但帧坏：原因是 register 自己的校验失败，并带 device/seq 依据
r = gw.ingest(reg.emit_auto(1, corrupt="bad_crc"))
assert r.reason.value == "CHECKSUM_MISMATCH" and r.protocol == "register"
assert r.evidence == {"device_id": "line-A", "seq": 1}
# 2) 真正认不出
r = gw.ingest(RawMessage(AUTO_PROTOCOL, b"\x00\x01mystery", "?"))
assert r.reason.value == "UNKNOWN_PROTOCOL"
# 3) 歧义参见 tests/test_identification.py 的双认领桩用例

# 快照：策略字段不落盘；塞未知字段或凭据键恢复时明确报错
save_snapshot(gw, "/tmp/gw.json")
state = load_snapshot("/tmp/gw.json")
assert set(state) == {"format", "version", "taken_at_ms",
                      "stats", "sequencer", "spool"}
for key in ("password", "token", "config"):
    bad = json.loads(json.dumps(state)); bad[key] = "x"
    try:
        restore_gateway(bad)
    except SnapshotError as exc:
        print("rejected as expected:", exc)
```

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
| `tests/test_identification.py` | 两类字节自动归一、老路不回退、**认得出但帧坏（register/structured 各自可区分原因+依据）**、真正认不出、单方/双方认领歧义、三类结论计数分离（12） |
| `tests/test_device_policy.py` | 缺省回退/稳定解析、窗口与容量按设备独立、重连一致、每设备 drop_oldest（5） |
| `tests/test_snapshot.py` | 运行状态白名单无策略无凭据、水位/序号集/缓存/**全部计数（含两种溢出丢弃）**恢复、重放不回退不重复、文件原子往返、**未知字段/凭据键/类型错误明确拒绝**、策略只来自进程配置、自动识别帧重放（10） |
