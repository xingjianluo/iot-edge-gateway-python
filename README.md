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
│       ├── identification.py     # 协议自行判别（命中/归属坏帧/认不出/歧义）
│       ├── register.py           # 寄存器风格二进制帧（CRC16/MODBUS）
│       └── structured.py         # 结构化报文（JSON）
└── tests/                        # 97 个单测/集成测（识别/设备策略/快照/并发）
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
全部已注册解码器。判别首先要回答的是**"这串字节属不属于我们支持的某种
编码"**，而不是只有"解码成功/失败"两档：

| 情形 | 结论 | 拒绝原因 |
|---|---|---|
| 恰好 1 种协议解码成功 | 接受，`Reading.source` 标明命中协议；`IngestResult.protocol` 回填协议名 | — |
| 恰好 1 种协议**认得出但帧坏了** | 给该协议自己的、可区分的坏帧结论（见下），并带出能读出的设备/序号依据 | `CHECKSUM_MISMATCH` / `TRUNCATED_FRAME` / `FIELD_MISSING` / `BAD_VALUE` / `MALFORMED` |
| 没有任何协议认得出 | 明确拒绝，detail 逐个列出各解码器的失败原因 | **`UNKNOWN_PROTOCOL`** |
| 2 种及以上同时解码成功，或同时"认得出（但都坏了）" | 明确拒绝（歧义，绝不二选一硬塞），detail 列出全部候选 | **`AMBIGUOUS_PROTOCOL`** |

"认得出"（attributable）的标准，解决了"同一段坏字节、声明协议时能定位、
自动判别时却只回认不出"的对不上问题：

- **register**：帧首 magic `0xA55A` 一旦对得上，就足以确认它是寄存器报文；
  此后版本非法、长度不符/被截断、CRC 对不上、必填字段为空、取值非法，
  全部按 **register 坏帧**上报，并把帧里能安全读出的 `device_id`/`seq`
  放进 `IngestResult.evidence` 一并带出。短于 2 字节、或 magic 对不上的，
  不能归到 register 头上。
- **structured**：字节能解析成 JSON **对象**，就足以确认它是结构化报文；
  此后缺字段、空值、类型/取值非法，全部按 **structured 坏帧**上报，
  同样带 `device`/`seq` 依据。非 JSON（如被截断到无法解析）、或顶层是
  数字/数组等非对象，则不能归到 structured 头上。

要点：

- **坏帧 ≠ 认不出**：CRC 错、被截断、缺字段、取值非法这些"我们支持的编码
  发坏了"的现场，自动路径给的结论与设备事先声明协议时**完全一致**
  （有参数化测试逐条比对两条路径），排查时能直接定位；`evidence` 里的
  设备/序号来自对坏帧的尽力、边界安全读取（device_id 非 ASCII 时用替换符
  展示，读不出的字段不编造）。
- **认不出**：例如魔数错误且又不是 JSON 的字节，register 视角 `BAD_MAGIC`、
  structured 视角 `MALFORMED`，两边都认不出 -> `UNKNOWN_PROTOCOL`
  （"我们不认识这是什么"），与坏帧分开计数、分开日志。
- **歧义**：同一段字节同时像两种编码（两个解码器同时成功，或同时声称
  认得出）-> `AMBIGUOUS_PROTOCOL`，绝不硬挑一种塞进去。
- **指定协议的老路保持不变**：`protocol="register"` 时严格按 register
  解析，把 JSON 字节误标成 register 只会得到 register 的帧级拒绝，
  不会偷偷回退到 JSON 解析。
- 非字节输入（如 str）按 `MALFORMED` 拒绝。任何情况下都不静默丢弃；
  坏帧只计入 `decode_rejected`，认不出另计 `unidentified`、歧义另计
  `ambiguous`，三者在统计与 `/stats` 中分开。

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

> 这些策略设置是**进程配置**，不会写进状态快照；重启恢复时以新进程自己的
> 配置（环境变量/构造参数）为准，按设备覆盖、未覆盖回退全局缺省（见第 6 节）。

## 5. 拒绝原因一览

- 帧级（指定协议路径，或自动路径下"唯一认得出但帧坏"）：
  `TRUNCATED_FRAME`（长度不符/被截断）、`BAD_MAGIC`（帧头魔数错误）、
  `CHECKSUM_MISMATCH`（校验失败）、`FIELD_MISSING`（必填字段缺失）、
  `BAD_VALUE`（类型/取值非法）、`MALFORMED`（结构性无法解析/版本非法）；
- 自动判别专属：`UNKNOWN_PROTOCOL`（没有任何已支持编码认得出）、
  `AMBIGUOUS_PROTOCOL`（同时像两种编码，歧义）；
- 时序/缓冲阶段：`DUPLICATE_SEQ`、`OUT_OF_WINDOW`、`SEQ_ROLLBACK`、
  `BUFFER_OVERFLOW`。

以上取值互不相同，日志与统计中可区分；坏帧、认不出、歧义三类结论
在自动路径下分开计数（`decode_rejected` / `unidentified` / `ambiguous`）。

## 6. 重启后接着跑：纯运行状态快照（v2）

`edge_gateway/snapshot.py` 把网关**纯运行状态**落成一份本地 JSON 快照，
在全新进程中由调用进程按自己的配置重建网关后灌入状态：

```python
from edge_gateway.config import GatewayConfig
from edge_gateway.snapshot import save_snapshot, load_snapshot, restore_gateway

save_snapshot(gw, "/var/lib/gw/state.json")          # 临时文件 + os.replace 原子写
state = load_snapshot("/var/lib/gw/state.json")      # 全新进程

# 策略来自恢复进程自己的配置（含按设备覆盖），不从快照读取
cfg = GatewayConfig(
    window_size=8, spool_capacity=10,
    device_policies={"line-A/temp": DevicePolicy(window_size=3)},
)
gw = restore_gateway(state, config=cfg)
gw.on_downstream_recovered()                         # 继续补发缓存

# 也可直接传 Gateway 构造参数（window_size/spool_capacity/overflow_policy/
# device_policies），或先按目标配置建好网关再 state_to_gateway(gw, state)
```

快照内容（v2，**只有运行状态 + 外壳**）：

- `format` / `version`（=`edge-gateway-state` / `2`）/ `taken_at_ms`；
- `sequencer`：每设备 `watermark`、`last_seen_seq`、窗口内已见序号集 `seen`；
- `spool`：尚未补发的排队读数（保持全局 FIFO 顺序与全部字段）、
  已成功投递键、缓冲计数（含 `dropped_overflow`）；
- `stats`：各类计数与水位表（`accepted/rejected/decode_rejected/duplicate/
  out_of_window/rollback/overflow_dropped/delivered/unidentified/ambiguous`）。

**策略设置不进快照**：窗口大小、缓冲容量、溢出策略以及按设备覆盖，一律
由进程自己的配置决定（环境变量 / `GatewayConfig` / 构造参数）；按设备单独
覆盖、没有单独设定的设备回退全局缺省值（见第 4 节）。因此同一份快照拿到
不同策略的进程里恢复是允许的，恢复后的判定尺度以该进程配置为准。

**恢复时的严格校验（任何层级违反都抛 `SnapshotError`，绝不静默接受）**：

1. **白名单字段**：每个层级只允许上表列出的键。多出任何未知字段（例如
   旧版快照里的 `config` 策略段、随手塞的 `extra`）都明确拒绝并说明位置；
2. **凭据类内容**：任意层级出现 `password/passwd/secret/token/api_key/
   credential/private_key/access_key` 等凭据样式键名（子串匹配，不区分
   大小写），一律拒绝并报出位置——快照本来就不该出现凭据；
3. **结构与类型**：format/version 必须匹配，计数器必须是非负整数，
   排队读数字段必须完整、`value` 必须是有限数、`source` 必须是
   `register`/`structured` 等；校验顺序上凭据优先于"未知字段"报告。
   直接调用 `Gateway.load_state(export_state() 的纯运行状态)` 时执行同一套
   校验（只是不要求 format/version 外壳）。

恢复语义（有专门测试逐条断言）：

1. **计数原样接续**：重启前发生的**丢弃（两种溢出策略都算）、投递、重复、
   超窗、回退、认不出、歧义**等计数都进快照，恢复后逐一相等；
2. **水位/已见序号接得上**：重放重启前已投递或仍在缓存排队的读数，窗口内
   一律 `DUPLICATE_SEQ`，水位**只增不减、不倒退**；
3. **不重复投递/计数**：重启前已投递的键随快照恢复，作为补发幂等防线；
   恢复后 `drain` 只会投递缓存里尚未发出的读数，恰好一次；被
   `drop_oldest` 腾退的旧读数不会复活；
4. **缓存接着补发**：排队读数、顺序与字段完整恢复；恢复后下游仍不可用时，
   容量/溢出策略按**恢复进程**对各设备的生效配置继续判定。

## 7. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 端到端剧本：自动判别三结论（坏帧/认不出/歧义）、指定协议老路、
# 按设备策略、六类畸形帧、中断→按设备缓冲/溢出→落快照→新进程恢复→补发
uv run python main.py

# 全部 97 个测试；加 -s 可查看每条序号/水位/判定依据日志
uv run pytest -s

# 仅看本轮三类新增/修复行为
uv run pytest tests/test_identification.py -s   # 自动判别：坏帧/认不出/歧义三类分别断言
uv run pytest tests/test_device_policy.py -s    # 按设备差异化策略
uv run pytest tests/test_snapshot.py -s         # 快照纯运行状态/严格拒绝/计数接续

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
| `tests/test_identification.py` | 自动判别三结论分别断言：register/structured 各类坏帧（含 evidence 依据）与声明路径结论一致、真正认不出、两编码歧义、三类分开计数与日志、老路不回退（16） |
| `tests/test_device_policy.py` | 缺省回退/稳定解析、窗口与容量按设备独立、重连一致、每设备 drop_oldest（5） |
| `tests/test_snapshot.py` | 快照只含运行状态（无策略/凭据）、严格白名单与凭据键拒绝（含嵌套/旧 config 段/坏类型/坏版本）、水位/序号集/缓存恢复、重放不回退不重复、溢出丢弃（两种策略）与各类计数重启前后一致、策略随恢复进程配置与缺省回退、文件原子往返（26） |
