# IoT 边缘网关（进程内模拟）

一套**本地可运行**的工业边缘网关能力演示，使用进程内的模拟设备端与模拟下游汇聚端，
在同一进程内复现多协议数据的接入归一、面向设备时序的幂等/乱序处理，以及下游
不可用时的有界缓冲与恢复补发，使异常链路下行为确定、可解释、可复现。

## 目录结构

```text.
├── main.py                       # 演示入口 + HTTP（统计/故障/恢复/快照）
├── edge_gateway/
│   ├── config.py                 # 配置项及环境变量（窗口/容量/溢出/按设备覆盖）
│   ├── policies.py               # 按设备差异化策略（缺省回退全局默认）
│   ├── snapshot.py               # 运行状态快照：落盘/校验/恢复（白名单）
│   ├── models.py                 # 统一读数模型与拒绝原因枚举
│   ├── gateway.py                # 编排：识别/解码→判定→直投/缓冲→恢复
│   ├── buffer.py                 # 有界缓冲（按设备容量/溢出）与两种策略
│   ├── dedup.py                  # 每设备幂等/乱序窗口/单调水位
│   ├── downstream.py             # 进程内模拟下游汇聚端（可注入故障）
│   ├── devices.py                # 进程内模拟设备端（含畸形帧/auto 报文）
│   ├── demo.py                   # 逐条打印序号/水位/判定依据的剧本
│   └── protocols/
│       ├── base.py               # 解码器协议接口（sniff + decode）
│       ├── detect.py             # 协议自行判别（0/1/多候选结论）
│       ├── register.py           # 寄存器风格二进制帧（CRC16/MODBUS）
│       └── structured.py         # 结构化报文（JSON）
└── tests/                        # 79 个单测/集成测（含并发、识别、策略、恢复）
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

### 1.3 协议自行判别（auto 接入）

设备侧只给一段原始字节、不声明编码时，构造 `RawMessage(protocol="auto", ...)`
（或用 `devices.auto_message(payload)`）。网关先跑 `ProtocolDetector` 做
**结构签名匹配**，再正式解析：

| 解码器 | sniff 签名（保守、互不相交） |
|---|---|
| register | 二进制且首 2 字节为 `5A A5` 帧头魔数 |
| structured | 可解为 UTF-8 文本，首个非空白字符为 `{` |

判定结论确定、可区分，**绝不瞎猜一种硬塞，也不静默丢弃**：

| 候选命中数 | 结论 | 阶段 |
|---|---|---|
| 恰好 1 | 按该协议正式 `decode`（解析仍可能失败，返回对应解码原因） | `detect` 通过→`decode` |
| 0 | **`UNRECOGNIZED_PROTOCOL`**（detail 含长度与十六进制预览） | `detect` |
| 多于 1 | **`AMBIGUOUS_PROTOCOL`**（detail 列出全部候选协议名） | `detect` |

签名命中只代表"像"，后续 CRC/字段等校验照常执行；例如以 `5A A5` 开头但
CRC 错的帧，结论是 `detect` 通过、`decode` 阶段 `CHECKSUM_MISMATCH`。
默认的"已知协议、直接按指定协议解析"老路径（`protocol="register"` /
`"structured"`）行为完全不变，显式传未知协议仍是 `MALFORMED`。

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

## 3. 缓冲与溢出策略（容量与策略可按设备差异化）

下游不可用时，读数进入 `BoundedSpool`。队列是**全局 FIFO**（补发顺序确定），
但容量上限与满队策略**按设备独立计算**：每台设备有自己的容量上限，设备之间
不互相挤占；缺省设备回退全局默认值（见第 4 节）。

- **`drop_newest`（默认）**：该设备满则拒绝新读数，返回 `BUFFER_OVERFLOW`
  （detail 中带该设备实际生效的 capacity/policy）。
  语义：保证已缓存数据按序、不丢旧地补发。
- **`drop_oldest`**：该设备满则腾退**该设备**在全局队列中最旧的读数再放入。
  语义：优先实时性。

两种策略的丢弃都累加 `dropped_overflow`，可通过 `spool.snapshot_stats()`
或 `GET /stats` 查询（另含 enqueued/delivered/duplicate_delivery/
downstream_failures/flushes/size）。

**恢复补发**：`drain` / `POST /recover` 严格 FIFO 逐条投递；投递抛异常立即
中断，失败项保留在队首，下次恢复重试，保证全局顺序。已成功投递的
`(device_id, seq)` 记录在案，再次出现跳过并计入 `duplicate_delivery`；
下游 `MockDownstream` 自身也按 `(device_id, seq)` 做最终幂等。
全部缓冲操作同一把锁串行，并发接入/补发计数不错乱。

## 4. 配置项与按设备差异化策略

### 4.1 全局默认

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `GW_WINDOW_SIZE` | 32 | 每设备乱序/迟到可接受窗口 W（≥1） |
| `GW_SPOOL_CAPACITY` | 100 | 边界缓冲硬上限（条数/每设备，≥1） |
| `GW_OVERFLOW_POLICY` | drop_newest | `drop_newest` / `drop_oldest` |
| `GW_DEVICE_POLICIES` | 无 | 按设备覆盖（JSON，见 4.2） |

也可在代码中用 `GatewayConfig(...)` 或向 `Gateway(window_size=...,
spool_capacity=..., overflow_policy=...)` 直接传参（关键字参数优先）。

### 4.2 按设备覆盖（缺省回退）

每台设备可单独设定乱序窗口、缓冲容量、溢出策略；未设定（或字段缺省）的项
回退全局默认。解析以 `device_id` 为唯一依据，确定性、无状态：同一台设备
无论第几条、哪一次接入（含 auto 与显式协议交错）拿到的策略都相同，
设备之间互不影响。

代码方式：

```python
from edge_gateway.config import GatewayConfig
from edge_gateway.gateway import Gateway
from edge_gateway.policies import DevicePolicy

cfg = GatewayConfig(
    window_size=32, spool_capacity=100,
    device_policies={
        "line-a": DevicePolicy(window_size=4, spool_capacity=8,
                               overflow_policy="drop_oldest"),
        "line-b": DevicePolicy(window_size=2),   # 其余两项回退默认
    },
)
gw = Gateway(config=cfg)
gw.policy_resolver.resolve("line-a")   # -> window=4 cap=8 policy=drop_oldest
gw.policy_resolver.resolve("any-x")    # -> 全部走全局默认
```

环境变量方式（JSON 对象，键为 device_id，字段均可省略）：

```bash
GW_DEVICE_POLICIES='{"line-a":{"window_size":4,"spool_capacity":8,
  "overflow_policy":"drop_oldest"},"line-b":{"window_size":2}}' \
  uv run python main.py
```

非法值（窗口/容量 <1、未知策略名、未知字段、JSON 非法）在构造/读取配置时
即抛 `ValueError`；设备窗口在每次判定的日志 detail 中可见（`window=N`）。

## 5. 重启后接着跑：状态快照与恢复语义

网关可把当前**运行状态**落成一份本地 JSON 快照，并在全新进程/全新网关实例中
恢复（`edge_gateway.snapshot`）：

```python
from edge_gateway.snapshot import save_snapshot, load_snapshot_file
from edge_gateway.gateway import Gateway

save_snapshot(gw, "/data/gw-snapshot.json")          # 原子写（tmp + rename）
snap = load_snapshot_file("/data/gw-snapshot.json")  # 读入并严格校验
gw2 = Gateway.restore_from_snapshot(snap, config=cfg)  # 全新进程语义恢复
```

快照内容（严格白名单，递归校验；**只含真实运行状态，不写任何配置与凭据**）：

- `sequencer`：每设备 `watermark`、`last_seen_seq`、窗口内已见序号 `seen`
- `spool.queue`：还没补发的读数（保持 FIFO 顺序）
- `spool.delivered`：已成功投递的 `(device_id, seq)` 幂等键
- `spool.stats` / `stats`：缓冲与网关的各类计数及水位表

恢复语义：

1. 恢复后每设备**水位、已见序号、待补发缓存、各类计数**与重启前一致；
2. 把重启前**已交给下游**或**已在缓存排队**的读数再喂一遍：被时序层
   `DUPLICATE_SEQ` 拦截，**水位不倒退、不重复投递、不重复计数**；
   恢复后触发补发，只会投递仍在队列中的读数；
3. 水位之后的新数据照常前向推进，补发保序；
4. **按设备策略属于配置，不在快照内**——恢复时由新进程的配置重新提供，
   因此恢复调用应传入与重启前等价的 `GatewayConfig`；
5. 版本不符、JSON 损坏、出现白名单外字段或 `password/token/secret/
   credential/api_key/authorization` 等凭据风格键名时，抛 `SnapshotError`
   拒绝恢复。HTTP 对应 `POST /snapshot`、`POST /recover` 前后配合的
   `POST /restore`（体 `{"path": "..."}`）。

## 6. 拒绝原因一览

`TRUNCATED_FRAME`、`BAD_MAGIC`、`CHECKSUM_MISMATCH`、`FIELD_MISSING`、
`BAD_VALUE`、`MALFORMED`、`UNRECOGNIZED_PROTOCOL`、`AMBIGUOUS_PROTOCOL`、
`DUPLICATE_SEQ`、`OUT_OF_WINDOW`、`SEQ_ROLLBACK`、`BUFFER_OVERFLOW`
——取值互不相同，日志与统计中可区分。

## 7. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 端到端剧本：协议自行判别、按设备策略、畸形帧、中断→缓冲→溢出→
# 快照落盘→全新网关恢复→重放不重复→恢复补发
uv run python main.py

# 全部 79 个测试；加 -s 可查看每条序号/水位/窗口/识别依据日志
uv run pytest -s

# 仅看本轮新增能力
uv run pytest tests/test_detect.py tests/test_device_policies.py tests/test_snapshot.py -s

# 自定义配置运行（含按设备覆盖）
GW_WINDOW_SIZE=8 GW_SPOOL_CAPACITY=4 GW_OVERFLOW_POLICY=drop_oldest \
GW_DEVICE_POLICIES='{"line-a":{"window_size":2,"overflow_policy":"drop_oldest"}}' \
  uv run python main.py

# HTTP：统计查询 / 故障注入 / 恢复 / 快照 / 恢复快照
uv run uvicorn main:app
curl localhost:8000/stats
curl -XPOST localhost:8000/downstream/available -d '{"available":false}' \
  -H 'content-type: application/json'
curl -XPOST localhost:8000/snapshot -d '{"path":"/tmp/gw-snapshot.json"}' \
  -H 'content-type: application/json'
curl -XPOST localhost:8000/restore -d '{"path":"/tmp/gw-snapshot.json"}' \
  -H 'content-type: application/json'
curl -XPOST localhost:8000/recover
```

## 8. 测试覆盖结论（可复现）

| 测试文件 | 覆盖点 |
|---|---|
| `tests/test_protocols.py` | CRC 标准向量、两种协议往返归一、各类畸形帧原因矩阵（23） |
| `tests/test_detect.py` | auto 双协议归一、认不出/歧义可区分拒绝、签名命中后解析失败分阶段、老路径不变、内置签名不相交（6） |
| `tests/test_sequencer.py` | 单调水位、乱序补缺、重复只计一次、回退 vs 超窗、剪枝、并发幂等（8） |
| `tests/test_spool.py` | 容量上限、两种溢出策略、失败保留队首、FIFO 补发、并发一致（6） |
| `tests/test_device_policies.py` | 缺省回退、解析稳定、按设备窗口/容量/溢出互不影响、auto 路径一致、JSON/环境变量解析与非法拒绝（15） |
| `tests/test_snapshot.py` | 水位/已见/队列/计数恢复、重放不回退不重复、新数据续跑、原子写、白名单与凭据键拒绝、损坏文件、按设备策略恢复（8） |
| `tests/test_pipeline.py` | 双协议、乱序重复、9 类畸形帧、中断→恢复保序、重放不变、并发、抖动恢复（8） |
| `tests/test_config.py` | 默认值、环境覆盖、非法配置（5） |
