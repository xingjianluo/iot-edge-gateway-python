# IoT 边缘网关（进程内模拟）

一套**本地可运行**的工业边缘网关能力演示，使用进程内的模拟设备端与模拟下游汇聚端，
在同一进程内复现多协议数据的接入归一、面向设备时序的幂等/乱序处理，以及下游
不可用时的有界缓冲与恢复补发，使异常链路下行为确定、可解释、可复现。

## 目录结构

```text.
├── main.py                       # 演示入口 + HTTP（统计查询/故障注入/恢复）
├── edge_gateway/
│   ├── config.py                 # 配置项及环境变量（窗口/容量/溢出策略）
│   ├── models.py                 # 统一读数模型与拒绝原因枚举
│   ├── gateway.py                # 编排：解码→判定→直投/缓冲→恢复
│   ├── buffer.py                 # 有界缓冲与两种溢出策略
│   ├── dedup.py                  # 每设备幂等/乱序窗口/单调水位
│   ├── downstream.py             # 进程内模拟下游汇聚端（可注入故障）
│   ├── devices.py                # 进程内模拟设备端（含畸形帧注入）
│   ├── demo.py                   # 逐条打印序号/水位/判定依据的剧本
│   └── protocols/
│       ├── base.py               # 解码器协议接口
│       ├── register.py           # 寄存器风格二进制帧（CRC16/MODBUS）
│       └── structured.py         # 结构化报文（JSON）
└── tests/                        # 50 个单测/集成测（含并发与完整链路）
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

## 3. 缓冲与溢出策略

下游不可用时，读数进入 `BoundedSpool`（FIFO，容量为硬上限，绝不无限增长）：

- **`drop_newest`（默认）**：满则拒绝新读数，返回 `BUFFER_OVERFLOW`。
  语义：保证已缓存数据按序、不丢旧地补发。
- **`drop_oldest`**：满则腾退最旧读数再放入新读数。语义：优先实时性。

两种策略的丢弃都累加 `dropped_overflow`，可通过 `spool.snapshot_stats()`
或 `GET /stats` 查询（另含 enqueued/delivered/duplicate_delivery/
downstream_failures/flushes/size）。

**恢复补发**：`drain` / `POST /recover` 严格 FIFO 逐条投递；投递抛异常立即
中断，失败项保留在队首，下次恢复重试，保证全局顺序。已成功投递的
`(device_id, seq)` 记录在案，再次出现跳过并计入 `duplicate_delivery`；
下游 `MockDownstream` 自身也按 `(device_id, seq)` 做最终幂等。
全部缓冲操作同一把锁串行，并发接入/补发计数不错乱。

## 4. 配置项

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `GW_WINDOW_SIZE` | 32 | 每设备乱序/迟到可接受窗口 W（≥1） |
| `GW_SPOOL_CAPACITY` | 100 | 边界缓冲硬上限（条数，≥1） |
| `GW_OVERFLOW_POLICY` | drop_newest | `drop_newest` / `drop_oldest` |

也可在代码中用 `GatewayConfig(...)` 或向 `Gateway(window_size=...,
spool_capacity=..., overflow_policy=...)` 直接传参（关键字参数优先）。

## 5. 拒绝原因一览

`TRUNCATED_FRAME`、`BAD_MAGIC`、`CHECKSUM_MISMATCH`、`FIELD_MISSING`、
`BAD_VALUE`、`MALFORMED`、`DUPLICATE_SEQ`、`OUT_OF_WINDOW`、
`SEQ_ROLLBACK`、`BUFFER_OVERFLOW`——取值互不相同，日志与统计中可区分。

## 6. 本地验证方法

```bash
# 安装依赖（uv）
uv sync

# 端到端剧本：双协议、乱序、重复、六类畸形帧、中断→缓冲→溢出→恢复补发
uv run python main.py

# 全部 50 个测试；加 -s 可查看每条序号/水位/判定依据日志
uv run pytest -s

# 仅看完整链路测试
uv run pytest tests/test_pipeline.py -s

# 自定义配置运行
GW_WINDOW_SIZE=8 GW_SPOOL_CAPACITY=4 GW_OVERFLOW_POLICY=drop_oldest \
  uv run python main.py

# HTTP：统计查询 / 故障注入 / 恢复
uv run uvicorn main:app
curl localhost:8000/stats
curl -XPOST localhost:8000/downstream/available -d '{"available":false}' \
  -H 'content-type: application/json'
curl -XPOST localhost:8000/recover
```

## 7. 测试覆盖结论（可复现）

| 测试文件 | 覆盖点 |
|---|---|
| `tests/test_protocols.py` | CRC 标准向量、两种协议往返归一、各类畸形帧原因矩阵（23） |
| `tests/test_sequencer.py` | 单调水位、乱序补缺、重复只计一次、回退 vs 超窗、剪枝、并发幂等（8） |
| `tests/test_spool.py` | 容量上限、两种溢出策略、失败保留队首、FIFO 补发、并发一致（6） |
| `tests/test_pipeline.py` | 双协议、乱序重复、9 类畸形帧、中断→恢复保序、重放不变、并发、抖动恢复（8） |
| `tests/test_config.py` | 默认值、环境覆盖、非法配置（5） |
