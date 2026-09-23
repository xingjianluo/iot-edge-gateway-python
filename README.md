# 工业边缘网关接入与转发（本地可运行）

进程内的模拟设备端 + 边缘网关 + 模拟下游汇聚端，验证多协议数据接入归一、
面向设备时序的幂等/乱序处理、下游中断时的有界缓冲与恢复补发。全部组件在
单进程内运行，不依赖外部 broker、网络或数据库。

## 1. 目录结构

```
gateway/
  model.py       统一读数模型 Reading、拒绝原因 RejectReason、接入结论 AcceptResult
  protocols.py   两种协议解码器：寄存器风格二进制帧 / 结构化 JSON 报文
  device.py      模拟设备端：产生两种合法帧及各类可控畸形帧
  dedup.py       单设备序号状态（水位/窗口/去重）与并发安全的多设备注册表
  buffer.py      有界 FIFO 缓冲、两种确定性溢出策略与统计
  downstream.py  模拟下游汇聚端：可控通断、按序计数、拒绝重复投递
  gateway.py     编排：解码 → 去重/窗口判定 → 提交水位 → 投递/缓冲/补发
demo.py          本地端到端演示脚本
main.py          可选 FastAPI 观测入口（/health、/stats）
tests/           42 个单测/并发/端到端用例
```

## 2. 支持的协议编码

两种输入都被还原为统一读数模型：

```python
Reading(device_id: str, seq: int, metric: str, value: float,
        timestamp: float, source: "register" | "json")
```

### 2.1 寄存器风格二进制帧（小端，定长头 + 变长指标名）

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 2 | 魔数 `0xA5 0x5A` |
| 2 | 1 | 版本，固定 `0x01` |
| 3 | 1 | 载荷长度 N（= 40 + 指标名长度 M） |
| 4 | 16 | 设备 ID，ASCII，NUL 右填充 |
| 20 | 4 | 序号 seq，uint32 LE |
| 24 | 4 | 指标名长度 M，uint32 LE |
| 28 | M | 指标名 ASCII |
| 28+M | 8 | 值，double LE |
| 36+M | 8 | 时间戳，double LE |
| 末字节 | 1 | 校验：前面全部字节逐字节 XOR |

### 2.2 结构化 JSON 报文

```json
{"device_id": "dev-1", "seq": 3, "metric": "temp",
 "value": 21.5, "timestamp": 1727000000.0}
```

### 2.3 畸形输入不静默丢弃，原因可区分

任何无法接受的输入都会在 `AcceptResult.reason` 中给出以下枚举之一，并附
`detail` 文本与当前水位：

| 拒绝原因 | 触发条件 |
| --- | --- |
| `DECODE_ERROR` | 魔数/版本不符、JSON 语法错误、无解码器认领 |
| `LENGTH_MISMATCH` | 帧截断、载荷长度字段与实际帧长不符 |
| `CHECKSUM_FAILED` | 寄存器帧 XOR 校验不一致 |
| `MISSING_FIELD` | 设备 ID/指标名为空，或 JSON 缺必填字段 |
| `BAD_FIELD_TYPE` | 非 ASCII 字段、seq 非非负整数、value/timestamp 非数值、根节点非对象 |
| `DUPLICATE_SEQ` | 序号等于已提交水位，或已在乱序待提交集合中 |
| `SEQ_ROLLBACK` | 序号严格小于已提交水位（序号回退/迟到于水位之前） |
| `OUT_OF_WINDOW` | 序号超过可接受窗口上界的“超前”数据 |

## 3. 去重与窗口语义（按设备独立）

- **水位 watermark**：某设备已连续提交的最大序号，只增不减（单调），初值 -1。
- **首帧基线**：设备首帧序号 N 到达即建立基线 N-1，N 为第一个连续提交点；
  之后再到达的更早序号按 `SEQ_ROLLBACK` 拒绝。
- **乱序窗口 W**：设当前水位为 Wm，下一个期望序号为 Wm+1，可接受区间为
  `[Wm+1, Wm+1+W]`。
  - 区间内的乱序序号被接收并暂存，不重复计数；
  - 当缺失序号补齐，水位连续推进（可能一次推进多号），对应读数此时才按序下发；
  - `seq > Wm+1+W` 判 `OUT_OF_WINDOW`。
- **幂等**：`seq == 水位` 或同一序号重复出现判 `DUPLICATE_SEQ`；
  `seq < 水位` 判 `SEQ_ROLLBACK`。两类输入都不会再次计数。
- **重放不改变聚合**：重复投递的帧全部被拒绝，下游收到的序列与计数保持不变。
- 多设备各自维护独立水位与待提交集合。

> 说明：迟到数据在语义上分为两类——落后于水位的“迟到/回退”
> （`SEQ_ROLLBACK`/`DUPLICATE_SEQ`，明确拒绝），与过度超前的
> “超出窗口”（`OUT_OF_WINDOW`，明确拒绝）。窗口内乱序则正常接收。

## 4. 缓冲与溢出策略

只有“水位连续推进而真正就绪”的读数才会尝试下发，保证每设备严格按序号有序。
下游不可用时读数进入**有界 FIFO 缓冲**（容量为条数硬上限）：

- **`DROP_OLDEST`**：缓冲满时驱逐全局最旧的一条再放入新读数；
  被驱逐项计入 `dropped_oldest`，日志打印被丢弃与新放入的 `(device, seq)`。
- **`DROP_NEWEST`**：缓冲满时直接拒绝新读数（缓冲不变）；
  计入 `dropped_newest`，日志打印被拒绝项。
- 统计 `buffer.stats()` 可查询：`capacity / size / total_in /
  dropped_oldest / dropped_newest / last_dropped / policy`。
- **恢复补发**：下游恢复后调用 `on_downstream_restored()`，按 FIFO 顺序补发；
  补发途中若再次中断，未发出的剩余项按原顺序退回队首，不会丢失或乱序。
- 下游端自身维护已接收 `(device_id, seq)` 集合，任何重复投递（含补发）都被
  拒绝并计入 `duplicate_delivery`，因此**不会重复计数**。

## 5. 配置项

`EdgeGateway(decoders, window, capacity, overflow_policy, downstream)`：

| 参数 | 含义 |
| --- | --- |
| `decoders` | 启用的协议解码器列表，按 `match()` 认领帧（默认两种都启用） |
| `window` | 乱序容忍窗口 W（非负整数），可接受上界 = 水位+1+W |
| `capacity` | 下游中断缓冲的条数硬上限（正整数） |
| `overflow_policy` | `DROP_OLDEST` / `DROP_NEWEST` |
| `downstream` | 注入的下游汇聚端，`set_available(False/True)` 模拟中断/恢复 |
| `log_decisions` | 是否打印每帧判定日志（序号、水位、判定依据） |

## 6. 本地验证方法

需要 Python 3.14 与 [uv](https://docs.astral.sh/uv/)。

```bash
# 安装依赖（含 pytest）
uv sync

# 运行全部测试（日志实时打印 序号 / 水位 / 判定依据）
uv run python -m pytest -v

# 端到端演示：正常乱序/重复/畸形 -> 中断溢出 -> 恢复补发 -> 统计
uv run python demo.py

# 可选 HTTP 观测入口
uv run fastapi dev main.py      # 然后访问 /health、/stats
```

### 测试覆盖

- `tests/test_protocols.py`：两种编码往返 + 校验失败/长度不符/缺字段/
  类型错误/未知字节各自的拒绝原因；
- `tests/test_dedup.py`：首帧基线、重复、回退、窗口上界、补洞推进、水位单调；
- `tests/test_gateway_ordering.py`：双协议归一、乱序按序下发、重复只计一次、
  整体重放不改变聚合、多设备独立水位；
- `tests/test_buffer.py`：容量硬上限、两种溢出策略、FIFO、退回保序；
- `tests/test_outage_recovery.py`：中断缓冲、溢出统计与日志、恢复按序补发、
  重复恢复不重复计数、畸形帧在中断期也不被缓冲；
- `tests/test_concurrency.py`：多线程并发接入/并发重放/中断期并发，
  校验计数、水位、零重复投递；
- `tests/test_end_to_end.py`：乱序 + 重复 + 畸形帧 + 中断溢出 + 恢复补发
  完整链路，全部计数确定可复现。

用例通过 `caplog` 采集并打印形如下面的判定日志，满足“序号、水位与判定依据
可复现”：

```
device=dev-1 seq=3 watermark=1 => ACCEPT | 窗口内接受：seq=3（水位=1，上界=7）
device=dev-1 seq=2 watermark=3 => ACCEPT | 提交后水位=3，本次连续就绪序号=[2, 3]
device=dev-1 seq=2 watermark=3 => REJECT[seq_rollback] | 序号回退：seq=2 < 水位=3
溢出策略=drop_oldest 丢弃最旧读数 device=dev-B seq=1，为放入 device=dev-B seq=5
```
