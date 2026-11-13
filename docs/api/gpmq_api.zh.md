# GPMQ API 参考

配置与生命周期管理函数，以及消息发布、订阅和结果收集的核心类。

---

## 目录

- [配置函数](#配置函数)
  - [set_config_manager](#set_config_manager)
  - [get_client](#get_client)
  - [get_subscriber](#get_subscriber)
- [GPMQClient](#gpmqclient)
  - [定向发布](#定向发布)
  - [InvalidTargetSubscriberError](#invalidtargetsubscribererror)
  - [粘性路由（target_worker）](#粘性路由target_worker)
  - [InvalidTargetWorkerError](#invalidtargetworkererror)
- [Subscriber](#subscriber)
- [ResultHandler](#resulthandler)
- [GPMQAsyncBatchHandler](#gpmqasyncbatchhandler)
- [Stream 保留策略](#stream-保留策略)
- [GPMQProgress](#gpmqprogress)
- [装饰器](#装饰器)
  - [message_handler](#message_handler)
  - [worker_context_processor](#worker_context_processor)
  - [worker_context_finalizer](#worker_context_finalizer)
  - [run_forever_method](#run_forever_method)
- [数据模型](#数据模型)
  - [Message](#message)
  - [ProcessResult / ProcessStatus](#processresult--processstatus)
  - [WorkerInfo / ConsumerGroupWorkers](#workerinfo--consumergroupworkers)

---

## 配置函数

### `set_config_manager`

```python
set_config_manager(cfg_mgr: GPConfigManager) -> None
```

设置全局 `GPConfigManager` 实例。必须在调用 `get_client()` 或 `get_subscriber()` 之前调用。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `cfg_mgr` | `GPConfigManager` | 已初始化的配置管理器实例 |

**异常**

| 异常 | 触发条件 |
|------|---------|
| `TypeError` | `cfg_mgr` 不是 `GPConfigManager` 实例 |

**示例**

```python
from gpconfig import GPConfigManager
from gpmq import set_config_manager

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)
```

---

### `get_client`

```python
get_client(
    cfg_path: Optional[str] = None,
    publisher_name: Optional[str] = None,
    timeout: Optional[float] = None,
) -> GPMQClient
```

通过合并公共 + 专属配置创建 `GPMQClient`。需要事先调用 `set_config_manager()`。

**参数**

| 名称 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `cfg_path` | `str?` | `None` | PublisherConfig 配置文件的点分路径，如 `"gpmq.publisher.main"` |
| `publisher_name` | `str?` | `None` | 发布者名称。无 `cfg_path` 时用于构造配置，有 `cfg_path` 时用于验证。 |
| `timeout` | `float?` | `None` | 覆盖最终配置中的 `publish_timeout`。需要 `cfg_path` 或 `publisher_name`。 |

**三种使用模式**

| 模式 | 调用方式 | publisher_name 来源 |
|------|---------|---------------------|
| 从配置文件 | `get_client("gpmq.publisher.main")` | YAML 文件 |
| 按名称 | `get_client(publisher_name="my_app")` | 参数 |
| 匿名 | `get_client()` | `"anonymous"` |

**返回值** — 已构造（但未连接）的 `GPMQClient` 实例。

**异常**

| 异常 | 触发条件 |
|------|---------|
| `RuntimeError` | 未调用 `set_config_manager()` |
| `ValueError` | `cfg_path` 和 `publisher_name` 名称冲突，或匿名客户端传入了 `timeout` |

---

### `get_subscriber`

```python
get_subscriber(cfg_path: str) -> Subscriber
```

通过合并公共 + 专属配置创建 `Subscriber`。需要事先调用 `set_config_manager()`。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `cfg_path` | `str` | 点分配置路径，如 `"gpmq.subscribers.data_loader"` |

**返回值** — 已构造（但未启动）的 `Subscriber` 实例。

**异常**

| 异常 | 触发条件 |
|------|---------|
| `RuntimeError` | 未调用 `set_config_manager()` |

---

## GPMQClient

消息发布的主入口。支持同步、异步、发后即忘和批量模式。

### 构造函数

```python
GPMQClient(config: GPMQConfig)
```

通常通过 `get_client()` 创建，而非直接构造。支持上下文管理器（`with` 语句），自动调用 `connect()` / `close()`。

### 方法

#### `publish`

```python
publish(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    timeout: Optional[float] = None,
    target_subscribers: Optional[Union[str, list[str]]] = None,
    target_worker: Optional[int] = None,
) -> dict[str, ProcessResult]
```

发布消息并**阻塞等待**所有订阅者返回结果（或超时）。内部等价于 `publish_async()` + `handler.wait()`。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `message_type` | `str` | 消息类型标识 |
| `payload` | `dict` | 消息载荷 |
| `correlation_id` | `str?` | 可选，用于追踪消息链的关联 ID |
| `timeout` | `float?` | 最大等待秒数。`None` = 使用配置默认值。 |
| `target_subscribers` | `str \| list[str] \| None` | 可选，允许处理的订阅者名称白名单。只有列表中的订阅者会处理该消息，已注册但不在列表中的订阅者会丢弃它。`None`（默认）广播给所有匹配的订阅者。详见[定向发布](#定向发布)。 |
| `target_worker` | `int?` | 可选，粘性路由的 0-based worker 槽位，作用于唯一的目标订阅者；要求显式指定恰一个 `target_subscriber`。详见[粘性路由](#粘性路由target_worker)。 |

**返回值** — `dict[str, ProcessResult]`，以订阅者名称为键。

**注意事项**
- 如果没有活跃的订阅者，立即返回空字典。
- 未在 `timeout` 内响应的订阅者状态为 `ProcessStatus.TIMEOUT`。
- 设置 `target_subscribers` 时，返回字典中只包含被指定的订阅者。

---

#### `publish_async`

```python
publish_async(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    target_subscribers: Optional[Union[str, list[str]]] = None,
    target_worker: Optional[int] = None,
) -> ResultHandler
```

发布消息并立即返回 `ResultHandler`。稍后调用 `handler.wait()` 收集结果。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `message_type` | `str` | 消息类型标识 |
| `payload` | `dict` | 消息载荷 |
| `correlation_id` | `str?` | 可选，用于追踪消息链的关联 ID |
| `target_subscribers` | `str \| list[str] \| None` | 可选，允许处理的订阅者名称白名单。详见[定向发布](#定向发布)。 |
| `target_worker` | `int?` | 可选，粘性路由的 0-based worker 槽位。详见[粘性路由](#粘性路由target_worker)。 |

**返回值** — `ResultHandler`，用于延迟收集结果。

---

#### `send`

```python
send(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    target_subscribers: Optional[Union[str, list[str]]] = None,
    target_worker: Optional[int] = None,
) -> Optional[str]
```

发后即忘（fire-and-forget）发布。将消息写入 Redis Stream，但**不订阅**结果通道。不创建 `ResultHandler`，避免 Pub/Sub 资源开销。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `message_type` | `str` | 消息类型标识 |
| `payload` | `dict` | 消息载荷 |
| `correlation_id` | `str?` | 可选，用于追踪消息链的关联 ID |
| `target_subscribers` | `str \| list[str] \| None` | 可选，允许处理的订阅者名称白名单。详见[定向发布](#定向发布)。 |
| `target_worker` | `int?` | 可选，粘性路由的 0-based worker 槽位。详见[粘性路由](#粘性路由target_worker)。 |

**返回值** — Redis 生成的消息 ID（可用于日志记录）；若设置了 `publish_require_subscriber: true` 且没有已注册的订阅者处理该消息类型，消息会被跳过，返回 `None`。

**适用场景** — 通知类消息，不需要知道订阅者是否/何时处理完成（如发送邮件、记录事件日志等）。

---

#### 定向发布

`publish()`、`publish_async()`、`send()` 以及[批处理 `send_message()`](#send_message) 都接受可选的 `target_subscribers` 参数，用于把消息限定投递给一个或多个**指定**订阅者——即使其他订阅者也注册了同一消息类型。

```python
# 即便 "notifier" 也处理 "load_user_profile"，这里只有 "data_loader" 会处理
results = client.publish("load_user_profile", {"user_name": "alice"},
                         target_subscribers="data_loader")

# 指定多个目标
client.send("order_created", {...}, target_subscribers=["billing", "warehouse"])
```

**工作原理**
- `target_subscribers` 接受单个名称（`str`）或列表（`list[str]`）；会去重并保持顺序。
- `None`（默认）广播给所有注册了该类型的订阅者——完全向后兼容。
- 发布者**在发送前校验**：每个被指定的目标必须已注册，且在其 `handlers` 中声明了该消息类型。未知或无能力的目标会抛出 `InvalidTargetSubscriberError`（见下文），消息**不会**被写入 Stream。
- 不在目标列表中的订阅者会静默地 ACK 并丢弃消息（不发布结果）。
- `ResultHandler` 只等待被指定的订阅者；已注册且有能力但当前不活跃的目标会以 `ProcessStatus.TIMEOUT` 体现。

**校验说明** — 只校验"能力"，不校验"活跃"。已注册该类型但当前没有活跃 worker 的目标会通过校验，随后在下游超时，这与既有 `publish()` 对不活跃订阅者的行为一致。

---

#### `InvalidTargetSubscriberError`

```python
class InvalidTargetSubscriberError(GPMQError):
    message_type: str
    failures: list[tuple[str, str]]   # (订阅者名称, 原因)
```

当某个被指定的目标无法处理该消息类型时，由 `publish` / `publish_async` / `send` / 批处理 `send_message`（在 `wait_all` 期间）抛出。`原因` 取值为 `"not registered"`（未知的订阅者名称）或 `"does not handle message type"`（已注册但该类型不在其 handlers 中）。所有无效目标会被聚合后一次性报出。

---

#### 粘性路由（target_worker）

当任务处理需要载入昂贵的可复用上下文时（`Task(n, m)` 需要 `Context(m)`），组内负载均衡会让同一上下文在不同 worker 上反复载入。粘性路由把均衡决策权移交给**发布方**：四个发布接口都接受可选的 `target_worker`——**一个订阅组内的 0-based worker 槽位**——消息会被确定性地投递到该槽位，使每个 worker 只载入一份上下文并长期复用。不传 `target_worker`（默认）时保持组内负载均衡，行为与之前完全一致。

```python
# 指定 target_worker 时，必须显式指定恰一个 target_subscriber
results = client.publish("task", payload, target_subscribers="ctx-holder", target_worker=3)
client.send("task", payload, target_subscribers="ctx-holder", target_worker=3)
handler = client.publish_async("task", payload, target_subscribers="ctx-holder", target_worker=3)

# 批处理：参数相同；形状类错误在入队时即失败，而不是 wait_all 中途
with client.async_batch(queue_size=16) as batch:
    for m in range(8):
        batch.send_message("task", {"m": m},
                           target_subscribers="ctx-holder", target_worker=m)
    results = batch.wait_all()
```

**规则**

- **槽位从 0 开始编号。** 槽位即 worker ID 的 `-worker-{n}` 后缀（`target_worker=0` 是第一个 worker）。槽位必须落在目标订阅者注册的 worker 数量范围内（`0 <= target_worker < workers`）；注册信息缺失同样拒绝。范围规则与下面的互斥规则都抛出 `InvalidTargetWorkerError`（见[下文](#invalidtargetworkererror)），消息**不会**被写入 Stream。
- **显式单一接收者（互斥）。** `target_worker` 非 `None` 时，`target_subscribers` 必须显式提供，且归一化后**恰为一个**名称（单个 `str` 视为一个）。不存在"该类型恰只有一个活跃匹配订阅者"的隐式解析——不同订阅者的 `workers` 配置可能不同，且另一订阅者中途声明同类型会静默破坏路由。
- **单机部署约束。** 槽位号解析为 consumer 名的前提是该订阅者的全部 worker 共享同一 hostname（单订阅者实例部署）。粘性路由仅在**单机部署**下有定义；同一订阅者名跨机部署会使槽位号产生歧义。
- **重启继承以 hostname 不变为前提。** Worker 以确定性 consumer 名重启，自动继承名下 PEL 中排队的条目——同机重启或崩溃恢复因此自动继承排队的路由消息。hostname 变更或跨机迁移视为全新部署：旧 consumer 名下的残留条目会被孤儿回收放弃，调用方收到 `ABANDONED` 回执（属预期行为，非故障）。

**语义**

- **槽内保序。** 同一槽位内的路由消息按流 ID 顺序处理——降级期间（见下一条）除外，此时错投的 worker 本地处理，不保证跨 worker 的顺序。
- **死目标降级为本地处理。** 向目标槽位的转交以该槽位心跳新鲜为前提（阈值 `heartbeat_interval * 3`）；目标死亡（或心跳过期）时，收到消息的 worker 自行处理——可用性优先，代价是上下文重复载入。目标存活时，条目在一个读取阻塞周期内（`worker_read_block_ms`，默认 1000ms）被领取。
- **重启对路由条目是 at-least-once——handler 必须幂等。** 处理中途被中断的路由条目在其 worker 重启后会被重新处理。（非路由消息维持 0.11.1 的 at-most-once 裁定：崩溃残留被跳过，闲置超过 `pel_abandon_idle_seconds` 后被回收放弃，报 `ABANDONED`。）
- **可能出现 ABANDONED 回执。** 孤儿回收对路由 PEL 条目设有豁免：只要条目**归属**的 worker 心跳新鲜就不放弃（归属者会处理或重放转交）；归属 worker 死亡后，条目在闲置超过 `pel_abandon_idle_seconds` 时被放弃，仍在等待的 RPC 调用方收到 `ABANDONED` 回执。
- **版本偏斜安全。** 旧 worker 忽略该字段（退化为纯负载均衡）；旧发布方不会设置该字段。注意 0.12.0 CHANGELOG 中的部署顺序约束：发布方启用 `target_worker` 前，订阅者须全部完成升级。

**超时预算与批量建议**

- 单个任务时长必须小于 `heartbeat_interval * 3`（默认 `heartbeat_interval=15` 时为 45 秒）——超过即被判死，转交降级，且其名下排队条目失去回收豁免。确有更长任务时调大 `heartbeat_interval`（如 20 → 阈值 60 秒）。
- 路由消息的 RPC `wait(timeout)` 必须覆盖目标槽位的排队深度：`timeout >= 槽内排队深度 × 任务时长 + 自身处理 + 领取余量（<= worker_read_block_ms / 1000）`。各槽位深度可通过 `gpmq status` 输出（`slot_pending`）与 `WorkerStatus.pending_pel` 查看。
- 大批量场景建议改用 `send`（发后即忘）+ 审计库核对，而非逐条收集 RPC 回执——槽位队列一深，逐消息超时必爆。
- 使用 `GPMQAsyncBatchHandler` 时，`queue_size` 建议设为 **worker 数 × 1~2**：它就是压住槽位排队深度的背压旋钮。避免裸用 `wait(None)`——某个槽位死亡会挂死等待；改用有限超时并配合 `allow_timeout_retry` 再入。
- 混合负载仍均衡：不带 `target_worker` 的消息继续在所有 worker 间均匀散布。延迟敏感的非路由流量与深度排队的路由流量混跑时，建议分组部署。

---

#### `InvalidTargetWorkerError`

```python
class InvalidTargetWorkerError(GPMQError):
    subscriber_name: str
    target_worker: int
    reason: str
```

当 `target_worker` 用法错误时由 `publish` / `publish_async` / `send` 抛出——范围规则（槽位超出目标订阅者的 worker 数量，或注册信息缺失）或互斥规则（`target_worker` 未搭配恰一个显式 `target_subscriber`）。批处理 `send_message` 在**入队时**即对形状规则（非负 `int`；互斥）抛出该异常，使用法错误在首条消息在途之前就失败；范围/注册类校验仍在发送时进行（需要 Redis）。两类违规共用一个异常类型——同属"target_worker 用法错误"一族，以 `reason` 区分。

---

#### `wait_all`

```python
wait_all(
    handlers: list[ResultHandler],
    timeout: Optional[float] = None,
    allow_timeout_retry: Optional[bool] = None,
) -> list[dict[str, ProcessResult]]
```

等待多个 `ResultHandler` 的结果。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `handlers` | `list[ResultHandler]` | 要等待的结果处理器列表 |
| `timeout` | `float?` | 每个 handler 的最大等待秒数 |
| `allow_timeout_retry` | `bool?` | 部分超时后是否允许再次调用此方法 |

**返回值** — `list[dict[str, ProcessResult]]`，每个元素对应一个 handler 的结果字典，保持输入顺序。

---

#### `async_batch`

```python
async_batch(
    progress: Optional[GPMQProgress] = None,
    timeout: Optional[float] = None,
    queue_size: Optional[int] = None,
) -> GPMQAsyncBatchHandler
```

创建一个带有**滑动窗口并发**控制的批量处理器。详见 [GPMQAsyncBatchHandler](#gpmqasyncbatchhandler)。

**参数**

| 名称 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `progress` | `GPMQProgress?` | `None` | 进度指示器 |
| `timeout` | `float?` | `None` | 所有结果的总超时。`None` = 无限等待。 |
| `queue_size` | `int?` | `None` | 最大并发在途消息数。`None` = 使用配置中的 `async_batch_queue_size`（默认 `32`）。 |

---

#### `get_audit_store`

```python
get_audit_store() -> Optional[AuditStore]
```

返回审计存储实例。若审计已禁用（`enable_audit: false`），返回 `None`。

---

#### `get_consumer_group_workers`

```python
get_consumer_group_workers(subscriber_name: str) -> ConsumerGroupWorkers
```

查询消费组中所有已注册的 Worker，包括空闲 Worker。心跳时间超过 `heartbeat_interval * 3` 的 Worker 会被标记为 `"stale"`。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `subscriber_name` | `str` | 订阅者 / 消费组名称 |

**返回值** — `ConsumerGroupWorkers`，包含 Worker 列表和计数。

**说明**
- 未正常关闭的 Worker（如崩溃）仍会保留在注册表中，状态显示为 `"stale"`。
- 心跳超时阈值由 `heartbeat_interval * 3` 决定（派生自共享的 `heartbeat_interval`）。

**示例**

```python
with get_client("gpmq.publisher.main") as client:
    group = client.get_consumer_group_workers("data_loader")
    print(f"Workers: {group.worker_count}")
    for w in group.workers:
        print(f"  {w.worker_id} on {w.hostname} (pid {w.pid}) — {w.status}")
```

---

#### `connect` / `close`

```python
connect() -> None
close() -> None
```

连接 Redis 和审计存储，或断开连接并清理资源。两者都是**可重入**的——多次调用是安全的。使用上下文管理器时会自动调用。

---

## Subscriber

管理订阅者的生命周期：连接 Redis、注册订阅者、启动心跳、管理工作进程。

### 构造函数

```python
Subscriber(config: SubscriberConfig)
```

通常通过 `get_subscriber()` 创建。

### 方法

#### `start`

```python
start() -> None
```

启动订阅者。执行流程：连接 Redis → 加载处理器 → 校验处理器类型 → 注册到 Redis → 创建消费组 → 启动心跳守护线程 → 启动 Worker。

**注意事项**
- 此方法在初始化阶段短暂阻塞后返回。Worker 在子进程中运行。
- 必须在订阅者处理消息之前调用。
- 会校验配置的 `message_types` 是否为装饰器声明类型的子集；不匹配时抛出 `HandlerTypeMismatchError`。

---

#### `stop`

```python
stop() -> None
```

优雅停止订阅者：停止 Worker → 停止心跳线程 → 若为最后一个已注册的订阅者则先执行一次最终 Stream 裁剪（详见 [Stream 保留策略](#stream-保留策略)）→ 从 Redis 注销 → 断开连接。

**注意事项**
- 等待最多 `subscriber_timeout`（取自配置）让每个 Worker 优雅退出；若仍存活，则发送 SIGTERM 并再等待一段宽限时间。

---

#### `apply_worker_context`

```python
apply_worker_context(**kwargs) -> Subscriber
```

向每个 Worker 的上下文字典注入自定义数据。必须在 `start()` **之前**调用。可多次调用——每次调用合并新的键。

**返回值** — `self`，支持链式调用。

**示例**

```python
sub = get_subscriber("gpmq.subscribers.data_loader")
sub.apply_worker_context(db_url="postgres://...", max_retries=3)
sub.start()
```

---

#### `get_status`

```python
get_status() -> dict
```

返回订阅者状态信息，包括名称、运行状态、Worker 数量、订阅的消息类型列表以及各 Worker 的状态。

---

#### `run_forever`

```python
run_forever() -> None
```

启动订阅者并阻塞调用线程，直到被中断（`KeyboardInterrupt` / `SystemExit`）。中断时自动调用 `stop()`。

如果在 `SubscriberConfig` 中配置了 `run_forever_method`，则会调用该自定义函数替代默认的 `time.sleep(1)` 循环。详见 [run_forever_method](#run_forever_method)。

**注意事项**
- 等价于调用 `start()` 后接一个阻塞循环。
- 这是推荐的编程方式启动订阅者的方法。

**示例**

```python
sub = get_subscriber("gpmq.subscribers.data_loader")
sub.run_forever()  # 阻塞运行，直到 Ctrl+C
```

---

## ResultHandler

管理单条已发布消息的异步结果收集。订阅 Redis Pub/Sub 通道，收集各订阅者返回的 `ProcessResult`。

### 属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `message_id` | `str` | 正在追踪的消息 ID |

### 方法

#### `wait`

```python
wait(
    timeout: Optional[float] = None,
    allow_timeout_retry: Optional[bool] = None,
) -> dict[str, ProcessResult]
```

阻塞等待所有预期的订阅者返回结果，或直到超时。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `timeout` | `float?` | 最大等待秒数。`None` = 使用默认轮询。 |
| `allow_timeout_retry` | `bool?` | 若为 `True`，允许在部分超时后再次调用 `wait()`，不保存超时结果。 |

**返回值** — `dict[str, ProcessResult]`，以订阅者名称为键。未响应的订阅者状态为 `ProcessStatus.TIMEOUT`。

**注意事项**
- `wait()` 完成后，Pub/Sub 订阅会自动清理。
- 若 `allow_timeout_retry` 为 `False`（默认），再次调用 `wait()` 返回相同的缓存结果。

---

#### `wait_any`

```python
wait_any(timeout: Optional[float] = None) -> dict[str, ProcessResult]
```

等待**至少一个**订阅者返回结果。

**返回值** — `dict[str, ProcessResult]`，至少包含一个结果。超时无结果时返回空字典。

---

#### `is_complete`

```python
is_complete() -> bool
```

检查是否已收集到所有预期结果。非阻塞。

---

#### `get_completed_count`

```python
get_completed_count() -> int
```

已收到的结果数量。

---

#### `get_results`

```python
get_results() -> dict[str, ProcessResult]
```

获取当前已收集的所有结果，不等待。可能不完整。

---

## GPMQAsyncBatchHandler

带有滑动窗口并发控制的批量异步消息处理器。由 `GPMQClient.async_batch()` 创建。

### 工作原理

1. 通过 `send_message()` 将消息加入队列。
2. 调用 `wait_all()`（或退出 `with` 上下文）时，以滑动窗口方式发送消息：
   - 首先并发发送最多 `queue_size` 条消息。
   - 每完成一条，立即发送队列中的下一条。
3. 结果按发送顺序收集。

### 方法

#### `send_message`

```python
send_message(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    target_subscribers: Optional[Union[str, list[str]]] = None,
    target_worker: Optional[int] = None,
) -> None
```

将消息加入待发送队列。消息在调用 `wait_all()` 之前不会发布。

`target_subscribers` 会原样保存，并在 `wait_all()` 期间转发给 `client.publish_async()`，归一化和校验在那里进行。详见[定向发布](#定向发布)。设置 `target_worker` 时，其**形状**规则（非负 `int`；恰一个显式 `target_subscriber`）在**此处入队时**即校验，使用法错误在首条消息在途之前就失败——范围/注册类校验仍在发送时进行（需要 Redis）。详见[粘性路由](#粘性路由target_worker)。

---

#### `wait_all`

```python
wait_all() -> list[dict[str, ProcessResult]]
```

发送所有队列中的消息（遵循滑动窗口限制）并等待所有结果。

**返回值** — `list[dict[str, ProcessResult]]`，每条消息一个结果字典，按 `send_message()` 的调用顺序排列。

---

#### `get_all_result`

```python
get_all_result() -> list[dict[str, ProcessResult]]
```

在 `wait_all()` 完成后获取所有结果。如果在 `wait_all()` 之前调用，返回空列表。

---

### 上下文管理器

```python
with client.async_batch(progress, timeout=30) as batch:
    for i in range(100):
        batch.send_message("my_type", {"index": i})
# 退出时自动调用 wait_all()
results = batch.get_all_result()
```

---

## Stream 保留策略

默认情况下，每个订阅者的心跳线程会定期用 `XTRIM MINID` 把 Redis Stream 裁剪到安全点：对每个**已注册**消费组，取其最后投递 ID 与 PEL（待处理条目列表）最小 ID 的较小值，再在全体已注册组中取最小。只要一个条目尚未投递给某个已注册订阅者、或仍滞留在其消费组的 PEL 中，该条目就不会被移除，崩溃订阅者的积压消息因此能完整保留到它恢复上线。全新的订阅者（首次创建消费组）因此只会补读一小段尾部——自上次裁剪以来新增的条目，加上安全点及其之后的条目（任一已注册订阅者仍需要的条目，以及安全点处那条条目本身）——而不是整个历史记录。

保留策略由以下 `GPMQConfig` 字段控制（默认写在 `gpmq/common.yaml`，可在各组件的专属配置中覆盖）：

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `stream_trim_interval` | `60.0` | 每个订阅者心跳线程执行裁剪的间隔秒数（裁剪随心跳轮次执行，实际间隔不小于 `heartbeat_interval`）。`0` 仅关闭订阅者侧的裁剪（周期裁剪与停止时收尾）；发布者侧的 `stream_trim_idle_maxlen` 上限独立生效。 |
| `stream_trim_on_last_stop` | `true` | 最后一个已注册的订阅者停止时执行一次最终裁剪。 |
| `stream_trim_idle_maxlen` | `None` | 注册表为空期间，发布者通过 `XADD MAXLEN N` 为 Stream 施加条目数上限。`None` = 不设上限。 |
| `publish_require_subscriber` | `false` | 没有**已注册**订阅者声明该消息类型时，直接跳过发布（不写入 Stream）。已注册但离线的订阅者仍会收到消息，因此崩溃后仍能补上离线期间的消息。 |
| `pel_abandon_idle_seconds` | `60.0` | PEL 条目闲置多少秒后被订阅者心跳扫描放弃（XACK 清出 PEL + `abandoned` 审计记录；绝不重投）。`0` 关闭孤儿回收。应显著大于最长 handler 执行时长；RPC 调用方的 timeout 建议显著小于该值（先到期的调用方仍看到自己的 `TIMEOUT`；回收时仍在等待的调用方会收到 `ABANDONED` 回执——见下文）。需要 Redis >= 6.2（IDLE 过滤）；旧版服务器请设为 `0`。 |

**孤儿 pending 条目** — Worker 在处理中途死亡会将其条目留在消费组的 PEL 中。心跳扫描会将闲置超过 `pel_abandon_idle_seconds` 的条目放弃：XACK 清出 PEL、在审计库记为 `abandoned` 结果，且不再卡住保留策略的安全点。绝不重投——中断即终局失败（至多一次处理）：handler 不会观察到任何新状态。（粘性路由消息是唯一的例外，且仅在重启路径上：worker 重启后会重新处理自己名下排队的路由条目——见上文"粘性路由"一节；该场景 handler 必须幂等。）回收发生时仍在等待的调用方会在回包频道上收到 `ABANDONED` 回执（由扫描尽力发布，频道与格式和 worker 结果完全一致；发布者侧审计同步落档，与订阅者侧一致）。调用方自身 timeout 先到期的仍看到 `TIMEOUT`——`TIMEOUT` 回执只代表调用方放弃等待，不代表消息死亡；权威终态以审计为准。执行缓慢但活着的 handler 即便条目被提前放弃也无害：它随后写入的真结果会在审计库中覆盖 `abandoned` 占位（调用方可能已提前拿到 `ABANDONED` 回执返回）。

---

## GPMQProgress

批量操作进度追踪的抽象基类。继承此类实现自定义进度显示。

### 属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `max_value` | `int` | 总消息数 |
| `current_value` | `int` | 当前进度计数 |

### 需要重写的方法

| 方法 | 说明 |
|------|------|
| `create(max_value)` | 框架调用，传入总消息数 |
| `start()` | 批量处理开始时调用 |
| `update(value)` | 每条消息完成时调用。调用 `super().update(value)` 以更新 `current_value`。 |
| `finish()` | 所有消息处理完成时调用 |

**示例**

```python
from gpmq import GPMQProgress

class MyProgress(GPMQProgress):
    def start(self):
        print(f"开始处理 0/{self.max_value}")
    def update(self, value):
        super().update(value)
        print(f"进度: {self.current_value}/{self.max_value}")
    def finish(self):
        print("全部完成！")
```

---

## 装饰器

### `message_handler`

```python
@message_handler(message_types: list[str])
```

将函数标记为消息处理器，并声明其可处理的消息类型。

**参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `message_types` | `list[str]` | 此处理器**能处理**的消息类型列表（必填，不可为空） |

**处理器函数签名**

框架自动检测参数个数：

```python
# 不使用上下文（1 个参数）
@message_handler(["user_created"])
def on_user_created(msg: Message) -> Any:
    ...

# 使用上下文（2 个参数）
@message_handler(["load_profile"])
def on_load_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    ...
```

**注意事项**
- 处理器返回值的处理规则：
  - `None`（或无 `return`）→ `ProcessStatus.SUCCESS`
  - `ProcessStatus.FAILURE` → `ProcessStatus.FAILURE`（显式声明规约中定义的失败状态）
  - `ProcessResult` → 原样使用
  - 其他任意值 → `ProcessStatus.SUCCESS`，值存入 `data`
- 如需表示异常，直接抛出即可——异常会被捕获为 `ProcessStatus.EXCEPTION`。
- YAML 中配置的 `message_types` 必须是装饰器声明类型的**子集**，否则启动时抛出 `HandlerTypeMismatchError`。

---

### `worker_context_processor`

```python
@worker_context_processor
def init_context(context: dict[str, Any]) -> dict[str, Any]:
    ...
```

将函数标记为 Worker 上下文初始化器。每个 Worker 进程启动后、处理任何消息之前调用一次。

**要求**
- 必须接受且仅接受一个名为 `context` 的参数。
- 必须返回 `dict[str, Any]`——返回的项将合并到 Worker 上下文中。
- 在订阅者 YAML 中通过 `worker_context_processor: "myapp.module.func"` 配置。

**内置上下文项**

| 键 | 类型 | 说明 |
|---|---|---|
| `project_name` | `str` | 来自 GPConfigManager 的项目名 |
| `subscriber_name` | `str` | 订阅者名称 |
| `subscribe_message_types` | `list[str]` | 该订阅者处理的消息类型 |
| `worker_id` | `str` | 如 `"data_loader-server1-worker-0"` |
| `worker_sn` | `int` | Worker 序号（0, 1, ...） |
| `config_manager` | `GPConfigManager` | 配置管理器实例（如已初始化） |
| `logger` | `GPCLogger` | 该 Worker 的日志实例 |
| `publisher` | `GPMQClient` | 内嵌发布者客户端（如已配置） |

---

### `worker_context_finalizer`

```python
@worker_context_finalizer
def close_context(context: dict[str, Any]) -> None:
    context["terminal"].close()
```

将函数标记为 Worker 上下文终结器——`worker_context_processor` 的停止侧对位。每个 Worker 进程优雅停止时调用一次，使经 processor 注入的自建资源（数据库客户端、终端会话、文件句柄……）能在 gpmq 生命周期内收尾，而无需依赖 `atexit`。

**调用时机**
- 覆盖全部优雅退出路径：正常读循环退出、`KeyboardInterrupt`、`SystemExit`、崩溃 teardown。
- 在 gpmq 关闭自建资源**之前**运行，此时 `logger`、内嵌 `publisher` 等上下文项仍可用（可发布最后通知）。
- 硬 terminate（join 超时后的 `Process.terminate()`）时**不会**运行——硬杀下没有任何代码会执行。
- 终结器抛出的异常会被记录，其余清理步骤继续执行。

**要求**
- 必须接受且仅接受一个名为 `context` 的参数。
- 返回值注解（如有）必须为 `None`——终结器是上下文的终点消费者。
- 在订阅者 YAML 中通过 `worker_context_finalizer: "myapp.module.func"` 配置，与 `worker_context_processor` 相互独立（各自均可单独配置）。

**配置**

```yaml
# subscribers/data_loader.yaml
worker_context_processor: "myapp.wiring.build_context"
worker_context_finalizer: "myapp.wiring.close_context"
```

---

### `run_forever_method`

```python
@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    ...
```

装饰器，用于验证并标记函数为 `Subscriber.run_forever()` 的自定义持久运行方法。被装饰的函数会替代默认的 `time.sleep(1)` 循环，允许主进程主动驱动工作。

**要求**
- 必须接受且仅接受两个参数：
  - 第一个：`Subscriber` 实例
  - 第二个：名为 `paras`（不区分大小写），类型为 `Optional[dict[str, Any]]` 或 `dict[str, Any]`
- 返回值注解（如有）必须为 `None`
- 必须是无限阻塞调用
- 在订阅者 YAML 中通过 `run_forever_method: "myapp.module.func"` 配置

**被装饰函数的参数**

| 名称 | 类型 | 说明 |
|------|------|------|
| `subscriber` | `Subscriber` | 调用 `run_forever()` 的订阅者实例 |
| `paras` | `dict[str, Any]?` | 来自 `run_forever_method_paras` 配置的自定义参数，或 `None` |

**配置**

```yaml
# subscribers/data_loader.yaml
run_forever_method: "myapp.workers.my_main_loop"
run_forever_method_paras:
  poll_interval: 5
  batch_size: 100
```

**示例**

```python
from typing import Any, Optional
from gpmq.subscriber import run_forever_method, Subscriber

@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    interval = paras.get("poll_interval", 5) if paras else 5
    while True:
        # 在此编写自定义逻辑
        time.sleep(interval)
```

---

## 数据模型

### Message

```python
class Message(BaseModel):
    id: str
    type: str
    timestamp: datetime
    publisher: str
    payload: dict
    correlation_id: Optional[str] = None
    target_subscribers: Optional[list[str]] = None
    target_worker: Optional[int] = None
```

消息队列中传递的核心数据单元。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | `str` | Redis Streams 生成的消息 ID |
| `type` | `str` | 消息类型标识 |
| `timestamp` | `datetime` | 发布时间 |
| `publisher` | `str` | 发布者名称 |
| `payload` | `dict` | 消息载荷 |
| `correlation_id` | `str?` | 用于追踪消息链的关联 ID |
| `target_subscribers` | `list[str]?` | 可选，允许处理的订阅者名称白名单（由发布方法的 `target_subscribers` 设置；`None` = 广播） |
| `target_worker` | `int?` | 可选，粘性路由的 0-based worker 槽位（由发布方法的 `target_worker` 设置；`None` = 组内负载均衡）。详见[粘性路由](#粘性路由target_worker)。 |

---

### ProcessResult / ProcessStatus

```python
class ProcessStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    EXCEPTION = "exception"
    TIMEOUT = "timeout"
    ABANDONED = "abandoned"

class ProcessResult(BaseModel):
    message_id: str
    status: ProcessStatus
    subscriber_name: str
    processing_time: float
    data: Optional[dict] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    timeout_seconds: Optional[float] = None
```

每个订阅者处理消息后返回的结果。

**ProcessStatus 取值**

| 值 | 含义 |
|----|------|
| `SUCCESS` | 处理器正常完成 |
| `FAILURE` | 处理器显式返回 `ProcessStatus.FAILURE`（规约定义的失败状态，非异常） |
| `EXCEPTION` | 处理过程中抛出了异常 |
| `TIMEOUT` | 处理超时，Worker 被强制终止 |
| `ABANDONED` | 条目被从 PEL 中放弃：闲置超过 `pel_abandon_idle_seconds`，处理被中断（推定 worker 死亡）。回收发生时投递给仍在等待的 RPC 调用方（0.11.3 起）；其余情况仅作审计占位 |

**ProcessResult 字段**

| 字段 | 类型 | 说明 |
|------|------|------|
| `message_id` | `str` | 被处理消息的 ID |
| `status` | `ProcessStatus` | 处理结果状态 |
| `subscriber_name` | `str` | 处理该消息的订阅者名称 |
| `processing_time` | `float` | 处理耗时（秒） |
| `data` | `dict?` | 处理器的返回值 |
| `error_message` | `str?` | 错误描述（EXCEPTION / TIMEOUT 时） |
| `error_traceback` | `str?` | 完整的异常堆栈（EXCEPTION 时） |
| `timeout_seconds` | `float?` | 配置的超时值（TIMEOUT 时） |

---

### WorkerInfo / ConsumerGroupWorkers

```python
class WorkerInfo(BaseModel):
    worker_id: str
    hostname: str
    pid: int
    status: str          # "running" 或 "stale"
    started_at: float
    last_heartbeat: float

class ConsumerGroupWorkers(BaseModel):
    subscriber_name: str
    workers: list[WorkerInfo]
    worker_count: int    # 从 workers 列表自动计算
```

Worker 可见性模型，用于查询消费组成员信息。

**WorkerInfo 字段**

| 字段 | 类型 | 说明 |
|------|------|------|
| `worker_id` | `str` | Worker 唯一标识，如 `"data_loader-server1-worker-0"` |
| `hostname` | `str` | Worker 运行所在的主机名 |
| `pid` | `int` | 进程 ID |
| `status` | `str` | Worker 状态：`"running"` 或 `"stale"` |
| `started_at` | `float` | Worker 启动时的 Unix 时间戳 |
| `last_heartbeat` | `float` | 最近一次心跳的 Unix 时间戳 |

**ConsumerGroupWorkers 字段**

| 字段 | 类型 | 说明 |
|------|------|------|
| `subscriber_name` | `str` | 订阅者 / 消费组名称 |
| `workers` | `list[WorkerInfo]` | Worker 列表 |
| `worker_count` | `int` | Worker 数量（自动计算） |
