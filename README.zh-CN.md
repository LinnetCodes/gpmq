# GPMQ

简体中文 | [English](README.md)

**General Purpose Message Queue** — 基于 Redis Streams 的轻量级 Python 分布式消息队列，面向个人项目设计。

## 特性

- **多种消息模式** — 同步、异步、发后即忘（fire-and-forget）、批量滑动窗口并发、消息转发
- **定向发布** — 通过 `target_subscribers` 把消息投递给指定的一个或多个订阅者；已注册但不在列表中的订阅者会丢弃它
- **可配置的持久运行方法** — 通过 `@run_forever_method` 自定义订阅者的主循环
- **Redis Streams 后端** — 基于 Consumer Group 的可靠消息投递
- **多进程 Worker** — 每个订阅者可配置独立 Worker 数量
- **YAML 配置** — 通过 gpconfig 实现类型安全的公共/专属配置合并
- **审计日志** — 可选的 SQLite 消息与结果审计记录
- **Worker 可见性** — 通过 `get_consumer_group_workers()` 查询消费组中所有 Worker，包括空闲/过期 Worker
- **CLI 工具** — 启动 Worker、查询审计记录、查看系统状态

## 环境要求

- Python >= 3.10
- Redis >= 5.0

## 安装

```bash
pip install gpmq
```

开发环境安装：

```bash
pip install -e ".[dev]"
```

## 快速开始

### 1. 编写消息处理器

```python
# myapp/handlers.py
from typing import Any, Optional
from gpmq import message_handler, Message

@message_handler(["load_user_profile"])
def on_load_user_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    user_name = msg.payload["user_name"]
    # ... 你的业务逻辑 ...
    return {"profile": f"profile of {user_name}"}

@message_handler(["send_email"])
def on_send_email(msg: Message) -> Any:
    # 不使用 context 参数也可以
    return {"status": "sent"}
```

**处理器返回值** — 框架对处理器返回值的处理规则：

| 返回值 | 结果状态 | 说明 |
|---|---|---|
| `None`（或无 `return`） | `SUCCESS` | 处理器正常完成，未返回值 |
| `ProcessStatus.FAILURE` | `FAILURE` | 显式声明规约中定义的失败状态 |
| `ProcessResult(...)` | 按指定值 | 原样使用，自动填充 `message_id` 和 `processing_time` |
| 其他任意值 | `SUCCESS` | 值存入 `ProcessResult.data` |
| 抛出异常 | `EXCEPTION` | 自动捕获，记录错误消息和完整堆栈 |

### 2. 编写配置

```
configs/
├── global_env.yaml            # gpconfig 必需
└── gpmq/
    ├── common.yaml            # 公共 Redis/Stream 设置
    ├── publisher/
    │   └── main.yaml          # Publisher 配置
    └── subscribers/
        ├── data_loader.yaml   # Subscriber 配置
        └── notifier.yaml
```

**common.yaml** — 公共配置：

```yaml
cfg_class_name: "GPMQConfig"

redis_host: "localhost"
redis_port: 6379
redis_password: ""
stream_name: "gpmq:messages"

enable_registry: true
heartbeat_interval: 10
```

**publisher/main.yaml：**

```yaml
cfg_class_name: "PublisherConfig"
publisher_name: "my_app"
publish_timeout: 30.0
```

**subscribers/data_loader.yaml：**

```yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
workers: 4

handlers:
  - message_types: ["load_user_profile"]
    handler: "myapp.handlers.on_load_user_profile"
  - message_types: ["send_email"]
    handler: "myapp.handlers.on_send_email"
```

### 3. 启动 Worker

使用 CLI 命令：

```bash
# 启动订阅者 Worker
gpmq worker gpmq.subscribers.data_loader --config ./configs

# 或使用独立命令
gpmq-worker gpmq.subscribers.data_loader --config ./configs
```

或以代码方式启动：

```python
# run_worker.py
from gpconfig import GPConfigManager
from gpmq import set_config_manager, get_subscriber

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)

sub = get_subscriber("gpmq.subscribers.data_loader")
sub.run_forever()  # 阻塞运行，直到 Ctrl+C
```

### 4. 发布消息

```python
from gpconfig import GPConfigManager
from gpmq import set_config_manager, get_client

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)

with get_client("gpmq.publisher.main") as client:
    # 同步发布 — 阻塞等待所有订阅者返回结果
    results = client.publish("load_user_profile", {"user_name": "alice"})
    for sub_name, result in results.items():
        print(f"{sub_name}: {result.status} -> {result.data}")

    # 异步发布 — 获取 ResultHandler，稍后等待
    handler = client.publish_async("load_user_profile", {"user_name": "bob"})
    # ... 做其他事情 ...
    results = handler.wait(timeout=10.0)

    # 发后即忘 — 不收集结果
    client.send("send_email", {"to": "user@example.com", "body": "Hello!"})
```

## 消息模式

### 同步发布

```python
results = client.publish("load_user_profile", {"user_name": "alice"})
# results: dict[str, ProcessResult] — 以订阅者名称为键
```

### 异步发布

```python
handler = client.publish_async("load_user_profile", {"user_name": "bob"})
# ... 做其他事情 ...
results = handler.wait(timeout=30.0)
```

等待多个异步结果：

```python
handlers = [
    client.publish_async("load_user_profile", {"user_name": f"user_{i}"})
    for i in range(10)
]
all_results = client.wait_all(handlers)
```

### 发后即忘

```python
client.send("send_email", {"to": "user@example.com"})
```

### 批量异步 + 进度条

```python
from gpmq import GPMQProgress

class MyProgress(GPMQProgress):
    def start(self):
        print(f"0/{self.max_value}")
    def update(self, value):
        super().update(value)
        print(f"{self.current_value}/{self.max_value}")
    def finish(self):
        print("done")

with client.async_batch(MyProgress(), timeout=30) as batch:
    for i in range(100):
        batch.send_message("load_user_profile", {"user_name": f"user_{i}"})

results = batch.get_all_result()
```

### 消息转发（Subscriber 内再次发送消息）

在订阅者配置中启用 `embedded_publisher`：

```yaml
# subscribers/data_loader.yaml
embedded_publisher: "gpmq.publisher.embedded"
```

然后在处理器中使用内嵌 Publisher 转发消息：

```python
@message_handler(["msg_ping"])
def on_ping(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    if context and "publisher" in context:
        context["publisher"].send("msg_pong", {"reply_to": msg.id}, correlation_id=msg.id)
    return {"status": "pong_sent"}
```

### 定向发布

`publish()`、`publish_async()`、`send()` 以及批处理 `send_message()` 都接受可选的 `target_subscribers` 参数，用于把消息限定投递给一个或多个**指定**订阅者——即使其他订阅者也注册了同一消息类型。接受单个名称或列表；`None`（默认）广播给所有匹配的订阅者。

```python
# 只有 "data_loader" 会处理这条消息；"notifier"（如果也注册了）会丢弃它
results = client.publish("load_user_profile", {"user_name": "alice"},
                         target_subscribers="data_loader")

# 指定多个目标（publish_async / send / 批处理同样支持）
client.send("order_created", {...}, target_subscribers=["billing", "warehouse"])
```

发布者会在**发送前**校验目标：每个被指定的目标必须已注册，且在其 handlers 中声明了该消息类型，否则抛出 `InvalidTargetSubscriberError`，消息不会被写入 Stream。不在目标列表中的订阅者会静默 ACK 并丢弃。详见 [API 文档](docs/api/gpmq_api.zh.md#定向发布)。

## 配置参考

GPMQ 采用两层配置机制：**公共配置**（`gpmq/common.yaml`）提供共享默认值，每个 **Publisher** 或 **Subscriber** 的专属配置只需指定自己的字段。专属配置中未显式设置的字段将自动使用公共配置中的值。

```
最终配置 = common.yaml 默认值  ⊕  专属配置覆盖（仅覆盖显式设置的字段）
```

### GPMQConfig — common.yaml

所有组件的共享设置，作为 Publisher 和 Subscriber 配置的基础层。

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `redis_host` | str | `"localhost"` | Redis 服务器地址 |
| `redis_port` | int | `6379` | Redis 端口（1–65535） |
| `redis_db` | int | `0` | Redis 数据库编号 |
| `redis_password` | str | `""` | Redis 密码 |
| `redis_max_retries` | int | `3` | 最大连接重试次数 |
| `redis_retry_delay` | float | `1.0` | 重试基础延迟（秒） |
| `stream_name` | str | `"gpmq:messages"` | Redis Stream 名称 |
| `enable_registry` | bool | `True` | 启用订阅者注册中心 |
| `heartbeat_interval` | int | `10` | 心跳间隔（秒） |
| `registry_key_prefix` | str | `"gpmq:"` | Redis 键前缀（自动补 `:`） |
| `subscriber_timeout` | float | `60.0` | 单个订阅者处理超时（秒） |
| `audit_db_path` | str | `"gpmq_audit.db"` | SQLite 审计数据库路径 |
| `enable_audit` | bool | `True` | 启用审计日志 |
| `log_category` | str | `"gpmq"` | gpclog 日志分类 |

### PublisherConfig — publisher/*.yaml

继承 `GPMQConfig`。配置文件需包含 `cfg_class_name: "PublisherConfig"`。

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `publisher_name` | str | *（必填）* | 唯一的 Publisher 名称 |
| `publish_timeout` | float \| null | `null` | 同步发布总超时（null = 无限制） |
| `async_batch_queue_size` | int | `32` | `async_batch()` 的默认队列大小 |

此外继承 `GPMQConfig` 的全部字段作为默认值。

**示例** — 仅覆盖 `publisher_name` 和 `log_category` 的最小 Publisher 配置：

```yaml
cfg_class_name: "PublisherConfig"
publisher_name: "my_app"
log_category: "my_app_publisher"
# redis_host、redis_port、stream_name 等均使用 common.yaml 的值
```

### SubscriberConfig — subscribers/*.yaml

继承 `GPMQConfig`。配置文件需包含 `cfg_class_name: "SubscriberConfig"`。

| 键 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | str | *（必填）* | 订阅者名称（同时作为 Consumer Group 名称） |
| `handlers` | list[HandlerConfig] | *（必填）* | 消息处理器配置列表 |
| `workers` | int | `1` | Worker 进程数量 |
| `embedded_publisher` | str \| null | `null` | 内嵌 Publisher 配置路径（用于消息转发） |
| `worker_context_processor` | str \| null | `null` | Worker 上下文初始化函数的导入路径 |
| `worker_context` | dict \| null | `null` | 注入到每个 Worker 上下文的自定义数据字典 |
| `run_forever_method` | str \| null | `null` | `@run_forever_method` 装饰器修饰的函数的导入路径 |
| `run_forever_method_paras` | dict \| null | `null` | 传递给 `run_forever_method` 函数 `paras` 参数的自定义参数字典 |

此外继承 `GPMQConfig` 的全部字段作为默认值。

**HandlerConfig**（`handlers` 列表中的项）：

| 键 | 类型 | 说明 |
|---|---|---|
| `message_types` | list[str] | 该处理器处理的消息类型 |
| `handler` | str | 处理器函数的导入路径 |

**示例** — 覆盖 Redis 设置并启用消息转发的 Subscriber：

```yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
workers: 4
redis_db: 1              # 覆盖 common.yaml 的默认值
enable_audit: true
audit_db_path: "./audit/data_loader.db"

handlers:
  - message_types: ["load_user_profile"]
    handler: "myapp.handlers.on_load_user_profile"

embedded_publisher: "gpmq.publisher.embedded"
```

## Worker 上下文处理器

`worker_context_processor` 允许你在每个 Worker 进程启动后、开始处理消息之前，执行自定义的初始化逻辑。它接收当前的 Worker 上下文字典，其返回值会合并回上下文中，使新增的项可供所有消息处理器使用。

### 默认上下文内容

Worker 启动时，上下文中已包含以下内容：

| 键 | 类型 | 说明 |
|---|---|---|
| `project_name` | str | 项目名称（来自 GPConfigManager） |
| `subscriber_name` | str | 订阅者名称 |
| `subscribe_message_types` | list[str] | 该订阅者处理的消息类型列表 |
| `worker_id` | str | Worker 标识（如 `"data_loader-server1-worker-0"`） |
| `worker_sn` | int | Worker 序号（0, 1, 2, ...） |
| `config_manager` | GPConfigManager | 配置管理器实例（如已初始化） |
| `logger` | GPCLogger | 该 Worker 的日志实例 |
| `publisher` | GPMQClient | 内嵌 Publisher 客户端（如设置了 `embedded_publisher`） |

### 配置

在订阅者配置中设置处理器函数的导入路径：

```yaml
# subscribers/data_loader.yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
worker_context_processor: "myapp.workers.init_context"
```

### 编写上下文处理器

```python
# myapp/workers.py
from typing import Any
from gpmq import worker_context_processor

@worker_context_processor
def init_context(context: dict[str, Any]) -> dict[str, Any]:
    """初始化该 Worker 中所有处理器共享的资源。"""
    logger = context["logger"]
    cfg_mgr = context.get("config_manager")

    logger.info(f"正在初始化 Worker {context['worker_id']}")

    # 返回一个字典 — 其中的项将被合并到 Worker 上下文中
    return {
        "db_connection": create_db_pool(cfg_mgr),
        "api_client": APIClient(cfg_mgr),
    }
```

函数要求：
- 接受且仅接受一个名为 `context` 的参数
- 返回 `dict[str, Any]` — 返回的项将合并到 Worker 上下文中
- 必须使用 `@worker_context_processor` 装饰器

### 在消息处理器中使用上下文

接受两个参数的处理器会将 Worker 上下文作为第二个参数传入：

```python
from gpmq import message_handler, Message
from typing import Any, Optional

@message_handler(["load_user_profile"])
def on_load_user_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    logger = context["logger"]
    db = context["db_connection"]          # 来自 worker_context_processor

    logger.info(f"正在加载用户 {msg.payload['user_name']} 的资料")
    user = db.query("SELECT * FROM users WHERE name = ?", msg.payload["user_name"])
    return {"profile": user}
```

### 执行顺序

1. Worker 进程启动
2. 构建内置上下文（`worker_id`、`logger`、`config_manager`、`publisher` 等）
3. **执行你定义的 `worker_context_processor`** — 接收内置上下文，可添加新项
4. 消息处理器开始消费消息 — 接收完整合并后的上下文

## 持久运行方法（Run Forever Method）

`run_forever_method` 允许你用自定义的阻塞函数替代订阅者默认的 `time.sleep(1)` 循环。适用于主进程需要主动驱动工作（如轮询外部数据源）而非被动等待消息的场景。

### 配置

```yaml
# subscribers/data_loader.yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
run_forever_method: "myapp.workers.my_main_loop"
run_forever_method_paras:
  poll_interval: 5
  batch_size: 100
```

### 编写持久运行方法

```python
# myapp/workers.py
from typing import Any, Optional
from gpmq.subscriber import run_forever_method, Subscriber

@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    """自定义的主循环，在订阅者进程中运行。"""
    interval = paras.get("poll_interval", 5) if paras else 5
    while True:
        # 在此编写自定义逻辑
        time.sleep(interval)
```

函数要求：
- 接受且仅接受两个参数：`Subscriber` 实例和 `paras`（`dict` 或 `None`）
- 返回值注解必须为 `None`（如有注解）
- 必须使用 `@run_forever_method` 装饰器
- 必须是无限阻塞调用（在 `KeyboardInterrupt` 时退出循环）

### 不配置自定义方法

如果未配置 `run_forever_method`，`Subscriber.run_forever()` 会使用简单的 `time.sleep(1)` 循环作为默认的阻塞行为。

## CLI 参考

> 详细的 CLI 使用说明请参阅 [docs/cli/gpmq_cli.zh-CN.md](docs/cli/gpmq_cli.zh-CN.md)。

```
gpmq [OPTIONS] COMMAND ...

Options:
  --config PATH   包含 global_env.yaml 的配置文件夹

Commands:
  worker SUBSCRIBER_NAME   启动订阅者 Worker
  status                   查看活跃的订阅者
  clear --stream NAME      清空 Redis Stream
  audit query              查询审计记录
  audit cleanup            删除旧的审计记录
  audit stats              查看审计统计
```

独立 Worker 命令：

```
gpmq-worker SUBSCRIBER_NAME [--config PATH]
```

## API 参考

> 详细的 API 文档请参阅 [docs/api/gpmq_api.zh-CN.md](docs/api/gpmq_api.zh-CN.md)。

| 类 / 函数 | 说明 |
|---|---|
| `GPMQClient` | 消息发布主入口 |
| `Subscriber` | 订阅者生命周期与 Worker 管理 |
| `message_handler(types)` | 注册消息处理器的装饰器 |
| `worker_context_processor` | Worker 上下文初始化装饰器 |
| `run_forever_method` | 自定义订阅者主循环装饰器 |
| `get_client(cfg_path)` | 从配置创建 `GPMQClient` |
| `get_subscriber(cfg_path)` | 从配置创建 `Subscriber` |
| `set_config_manager(manager)` | 设置全局配置管理器 |
| `ResultHandler` | 异步结果收集句柄 |
| `GPMQAsyncBatchHandler` | 批量异步消息处理器 |
| `GPMQProgress` | 进度指示器基类 |
| `Message` | 消息数据模型 |
| `ProcessResult` | 处理结果（含状态和数据） |
| `get_consumer_group_workers(name)` | 查询消费组中所有 Worker |
| `WorkerInfo` | Worker 状态数据模型 |
| `ConsumerGroupWorkers` | 消费组 Worker 列表模型 |

## 示例（Examples）

[`examples/`](examples/) 目录包含可运行的 demo 及其配置模板：

| 路径 | 说明 |
|---|---|
| `examples/client/demo_client.py` | 交互式菜单，包含多种发布者 demo（同步 / 异步 / 批量 / 超时 / 压测 / 消息转发） |
| `examples/worker/demo_data_loader_worker.py` | `data_loader` 订阅者 Worker |
| `examples/worker/demo_notifier_worker.py` | `notifier` 订阅者 Worker |
| `examples/cfgs_linux/`、`examples/cfgs_windows/` | 按操作系统区分的配置与日志模板 |
| `examples/init_configs.py` | 把 `examples/configs` 链接到对应的 `cfgs_<os>/` 目录 |
| `examples/unlink_configs.py` | 安全地移除 `examples/configs` 链接 |

> demo 用到了 `progressbar`，请先安装 dev 额外依赖：`pip install -e ".[dev]"`。

### 运行 demo 之前

demo 从 `examples/configs` 读取配置，而该目录**并未**纳入版本管理。每台机器上都需要先建立到当前操作系统配置模板的链接：

```bash
python examples/init_configs.py     # 创建 examples/configs -> cfgs_linux/ 或 cfgs_windows/
```

该脚本可重入，重复运行结果一致，会自动识别操作系统（Windows 下若没有符号链接权限会自动回退为目录联接 junction）。

> **不要提交 `examples/configs` 及其中的任何内容。** 它只是一个本地链接（已写入 `.gitignore`），把它指向错误目标——或误提交成一个真实目录——会影响他人使用 demo。**最佳实践**：不需要运行 demo 时，运行以下命令安全地删除该链接，避免误提交：
>
> ```bash
> python examples/unlink_configs.py
> ```

配置模板中包含与操作系统相关的**文件路径**（如 `audit_db_path`、日志路径）。你可以按自己的环境修改这些配置项，但请将此类改动保留在**本地**——只在本地分支提交，不要推送到远程仓库，以保持模板的可移植性。

## 许可证

MIT
