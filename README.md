# GPMQ

[简体中文](README.zh-CN.md) | English

**General Purpose Message Queue** — A lightweight Python distributed message queue built on Redis Streams, designed for personal projects.

## Features

- **Multiple messaging patterns** — sync, async, fire-and-forget, batch with sliding-window concurrency, and message relay
- **Configurable run-forever method** — customize the subscriber's main loop with `@run_forever_method`
- **Redis Streams backend** — reliable message delivery with consumer groups
- **Multi-process workers** — configurable worker count per subscriber
- **YAML configuration** — type-safe config via gpconfig with common/specific merging
- **Audit trail** — optional SQLite-based message and result logging
- **Worker visibility** — query all workers in a consumer group including idle/stale ones via `get_consumer_group_workers()`
- **CLI tools** — start workers, query audit records, check system status

## Requirements

- Python >= 3.10
- Redis >= 5.0

## Installation

```bash
pip install gpmq
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

### 1. Write message handlers

```python
# myapp/handlers.py
from typing import Any, Optional
from gpmq import message_handler, Message

@message_handler(["load_user_profile"])
def on_load_user_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    user_name = msg.payload["user_name"]
    # ... your business logic ...
    return {"profile": f"profile of {user_name}"}

@message_handler(["send_email"])
def on_send_email(msg: Message) -> Any:
    # handler without context is also fine
    return {"status": "sent"}
```

**Handler return values** — the framework interprets handler returns as follows:

| Return value | Result status | Notes |
|---|---|---|
| `None` (or no `return`) | `SUCCESS` | Handler completed without returning a value |
| `ProcessStatus.FAILURE` | `FAILURE` | Explicit spec-defined failure |
| `ProcessResult(...)` | As specified | Returned as-is, with auto-filled `message_id` and `processing_time` |
| Any other value | `SUCCESS` | Value is stored in `ProcessResult.data` |
| Exception raised | `EXCEPTION` | Caught automatically; error message and traceback are captured |

### 2. Write configuration

```
configs/
├── global_env.yaml            # Required by gpconfig
└── gpmq/
    ├── common.yaml            # Shared Redis/stream settings
    ├── publisher/
    │   └── main.yaml          # Publisher config
    └── subscribers/
        ├── data_loader.yaml   # Subscriber config
        └── notifier.yaml
```

**common.yaml** — shared settings:

```yaml
cfg_class_name: "GPMQConfig"

redis_host: "localhost"
redis_port: 6379
redis_password: ""
stream_name: "gpmq:messages"

enable_registry: true
heartbeat_interval: 10
```

**publisher/main.yaml:**

```yaml
cfg_class_name: "PublisherConfig"
publisher_name: "my_app"
publish_timeout: 30.0
```

**subscribers/data_loader.yaml:**

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

### 3. Start workers

Use the CLI:

```bash
# Start a subscriber worker
gpmq worker gpmq.subscribers.data_loader --config ./configs

# Or use the standalone command
gpmq-worker gpmq.subscribers.data_loader --config ./configs
```

Or start programmatically:

```python
# run_worker.py
from gpconfig import GPConfigManager
from gpmq import set_config_manager, get_subscriber

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)

sub = get_subscriber("gpmq.subscribers.data_loader")
sub.run_forever()  # blocks until Ctrl+C
```

### 4. Publish messages

```python
from gpconfig import GPConfigManager
from gpmq import set_config_manager, get_client

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)

with get_client("gpmq.publisher.main") as client:
    # Synchronous — blocks until all subscribers respond
    results = client.publish("load_user_profile", {"user_name": "alice"})
    for sub_name, result in results.items():
        print(f"{sub_name}: {result.status} -> {result.data}")

    # Asynchronous — get a ResultHandler and wait later
    handler = client.publish_async("load_user_profile", {"user_name": "bob"})
    # ... do other work ...
    results = handler.wait(timeout=10.0)

    # Fire-and-forget — no result collection
    client.send("send_email", {"to": "user@example.com", "body": "Hello!"})
```

## Message Patterns

### Synchronous publish

```python
results = client.publish("load_user_profile", {"user_name": "alice"})
# results: dict[str, ProcessResult] — keyed by subscriber name
```

### Asynchronous publish

```python
handler = client.publish_async("load_user_profile", {"user_name": "bob"})
# ... do other work ...
results = handler.wait(timeout=30.0)
```

Wait for multiple async handlers:

```python
handlers = [
    client.publish_async("load_user_profile", {"user_name": f"user_{i}"})
    for i in range(10)
]
all_results = client.wait_all(handlers)
```

### Fire-and-forget

```python
client.send("send_email", {"to": "user@example.com"})
```

### Batch async with progress

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

### Message relay (subscriber re-publishes)

Enable `embedded_publisher` in subscriber config to allow handlers to send new messages:

```yaml
# subscribers/data_loader.yaml
embedded_publisher: "gpmq.publisher.embedded"
```

Then in the handler, use the embedded publisher from context:

```python
@message_handler(["msg_ping"])
def on_ping(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    if context and "publisher" in context:
        context["publisher"].send("msg_pong", {"reply_to": msg.id}, correlation_id=msg.id)
    return {"status": "pong_sent"}
```

## Configuration Reference

GPMQ uses a two-level configuration: **common config** (`gpmq/common.yaml`) provides shared defaults, and each **publisher** or **subscriber** config only needs to specify its own fields. Any field not explicitly set in the specific config falls back to the common config value.

```
Final config = common.yaml defaults  ⊕  specific.yaml overrides (only explicitly set fields)
```

### GPMQConfig — common.yaml

Shared settings for all components. Loaded as the base layer for both publishers and subscribers.

| Key | Type | Default | Description |
|---|---|---|---|
| `redis_host` | str | `"localhost"` | Redis server host |
| `redis_port` | int | `6379` | Redis server port (1–65535) |
| `redis_db` | int | `0` | Redis database number |
| `redis_password` | str | `""` | Redis password |
| `redis_max_retries` | int | `3` | Max connection retry attempts |
| `redis_retry_delay` | float | `1.0` | Base retry delay in seconds |
| `stream_name` | str | `"gpmq:messages"` | Redis Stream name |
| `enable_registry` | bool | `True` | Enable subscriber registry |
| `heartbeat_interval` | int | `10` | Heartbeat interval in seconds |
| `registry_key_prefix` | str | `"gpmq:"` | Redis key prefix (auto-appends `:`) |
| `subscriber_timeout` | float | `60.0` | Per-subscriber processing timeout (seconds) |
| `audit_db_path` | str | `"gpmq_audit.db"` | SQLite audit database path |
| `enable_audit` | bool | `True` | Enable audit logging |
| `log_category` | str | `"gpmq"` | Log category for gpclog |

### PublisherConfig — publisher/*.yaml

Extends `GPMQConfig`. Config file must include `cfg_class_name: "PublisherConfig"`.

| Key | Type | Default | Description |
|---|---|---|---|
| `publisher_name` | str | *(required)* | Unique publisher name |
| `publish_timeout` | float \| null | `null` | Total timeout for sync publish (null = no limit) |
| `async_batch_queue_size` | int | `32` | Default queue size for `async_batch()` |

Plus all `GPMQConfig` fields as inherited defaults.

**Example** — a minimal publisher that only overrides `publisher_name` and `log_category`:

```yaml
cfg_class_name: "PublisherConfig"
publisher_name: "my_app"
log_category: "my_app_publisher"
# redis_host, redis_port, stream_name, etc. come from common.yaml
```

### SubscriberConfig — subscribers/*.yaml

Extends `GPMQConfig`. Config file must include `cfg_class_name: "SubscriberConfig"`.

| Key | Type | Default | Description |
|---|---|---|---|
| `name` | str | *(required)* | Subscriber name (also consumer group name) |
| `handlers` | list[HandlerConfig] | *(required)* | Message handler configurations |
| `workers` | int | `1` | Number of worker processes |
| `embedded_publisher` | str \| null | `null` | Publisher config path for message relay |
| `worker_context_processor` | str \| null | `null` | Import path to worker context init function |
| `worker_context` | dict \| null | `null` | Custom data dict injected into every worker's context |
| `run_forever_method` | str \| null | `null` | Import path to a `@run_forever_method` decorated function |
| `run_forever_method_paras` | dict \| null | `null` | Custom parameters dict passed to `run_forever_method` function's `paras` argument |

Plus all `GPMQConfig` fields as inherited defaults.

**HandlerConfig** (items in the `handlers` list):

| Key | Type | Description |
|---|---|---|
| `message_types` | list[str] | Message types this handler processes |
| `handler` | str | Import path to the handler function |

**Example** — a subscriber that overrides Redis settings and enables message relay:

```yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
workers: 4
redis_db: 1              # override common.yaml default
enable_audit: true
audit_db_path: "./audit/data_loader.db"

handlers:
  - message_types: ["load_user_profile"]
    handler: "myapp.handlers.on_load_user_profile"

embedded_publisher: "gpmq.publisher.embedded"
```

## Worker Context Processor

The `worker_context_processor` lets you run custom initialization logic in each worker process right after it starts, before any message is handled. It receives the current worker context dict, and its return value is merged back into the context, making new items available to all message handlers.

### What's in the context by default

When a worker starts, the context already contains:

| Key | Type | Description |
|---|---|---|
| `project_name` | str | Project name from GPConfigManager |
| `subscriber_name` | str | Name of the subscriber |
| `subscribe_message_types` | list[str] | Message types this subscriber handles |
| `worker_id` | str | Worker identifier (e.g. `"data_loader-server1-worker-0"`) |
| `worker_sn` | int | Worker serial number (0, 1, 2, ...) |
| `config_manager` | GPConfigManager | Config manager instance (if initialized) |
| `logger` | GPCLogger | Logger instance for this worker |
| `publisher` | GPMQClient | Embedded publisher client (if `embedded_publisher` is set) |

### Configuration

Set the import path of your processor function in the subscriber config:

```yaml
# subscribers/data_loader.yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
worker_context_processor: "myapp.workers.init_context"
```

### Writing a context processor

```python
# myapp/workers.py
from typing import Any
from gpmq import worker_context_processor

@worker_context_processor
def init_context(context: dict[str, Any]) -> dict[str, Any]:
    """Initialize resources shared by all handlers in this worker."""
    logger = context["logger"]
    cfg_mgr = context.get("config_manager")

    logger.info(f"Initializing worker {context['worker_id']}")

    # Return a dict — items will be merged into the worker context
    return {
        "db_connection": create_db_pool(cfg_mgr),
        "api_client": APIClient(cfg_mgr),
    }
```

The function must:
- Accept exactly one parameter named `context`
- Return a `dict[str, Any]` — returned items are merged into the context
- Be decorated with `@worker_context_processor`

### Using context in message handlers

Handlers that accept two parameters receive the worker context as the second argument:

```python
from gpmq import message_handler, Message
from typing import Any, Optional

@message_handler(["load_user_profile"])
def on_load_user_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    logger = context["logger"]
    db = context["db_connection"]          # from worker_context_processor

    logger.info(f"Loading profile for {msg.payload['user_name']}")
    user = db.query("SELECT * FROM users WHERE name = ?", msg.payload["user_name"])
    return {"profile": user}
```

### Execution order

1. Worker process starts
2. Built-in context is prepared (`worker_id`, `logger`, `config_manager`, `publisher`, etc.)
3. **Your `worker_context_processor` runs** — it receives the built-in context and can add new items
4. Message handlers begin consuming — they receive the full merged context

## Run Forever Method

The `run_forever_method` lets you replace the subscriber's default `time.sleep(1)` loop with your own blocking function. This is useful when you need the main process to actively drive work (e.g., polling an external source) instead of passively waiting for messages.

### Configuration

```yaml
# subscribers/data_loader.yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
run_forever_method: "myapp.workers.my_main_loop"
run_forever_method_paras:
  poll_interval: 5
  batch_size: 100
```

### Writing a run-forever method

```python
# myapp/workers.py
from typing import Any, Optional
from gpmq.subscriber import run_forever_method, Subscriber

@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    """Custom main loop that runs in the subscriber process."""
    interval = paras.get("poll_interval", 5) if paras else 5
    while True:
        # Your custom logic here
        time.sleep(interval)
```

The function must:
- Accept exactly two parameters: a `Subscriber` instance and `paras` (a `dict` or `None`)
- Have a return annotation of `None` (if annotated)
- Be decorated with `@run_forever_method`
- Be a blocking call that runs indefinitely (the loop ends on `KeyboardInterrupt`)

### Without a custom method

If `run_forever_method` is not configured, `Subscriber.run_forever()` uses a simple `time.sleep(1)` loop as the default blocking behavior.

## CLI Reference

> For detailed CLI usage, see [docs/cli/gpmq_cli.md](docs/cli/gpmq_cli.md).

```
gpmq [OPTIONS] COMMAND ...

Options:
  --config PATH   Configuration folder containing global_env.yaml

Commands:
  worker SUBSCRIBER_NAME   Start a subscriber worker
  status                   Show active subscribers
  clear --stream NAME      Clear a Redis Stream
  audit query              Query audit records
  audit cleanup            Delete old audit records
  audit stats              Show audit statistics
```

Standalone worker command:

```
gpmq-worker SUBSCRIBER_NAME [--config PATH]
```

## API Reference

> For detailed API documentation, see [docs/api/gpmq_api.md](docs/api/gpmq_api.md).

| Class / Function | Description |
|---|---|
| `GPMQClient` | Main entry point for publishing messages |
| `Subscriber` | Manages subscriber lifecycle and workers |
| `message_handler(types)` | Decorator to register a message handler |
| `worker_context_processor` | Decorator for worker context initialization |
| `run_forever_method` | Decorator for custom subscriber main loop |
| `get_client(cfg_path)` | Create a `GPMQClient` from config |
| `get_subscriber(cfg_path)` | Create a `Subscriber` from config |
| `set_config_manager(manager)` | Set the global config manager |
| `ResultHandler` | Async result collection handle |
| `GPMQAsyncBatchHandler` | Batch async message processing |
| `GPMQProgress` | Base class for progress indicators |
| `Message` | Message data model |
| `ProcessResult` | Processing result with status and data |
| `get_consumer_group_workers(name)` | Query all workers in a consumer group |
| `WorkerInfo` | Worker status data model |
| `ConsumerGroupWorkers` | Consumer group worker list model |

## License

MIT
