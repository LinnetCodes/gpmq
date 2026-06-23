# GPMQ API Reference

Configuration and lifecycle management functions, plus the core classes for publishing messages, subscribing to streams, and collecting results.

---

## Table of Contents

- [Configuration Functions](#configuration-functions)
  - [set_config_manager](#set_config_manager)
  - [get_client](#get_client)
  - [get_subscriber](#get_subscriber)
- [GPMQClient](#gpmqclient)
- [Subscriber](#subscriber)
- [ResultHandler](#resulthandler)
- [GPMQAsyncBatchHandler](#gpmqasyncbatchhandler)
- [GPMQProgress](#gpmqprogress)
- [Decorators](#decorators)
  - [message_handler](#message_handler)
  - [worker_context_processor](#worker_context_processor)
  - [run_forever_method](#run_forever_method)
- [Data Models](#data-models)
  - [Message](#message)
  - [ProcessResult / ProcessStatus](#processresult--processstatus)
  - [WorkerInfo / ConsumerGroupWorkers](#workerinfo--consumergroupworkers)

---

## Configuration Functions

### `set_config_manager`

```python
set_config_manager(cfg_mgr: GPConfigManager) -> None
```

Set the global `GPConfigManager` instance. Must be called before using `get_client()` or `get_subscriber()`.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `cfg_mgr` | `GPConfigManager` | An initialized config manager instance |

**Raises**

| Exception | Condition |
|-----------|-----------|
| `TypeError` | `cfg_mgr` is not a `GPConfigManager` instance |

**Example**

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

Create a `GPMQClient` from merged common + specific config. Requires `set_config_manager()` to have been called.

**Parameters**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `cfg_path` | `str?` | `None` | Dot-notation path to a PublisherConfig YAML, e.g. `"gpmq.publisher.main"` |
| `publisher_name` | `str?` | `None` | Publisher name. Used when no `cfg_path` is given, or to verify against a config file. |
| `timeout` | `float?` | `None` | Overrides `publish_timeout` in the final config. Requires `cfg_path` or `publisher_name`. |

**Three usage modes**

| Mode | Call | publisher_name source |
|------|------|-----------------------|
| From config file | `get_client("gpmq.publisher.main")` | From YAML |
| By name | `get_client(publisher_name="my_app")` | From argument |
| Anonymous | `get_client()` | `"anonymous"` |

**Returns** — A constructed (but not connected) `GPMQClient` instance.

**Raises**

| Exception | Condition |
|-----------|-----------|
| `RuntimeError` | `set_config_manager()` has not been called |
| `ValueError` | `cfg_path` and `publisher_name` names conflict, or `timeout` given for anonymous client |

---

### `get_subscriber`

```python
get_subscriber(cfg_path: str) -> Subscriber
```

Create a `Subscriber` from merged common + specific config. Requires `set_config_manager()` to have been called.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `cfg_path` | `str` | Dot-notation config path, e.g. `"gpmq.subscribers.data_loader"` |

**Returns** — A constructed (but not started) `Subscriber` instance.

**Raises**

| Exception | Condition |
|-----------|-----------|
| `RuntimeError` | `set_config_manager()` has not been called |

---

## GPMQClient

The main entry point for publishing messages. Supports synchronous, asynchronous, fire-and-forget, and batch modes.

### Constructor

```python
GPMQClient(config: GPMQConfig)
```

Typically created via `get_client()` rather than directly. Supports the context manager protocol (`with` statement) for automatic `connect()` / `close()`.

### Methods

#### `publish`

```python
publish(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    timeout: Optional[float] = None,
) -> dict[str, ProcessResult]
```

Publish a message and **block** until all subscribers return results (or timeout). Internally equivalent to `publish_async()` + `handler.wait()`.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `message_type` | `str` | Message type identifier |
| `payload` | `dict` | Message payload |
| `correlation_id` | `str?` | Optional ID for tracing message chains |
| `timeout` | `float?` | Max seconds to wait. `None` = use config default. |

**Returns** — `dict[str, ProcessResult]`, keyed by subscriber name.

**Notes**
- If no subscribers are active, returns an empty dict immediately.
- Subscribers that don't respond within `timeout` get `ProcessStatus.TIMEOUT`.

---

#### `publish_async`

```python
publish_async(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
) -> ResultHandler
```

Publish a message and return a `ResultHandler` immediately. Call `handler.wait()` later to collect results.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `message_type` | `str` | Message type identifier |
| `payload` | `dict` | Message payload |
| `correlation_id` | `str?` | Optional ID for tracing message chains |

**Returns** — `ResultHandler` for deferred result collection.

---

#### `send`

```python
send(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
) -> str
```

Fire-and-forget publish. Sends the message to the Redis Stream but does **not** subscribe to result channels. No `ResultHandler` is created, avoiding Pub/Sub resource overhead.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `message_type` | `str` | Message type identifier |
| `payload` | `dict` | Message payload |
| `correlation_id` | `str?` | Optional ID for tracing message chains |

**Returns** — The Redis-generated message ID (useful for logging).

**When to use** — Notification-style messages where you don't need to know if or when subscribers processed them (e.g., sending emails, logging events).

---

#### `wait_all`

```python
wait_all(
    handlers: list[ResultHandler],
    timeout: Optional[float] = None,
    allow_timeout_retry: Optional[bool] = None,
) -> list[dict[str, ProcessResult]]
```

Wait for results from multiple `ResultHandler` instances.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `handlers` | `list[ResultHandler]` | Result handlers to wait on |
| `timeout` | `float?` | Max seconds to wait per handler |
| `allow_timeout_retry` | `bool?` | Allow re-calling this method on the same handlers after a partial timeout |

**Returns** — `list[dict[str, ProcessResult]]`, one result dict per handler, preserving input order.

---

#### `async_batch`

```python
async_batch(
    progress: Optional[GPMQProgress] = None,
    timeout: Optional[float] = None,
    queue_size: int = 32,
) -> GPMQAsyncBatchHandler
```

Create a batch handler with **sliding-window concurrency**. See [GPMQAsyncBatchHandler](#gpmqasyncbatchhandler).

**Parameters**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `progress` | `GPMQProgress?` | `None` | Progress indicator |
| `timeout` | `float?` | `None` | Total timeout for all results. `None` = wait forever. |
| `queue_size` | `int` | `32` | Max concurrent in-flight messages |

---

#### `get_audit_store`

```python
get_audit_store() -> Optional[AuditStore]
```

Returns the audit store instance, or `None` if audit is disabled (`enable_audit: false`).

---

#### `get_consumer_group_workers`

```python
get_consumer_group_workers(subscriber_name: str) -> ConsumerGroupWorkers
```

Query all workers registered under a consumer group, including idle ones. Workers whose heartbeat exceeds `subscriber_timeout` are marked as `"stale"`.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `subscriber_name` | `str` | Name of the subscriber / consumer group |

**Returns** — `ConsumerGroupWorkers` with worker list and count.

**Notes**
- Workers that crashed without graceful shutdown remain in the registry. They appear with `status: "stale"`.
- Heartbeat timeout is determined by `subscriber_timeout` in config.

**Example**

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

Connect to Redis and the audit store, or disconnect and clean up. Both are **reentrant** — calling them multiple times is safe. When using the context manager, these are called automatically.

---

## Subscriber

Manages a subscriber's lifecycle: connects to Redis, registers the subscriber, starts a heartbeat, and manages worker processes.

### Constructor

```python
Subscriber(config: SubscriberConfig)
```

Typically created via `get_subscriber()`.

### Methods

#### `start`

```python
start() -> None
```

Start the subscriber. Performs: connect to Redis → load handlers → validate handler types → register in Redis → create consumer group → start heartbeat daemon → start workers.

**Notes**
- This method blocks briefly during setup, then returns. Workers run in child processes.
- Must be called before the subscriber can process messages.
- Validates that configured `message_types` are a subset of decorator-declared types; raises `HandlerTypeMismatchError` on mismatch.

---

#### `stop`

```python
stop() -> None
```

Gracefully stop the subscriber: stop workers, stop heartbeat thread, unregister from Redis, disconnect.

**Notes**
- Sends SIGTERM to worker processes, waits up to 30 seconds, then SIGKILL if needed.

---

#### `apply_worker_context`

```python
apply_worker_context(**kwargs) -> Subscriber
```

Inject custom data into every worker's context dict. Must be called **before** `start()`. Can be called multiple times — each call merges new keys.

**Returns** — `self`, for method chaining.

**Example**

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

Returns subscriber status information including name, running state, worker count, subscribed message types, and individual worker statuses.

---

#### `run_forever`

```python
run_forever() -> None
```

Start the subscriber and block the calling thread until interrupted (`KeyboardInterrupt` / `SystemExit`). On interruption, `stop()` is called automatically.

If `run_forever_method` is configured in `SubscriberConfig`, that custom function is called instead of the default `time.sleep(1)` loop. See [run_forever_method](#run_forever_method).

**Notes**
- Equivalent to calling `start()` followed by a blocking loop.
- This is the recommended way to run a subscriber programmatically.

**Example**

```python
sub = get_subscriber("gpmq.subscribers.data_loader")
sub.run_forever()  # blocks until Ctrl+C
```

---

## ResultHandler

Manages async result collection for a single published message. Subscribes to Redis Pub/Sub channels and collects `ProcessResult` from each subscriber.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `message_id` | `str` | The message ID being tracked |

### Methods

#### `wait`

```python
wait(
    timeout: Optional[float] = None,
    allow_timeout_retry: Optional[bool] = None,
) -> dict[str, ProcessResult]
```

Block until all expected subscribers return results, or until timeout.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `timeout` | `float?` | Max seconds to wait. `None` = use default polling. |
| `allow_timeout_retry` | `bool?` | If `True`, allows calling `wait()` again after a partial timeout without saving timeout results. |

**Returns** — `dict[str, ProcessResult]`, keyed by subscriber name. Subscribers that didn't respond get `ProcessStatus.TIMEOUT`.

**Notes**
- After `wait()` completes, the Pub/Sub subscription is cleaned up automatically.
- If `allow_timeout_retry` is `False` (default), a second call to `wait()` returns the same cached results.

---

#### `wait_any`

```python
wait_any(timeout: Optional[float] = None) -> dict[str, ProcessResult]
```

Wait for **at least one** subscriber to return a result.

**Returns** — `dict[str, ProcessResult]` with at least one entry. Empty dict if timeout with no results.

---

#### `is_complete`

```python
is_complete() -> bool
```

Check if all expected results have been collected. Non-blocking.

---

#### `get_completed_count`

```python
get_completed_count() -> int
```

Number of results received so far.

---

#### `get_results`

```python
get_results() -> dict[str, ProcessResult]
```

Get all results collected so far, without waiting. May be incomplete.

---

## GPMQAsyncBatchHandler

Batch async message processor with sliding-window concurrency. Created by `GPMQClient.async_batch()`.

### How it works

1. You queue messages with `send_message()`.
2. On `wait_all()` (or `with` exit), messages are sent using a sliding window:
   - First sends up to `queue_size` messages concurrently.
   - As each result completes, the next queued message is sent.
3. Results are collected in send order.

### Methods

#### `send_message`

```python
send_message(
    message_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
) -> None
```

Queue a message for deferred sending. Messages are not published until `wait_all()` is called.

---

#### `wait_all`

```python
wait_all() -> list[dict[str, ProcessResult]]
```

Send all queued messages (respecting the sliding window) and wait for all results.

**Returns** — `list[dict[str, ProcessResult]]`, one per message, in `send_message()` order.

---

#### `get_all_result`

```python
get_all_result() -> list[dict[str, ProcessResult]]
```

Retrieve all results after `wait_all()` has completed. Returns an empty list if called before `wait_all()`.

---

### Context Manager

```python
with client.async_batch(progress, timeout=30) as batch:
    for i in range(100):
        batch.send_message("my_type", {"index": i})
# wait_all() is called automatically on exit
results = batch.get_all_result()
```

---

## GPMQProgress

Abstract base class for progress tracking during batch operations. Subclass to implement custom progress display.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `max_value` | `int` | Total number of messages |
| `current_value` | `int` | Current progress count |

### Methods to Override

| Method | Description |
|--------|-------------|
| `create(max_value)` | Called by the framework with the total message count |
| `start()` | Called when batch processing begins |
| `update(value)` | Called each time a message completes. Call `super().update(value)` to update `current_value`. |
| `finish()` | Called when all messages are done |

**Example**

```python
from gpmq import GPMQProgress

class MyProgress(GPMQProgress):
    def start(self):
        print(f"Starting 0/{self.max_value}")
    def update(self, value):
        super().update(value)
        print(f"Progress: {self.current_value}/{self.max_value}")
    def finish(self):
        print("All done!")
```

---

## Decorators

### `message_handler`

```python
@message_handler(message_types: list[str])
```

Decorator that marks a function as a message handler and declares which message types it can process.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `message_types` | `list[str]` | Message types this handler is capable of processing (required, non-empty) |

**Handler signatures**

The framework auto-detects the number of parameters:

```python
# Without context (1 parameter)
@message_handler(["user_created"])
def on_user_created(msg: Message) -> Any:
    ...

# With context (2 parameters)
@message_handler(["load_profile"])
def on_load_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    ...
```

**Notes**
- Return values are interpreted as follows:
  - `None` (or no `return`) → `ProcessStatus.SUCCESS`
  - `ProcessStatus.FAILURE` → `ProcessStatus.FAILURE` (explicit spec-defined failure)
  - `ProcessResult` → used as-is
  - Any other value → `ProcessStatus.SUCCESS` with the value stored in `data`
- To indicate an exception, raise one — it will be captured as `ProcessStatus.EXCEPTION`.
- Configured `message_types` in YAML must be a **subset** of the decorator-declared types, otherwise `HandlerTypeMismatchError` is raised at startup.

---

### `worker_context_processor`

```python
@worker_context_processor
def init_context(context: dict[str, Any]) -> dict[str, Any]:
    ...
```

Decorator that marks a function as a worker context initializer. Called once per worker process, after startup but before any messages are processed.

**Requirements**
- Must accept exactly one parameter named `context`.
- Must return `dict[str, Any]` — items are merged into the worker context.
- Configured in subscriber YAML via `worker_context_processor: "myapp.module.func"`.

**Built-in context items available**

| Key | Type | Description |
|---|---|---|
| `project_name` | `str` | From GPConfigManager |
| `subscriber_name` | `str` | Subscriber name |
| `subscribe_message_types` | `list[str]` | Message types handled |
| `worker_id` | `str` | e.g. `"data_loader-worker-0"` |
| `worker_sn` | `int` | Worker serial number (0, 1, ...) |
| `config_manager` | `GPConfigManager` | Config manager (if initialized) |
| `logger` | `GPCLogger` | Logger for this worker |
| `publisher` | `GPMQClient` | Embedded publisher (if configured) |

---

### `run_forever_method`

```python
@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    ...
```

Decorator that validates and marks a function as a custom run-forever method for `Subscriber.run_forever()`. The decorated function replaces the default `time.sleep(1)` loop, allowing the main process to actively drive work.

**Requirements**
- Must accept exactly two parameters:
  - First: a `Subscriber` instance
  - Second: named `paras` (case-insensitive), typed as `Optional[dict[str, Any]]` or `dict[str, Any]`
- Return annotation if present must be `None`.
- Must be a blocking call that runs indefinitely.
- Configured in subscriber YAML via `run_forever_method: "myapp.module.func"`.

**Parameters of the decorated function**

| Name | Type | Description |
|------|------|-------------|
| `subscriber` | `Subscriber` | The subscriber instance that called `run_forever()` |
| `paras` | `dict[str, Any]?` | Custom parameters from `run_forever_method_paras` config, or `None` |

**Configuration**

```yaml
# subscribers/data_loader.yaml
run_forever_method: "myapp.workers.my_main_loop"
run_forever_method_paras:
  poll_interval: 5
  batch_size: 100
```

**Example**

```python
from typing import Any, Optional
from gpmq.subscriber import run_forever_method, Subscriber

@run_forever_method
def my_main_loop(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
    interval = paras.get("poll_interval", 5) if paras else 5
    while True:
        # Custom logic here
        time.sleep(interval)
```

---

## Data Models

### Message

```python
class Message(BaseModel):
    id: str
    type: str
    timestamp: datetime
    publisher: str
    payload: dict
    correlation_id: Optional[str] = None
```

The core data unit passed through the message queue.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Redis Streams generated message ID |
| `type` | `str` | Message type identifier |
| `timestamp` | `datetime` | Publish time |
| `publisher` | `str` | Name of the publisher |
| `payload` | `dict` | Message payload |
| `correlation_id` | `str?` | For tracing message chains |

---

### ProcessResult / ProcessStatus

```python
class ProcessStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    EXCEPTION = "exception"
    TIMEOUT = "timeout"

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

The result returned by each subscriber after processing a message.

**ProcessStatus values**

| Value | Meaning |
|-------|---------|
| `SUCCESS` | Handler completed normally |
| `FAILURE` | Handler explicitly returned `ProcessStatus.FAILURE` (spec-defined failure, not an exception) |
| `EXCEPTION` | An exception was thrown during processing |
| `TIMEOUT` | Processing timed out, worker was killed |

**ProcessResult fields**

| Field | Type | Description |
|-------|------|-------------|
| `message_id` | `str` | ID of the processed message |
| `status` | `ProcessStatus` | Processing outcome |
| `subscriber_name` | `str` | Name of the subscriber that processed it |
| `processing_time` | `float` | Processing time in seconds |
| `data` | `dict?` | Return value from the handler |
| `error_message` | `str?` | Error description (for EXCEPTION / TIMEOUT) |
| `error_traceback` | `str?` | Full traceback (for EXCEPTION) |
| `timeout_seconds` | `float?` | Configured timeout value (for TIMEOUT) |

---

### WorkerInfo / ConsumerGroupWorkers

```python
class WorkerInfo(BaseModel):
    worker_id: str
    hostname: str
    pid: int
    status: str          # "running" or "stale"
    started_at: float
    last_heartbeat: float

class ConsumerGroupWorkers(BaseModel):
    subscriber_name: str
    workers: list[WorkerInfo]
    worker_count: int    # auto-computed from workers list
```

Worker visibility models for querying consumer group membership.

**WorkerInfo fields**

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `str` | Unique worker identifier, e.g. `"data_loader-server1-worker-0"` |
| `hostname` | `str` | Host machine name where the worker is running |
| `pid` | `int` | Process ID |
| `status` | `str` | Worker status: `"running"` or `"stale"` |
| `started_at` | `float` | Unix timestamp when the worker started |
| `last_heartbeat` | `float` | Unix timestamp of the last heartbeat |

**ConsumerGroupWorkers fields**

| Field | Type | Description |
|-------|------|-------------|
| `subscriber_name` | `str` | Subscriber / consumer group name |
| `workers` | `list[WorkerInfo]` | List of workers |
| `worker_count` | `int` | Number of workers (auto-computed) |
