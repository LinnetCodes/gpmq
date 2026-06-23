# GPMQ

**General Purpose Message Queue** — A lightweight Python distributed message queue built on Redis Streams, designed for personal projects.

## Features

- **Multiple messaging patterns** — sync, async, fire-and-forget, batch with sliding-window concurrency, and message relay
- **Configurable run-forever method** — customize the subscriber's main loop with `@run_forever_method`
- **Redis Streams backend** — reliable message delivery with consumer groups
- **Multi-process workers** — configurable worker count per subscriber
- **YAML configuration** — type-safe config via gpconfig with common/specific merging
- **Audit trail** — optional SQLite-based message and result logging
- **Worker visibility** — query all workers in a consumer group via `get_consumer_group_workers()`
- **CLI tools** — start workers, query audit records, check system status

## Installation

```bash
pip install gpmq
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
    return {"profile": f"profile of {user_name}"}
```

### 2. Write configuration

```yaml
# configs/gpmq/common.yaml
cfg_class_name: "GPMQConfig"
redis_host: "localhost"
redis_port: 6379
stream_name: "gpmq:messages"
```

```yaml
# configs/gpmq/subscribers/data_loader.yaml
cfg_class_name: "SubscriberConfig"
name: "data_loader"
workers: 4
handlers:
  - message_types: ["load_user_profile"]
    handler: "myapp.handlers.on_load_user_profile"
```

### 3. Start workers

```bash
gpmq worker gpmq.subscribers.data_loader --config ./configs
```

### 4. Publish messages

```python
from gpmq import set_config_manager, get_client
from gpconfig import GPConfigManager

cfg_mgr = GPConfigManager("my_project", "./configs")
set_config_manager(cfg_mgr)

with get_client("gpmq.publisher.main") as client:
    results = client.publish("load_user_profile", {"user_name": "alice"})
    for sub_name, result in results.items():
        print(f"{sub_name}: {result.status} -> {result.data}")
```

## Documentation

| Doc | Description |
|-----|-------------|
| [API Reference](api/gpmq_api.md) | Full API documentation |
| [CLI Reference](cli/gpmq_cli.md) | Command-line tool usage |
