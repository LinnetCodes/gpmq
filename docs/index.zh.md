# GPMQ

**General Purpose Message Queue** — 基于 Redis Streams 的轻量级 Python 分布式消息队列，专为个人项目设计。

## 特性

- **多种消息模式** — 同步、异步、fire-and-forget、滑动窗口批量并发、消息中继
- **可定制 run_forever 方法** — 通过 `@run_forever_method` 自定义订阅者主循环
- **Redis Streams 后端** — 基于 Consumer Group 的可靠消息投递
- **多进程 Worker** — 每个订阅者可配置独立 Worker 数量
- **YAML 配置** — 通过 gpconfig 实现类型安全的 common/specific 配置合并
- **审计追踪** — 可选的 SQLite 消息与结果日志
- **Worker 可见性** — 通过 `get_consumer_group_workers()` 查询 Consumer Group 中所有 Worker 状态
- **CLI 工具** — 启动 Worker、查询审计记录、检查系统状态

## 安装

```bash
pip install gpmq
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
    return {"profile": f"profile of {user_name}"}
```

### 2. 编写配置

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

### 3. 启动 Worker

```bash
gpmq worker gpmq.subscribers.data_loader --config ./configs
```

### 4. 发布消息

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

## 文档

| 文档 | 说明 |
|------|------|
| [API 参考](api/gpmq_api.md) | 完整的 API 文档 |
| [CLI 参考](cli/gpmq_cli.md) | 命令行工具使用说明 |
