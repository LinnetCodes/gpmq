# GPMQ CLI 使用说明

GPMQ 提供两个命令行入口：

- **`gpmq`** — 主命令，包含审计查询、Stream 管理、订阅者查看、Worker 启动等子命令
- **`gpmq-worker`** — 独立启动订阅者 Worker 进程的快捷命令

## 全局选项

`--config` 选项用于指定配置文件夹路径，该文件夹根目录必须包含 `global_env.yaml` 文件，供 GPConfigManager 初始化使用。`--config` 可以放在子命令前或后。

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `--config` | 无 | 配置文件夹路径（必须包含 `global_env.yaml`） |

如果未指定 `--config`，GPConfigManager 会依次尝试：

1. 环境变量 `GPMQ_CLI_CFG_PATH`
2. 用户主目录下的 `gpmq_cli/` 子文件夹

如果都找不到有效的配置文件夹，GPConfigManager 会抛出异常。

---

## gpmq

```
gpmq [OPTIONS] COMMAND [ARGS]...
```

### 子命令一览

| 命令 | 说明 |
|------|------|
| `worker` | 启动订阅者 Worker 进程 |
| `audit` | 审计记录管理（含 query / cleanup / stats） |
| `status` | 查看系统状态：活跃的订阅者及其信息 |
| `clear` | 清空指定的 Redis Stream |

---

### gpmq worker

启动一个 GPMQ 订阅者 Worker 进程。

```
gpmq worker SUBSCRIBER_NAME [--config CONFIG_FOLDER]
```

| 参数 / 选项 | 必填 | 说明 |
|-------------|------|------|
| `SUBSCRIBER_NAME` | 是 | 订阅者配置路径（点分格式），如 `gpmq.subscribers.data_loader` |
| `--config` | 否 | 配置文件夹路径 |

**示例：**

```bash
# 指定配置文件夹和订阅者
gpmq worker gpmq.subscribers.data_loader --config "/path_to_your_configs"

# 通过环境变量指定配置路径后，直接启动
set GPMQ_CLI_CFG_PATH="/path_to_your_configs"
gpmq worker gpmq.subscribers.notifier
```

启动后 Worker 会持续运行，按 `Ctrl+C` 停止。

---

### gpmq audit query

查询审计记录，支持多种过滤条件。

```
gpmq audit query COMPONENT_NAME [OPTIONS]
```

| 参数 / 选项 | 必填 | 说明 |
|-------------|------|------|
| `COMPONENT_NAME` | 是 | Publisher 或 Subscriber 配置路径，如 `gpmq.subscribers.data_loader` 或 `gpmq.publisher.main` |
| `--type` | 否 | 按消息类型过滤 |
| `--status` | 否 | 按处理状态过滤，可选值：`success` / `failure` / `exception` / `timeout` |
| `--correlation-id` | 否 | 按关联 ID 过滤 |
| `--from` | 否 | 起始时间，ISO 格式，如 `2026-01-01T00:00:00` |
| `--to` | 否 | 结束时间，ISO 格式 |
| `--limit` | `100` | 最大返回条数 |
| `--format` | `table` | 输出格式：`json` / `table` |
| `--config` | 否 | 配置文件夹路径 |

执行时会显示当前操作的 audit store 文件路径。如果该组件的 `enable_audit` 为 `false`，会提示 audit 已禁用并退出。

**示例：**

```bash
# 查询指定 subscriber 的失败记录
gpmq audit query gpmq.subscribers.data_loader --status failure --limit 20

# 按消息类型 + 时间范围查询 publisher 的审计记录
gpmq audit query gpmq.publisher.main --type OrderCreated --from 2026-04-01T00:00:00 --to 2026-04-10T23:59:59

# 按关联 ID 查询
gpmq audit query gpmq.subscribers.notifier --correlation-id abc-123-def

# JSON 格式输出
gpmq audit query gpmq.subscribers.data_loader --status success --format json
```

> **注意：** 过滤条件的优先级为：`--correlation-id` > `--status` > `--type` > `--from/--to`。同时指定多个条件时，只有优先级最高的生效。不指定任何过滤条件时会提示用法。

---

### gpmq audit cleanup

删除旧的审计记录。

```
gpmq audit cleanup COMPONENT_NAME --before TIMESTAMP [OPTIONS]
```

| 参数 / 选项 | 必填 | 说明 |
|-------------|------|------|
| `COMPONENT_NAME` | 是 | Publisher 或 Subscriber 配置路径 |
| `--before` | 是 | 删除此时间戳之前的记录，ISO 格式 |
| `--type` | 否 | 按消息类型删除（指定时忽略 `--before`） |
| `--format` | `table` | 输出格式 |
| `--config` | 否 | 配置文件夹路径 |

**示例：**

```bash
# 删除 2026-03-01 之前的所有审计记录
gpmq audit cleanup gpmq.subscribers.data_loader --before 2026-03-01T00:00:00

# 删除指定类型的记录
gpmq audit cleanup gpmq.publisher.main --before 2026-03-01T00:00:00 --type OrderCreated
```

---

### gpmq audit stats

显示审计统计信息。

```
gpmq audit stats COMPONENT_NAME [OPTIONS]
```

| 参数 / 选项 | 必填 | 说明 |
|-------------|------|------|
| `COMPONENT_NAME` | 是 | Publisher 或 Subscriber 配置路径 |
| `--format` | `table` | 输出格式 |
| `--config` | 否 | 配置文件夹路径 |

**示例：**

```bash
gpmq audit stats gpmq.subscribers.data_loader
gpmq audit stats gpmq.publisher.main --format json
```

---

### gpmq status

查看系统当前状态，包括所有活跃的订阅者及其详细信息。

```
gpmq status [--config CONFIG_FOLDER] [OPTIONS]
```

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `--format` | `table` | 输出格式 |
| `--config` | 无 | 配置文件夹路径 |

**说明**
- 如需查询特定订阅者的 Worker 详情（主机名、PID、心跳状态），请使用 `GPMQClient` 的 `get_consumer_group_workers()` API 方法。

**示例：**

```bash
gpmq status
gpmq status --format json
gpmq status --config "/path_to_your_configs"
```

---

### gpmq clear

清空指定的 Redis Stream。执行前会要求确认。

```
gpmq clear --stream STREAM_NAME [--config CONFIG_FOLDER]
```

| 选项 | 必填 | 说明 |
|------|------|------|
| `--stream` | 是 | 要清空的 Stream 名称 |
| `--config` | 否 | 配置文件夹路径 |

**示例：**

```bash
gpmq clear --stream order_events
```

执行时会提示：`Are you sure you want to clear stream 'order_events'?`，输入 `y` 确认。

---

## gpmq-worker

独立启动一个 GPMQ 订阅者 Worker 进程的快捷命令，等价于 `gpmq worker`。

```
gpmq-worker SUBSCRIBER_NAME [--config CONFIG_FOLDER]
```

| 参数 / 选项 | 必填 | 说明 |
|-------------|------|------|
| `SUBSCRIBER_NAME` | 是 | 订阅者配置路径（点分格式），如 `gpmq.subscribers.data_loader` |
| `--config` | 否 | 配置文件夹路径（必须包含 `global_env.yaml`） |

**示例：**

```bash
# 指定配置文件夹和订阅者
gpmq-worker gpmq.subscribers.data_loader --config "/path_to_your_configs"

# 通过环境变量指定配置路径后，直接启动
set GPMQ_CLI_CFG_PATH="/path_to_your_configs"
gpmq-worker gpmq.subscribers.notifier
```

启动后 Worker 会持续运行，按 `Ctrl+C` 停止。
