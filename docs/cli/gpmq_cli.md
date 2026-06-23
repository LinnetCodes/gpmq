# GPMQ CLI Reference

GPMQ provides two CLI entry points:

- **`gpmq`** — The main command, with subcommands for audit queries, stream management, subscriber status, and worker startup
- **`gpmq-worker`** — A shortcut command for starting a subscriber worker process

## Global Options

The `--config` option specifies the configuration folder path. The folder root must contain a `global_env.yaml` file for GPConfigManager initialization. `--config` can be placed before or after the subcommand.

| Option | Default | Description |
|--------|---------|-------------|
| `--config` | None | Configuration folder path (must contain `global_env.yaml`) |

If `--config` is not specified, GPConfigManager tries in order:

1. Environment variable `GPMQ_CLI_CFG_PATH`
2. `gpmq_cli/` subfolder under the user's home directory

If no valid configuration folder is found, GPConfigManager raises an exception.

---

## gpmq

```
gpmq [OPTIONS] COMMAND [ARGS]...
```

### Subcommands

| Command | Description |
|---------|-------------|
| `worker` | Start a subscriber worker process |
| `audit` | Audit record management (query / cleanup / stats) |
| `status` | Show system status: active subscribers and their details |
| `clear` | Clear a specified Redis Stream |

---

### gpmq worker

Start a GPMQ subscriber worker process.

```
gpmq worker SUBSCRIBER_NAME [--config CONFIG_FOLDER]
```

| Argument / Option | Required | Description |
|-------------------|----------|-------------|
| `SUBSCRIBER_NAME` | Yes | Subscriber config path (dot notation), e.g. `gpmq.subscribers.data_loader` |
| `--config` | No | Configuration folder path |

**Examples:**

```bash
# Specify config folder and subscriber
gpmq worker gpmq.subscribers.data_loader --config "/path_to_your_configs"

# Start after setting config path via environment variable
export GPMQ_CLI_CFG_PATH="/path_to_your_configs"
gpmq worker gpmq.subscribers.notifier
```

Once started, the worker runs continuously. Press `Ctrl+C` to stop.

---

### gpmq audit query

Query audit records with various filter options.

```
gpmq audit query COMPONENT_NAME [OPTIONS]
```

| Argument / Option | Required | Description |
|-------------------|----------|-------------|
| `COMPONENT_NAME` | Yes | Publisher or Subscriber config path, e.g. `gpmq.subscribers.data_loader` or `gpmq.publisher.main` |
| `--type` | No | Filter by message type |
| `--status` | No | Filter by processing status: `success` / `failure` / `exception` / `timeout` |
| `--correlation-id` | No | Filter by correlation ID |
| `--from` | No | Start time, ISO format, e.g. `2026-01-01T00:00:00` |
| `--to` | No | End time, ISO format |
| `--limit` | `100` | Maximum number of records to return |
| `--format` | `table` | Output format: `json` / `yaml` / `table` |
| `--config` | No | Configuration folder path |

When executed, the audit store file path for the current operation is displayed. If the component has `enable_audit: false`, a message indicating audit is disabled is shown and the command exits.

**Examples:**

```bash
# Query failed records for a subscriber
gpmq audit query gpmq.subscribers.data_loader --status failure --limit 20

# Query publisher audit records by message type and time range
gpmq audit query gpmq.publisher.main --type OrderCreated --from 2026-04-01T00:00:00 --to 2026-04-10T23:59:59

# Query by correlation ID
gpmq audit query gpmq.subscribers.notifier --correlation-id abc-123-def

# JSON output
gpmq audit query gpmq.subscribers.data_loader --status success --format json
```

> **Note:** Filter priority: `--correlation-id` > `--status` > `--type` > `--from/--to`. When multiple filters are specified, only the highest-priority one takes effect. If no filter is specified, usage instructions are shown.

---

### gpmq audit cleanup

Delete old audit records.

```
gpmq audit cleanup COMPONENT_NAME --before TIMESTAMP [OPTIONS]
```

| Argument / Option | Required | Description |
|-------------------|----------|-------------|
| `COMPONENT_NAME` | Yes | Publisher or Subscriber config path |
| `--before` | Yes | Delete records before this timestamp (ISO format) |
| `--type` | No | Delete by message type (when specified, `--before` is ignored) |
| `--format` | `table` | Output format |
| `--config` | No | Configuration folder path |

**Examples:**

```bash
# Delete all audit records before 2026-03-01
gpmq audit cleanup gpmq.subscribers.data_loader --before 2026-03-01T00:00:00

# Delete records of a specific type
gpmq audit cleanup gpmq.publisher.main --before 2026-03-01T00:00:00 --type OrderCreated
```

---

### gpmq audit stats

Display audit statistics.

```
gpmq audit stats COMPONENT_NAME [OPTIONS]
```

| Argument / Option | Required | Description |
|-------------------|----------|-------------|
| `COMPONENT_NAME` | Yes | Publisher or Subscriber config path |
| `--format` | `table` | Output format |
| `--config` | No | Configuration folder path |

**Examples:**

```bash
gpmq audit stats gpmq.subscribers.data_loader
gpmq audit stats gpmq.publisher.main --format json
```

---

### gpmq status

Show current system status, including all active subscribers and their details.

```
gpmq status [--config CONFIG_FOLDER] [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--format` | `table` | Output format |
| `--config` | None | Configuration folder path |

**Notes**
- To query individual worker details (hostname, PID, heartbeat status) for a specific subscriber, use the `get_consumer_group_workers()` API method from `GPMQClient`.

**Examples:**

```bash
gpmq status
gpmq status --format json
gpmq status --config "/path_to_your_configs"
```

---

### gpmq clear

Clear a specified Redis Stream. Confirmation is required before execution.

```
gpmq clear --stream STREAM_NAME [--config CONFIG_FOLDER]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--stream` | Yes | Name of the stream to clear |
| `--config` | No | Configuration folder path |

**Example:**

```bash
gpmq clear --stream order_events
```

You will be prompted: `Are you sure you want to clear stream 'order_events'?` — type `y` to confirm.

---

## gpmq-worker

A shortcut command to start a GPMQ subscriber worker process, equivalent to `gpmq worker`.

```
gpmq-worker SUBSCRIBER_NAME [--config CONFIG_FOLDER]
```

| Argument / Option | Required | Description |
|-------------------|----------|-------------|
| `SUBSCRIBER_NAME` | Yes | Subscriber config path (dot notation), e.g. `gpmq.subscribers.data_loader` |
| `--config` | No | Configuration folder path (must contain `global_env.yaml`) |

**Examples:**

```bash
# Specify config folder and subscriber
gpmq-worker gpmq.subscribers.data_loader --config "/path_to_your_configs"

# Start after setting config path via environment variable
export GPMQ_CLI_CFG_PATH="/path_to_your_configs"
gpmq-worker gpmq.subscribers.notifier
```

Once started, the worker runs continuously. Press `Ctrl+C` to stop.
