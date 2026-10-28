# Changelog

All notable changes to **GPMQ** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.7.1] - 2026-10-28

### Added

- **Worker live-status fields ride the existing worker heartbeat
  (R-001, zero extra round trips)**: `messages_processed`,
  `last_activity`, and the renamed `last_message_id` (was
  `current_message_id`, never populated) are maintained in-process and
  flushed to Redis with the heartbeat — no per-message writes.
  `WorkerManager.get_worker_status()` now assembles them from the
  registry (values can be a few seconds stale), and `last_activity` is
  the real last-completed-message time instead of the query time. A
  message currently stuck mid-processing is located via the consumer's
  PEL (XPENDING), not via `last_message_id`.
  - worker 实时状态字段随现有心跳搭车写入 Redis（R-001，零新增往返）：
    `messages_processed`、`last_activity` 与更名后的 `last_message_id`
    （原 `current_message_id`，从未被写入）在进程内维护、随心跳
    flush——不逐消息写 Redis。`WorkerManager.get_worker_status()` 从
    registry 组装（数据可能数秒陈旧），`last_activity` 为真实的最后
    完成消息时刻而非查询时刻。卡在处理中的消息通过消费者 PEL
    （XPENDING）定位，而非 `last_message_id`。

### Changed

- **Worker heartbeat throttle tightened from a hardcoded 5s to a named
  4s constant** (`HEARTBEAT_THROTTLE_SECONDS`): the worst-case gap
  between heartbeats (throttle + 5s XREADGROUP block) drops from ~10s
  to ~9s, keeping the minimum-config stale threshold
  (`heartbeat_interval=5` → 15s) a ~1.7x margin. Idle steady-state
  heartbeat write frequency is unchanged.
  - worker 心跳节流由硬编码 5s 收紧为具名常量 4s
    （`HEARTBEAT_THROTTLE_SECONDS`）：最坏心跳间隔（节流 + 5s
    XREADGROUP 阻塞）从 ~10s 降至 ~9s，最小配置下的僵死判定阈值
    （`heartbeat_interval=5` → 15s）裕量升至约 1.7x；空闲稳态心跳写入
    频率不变。

---

## [0.7.0] - 2026-10-27

### Fixed

- **Idle workers no longer stall in transparent retry loops under
  redis-py ≥ 6** (any install resolving `redis>=5.0.0` to 6.x/8.x).
  redis-py 6.0 changed its defaults to `socket_timeout=5s` with 10
  built-in retries; gpmq never passed `socket_timeout` explicitly, so the
  client timeout raced — and by network RTT always lost to — the server's
  5s `XREADGROUP BLOCK 5000` nil reply. Every idle read was then
  transparently re-issued, stretching empty reads from 5s to 10-60s and
  eventually surfacing `TimeoutError` after retries exhausted. The
  connection now sets `socket_timeout` explicitly from the new
  `redis_socket_timeout` config field (default `10.0` = 5s block + 5s
  margin), per redis-py's official guidance that socket_timeout must
  exceed the blocking command duration. Real hangs (dead server /
  half-open connection) are still detected within the bounded timeout.
  - 修复 redis-py ≥ 6 下空闲 worker 陷入透明重试循环的问题（任何把
    `redis>=5.0.0` 解析到 6.x/8.x 的安装都受影响）。redis-py 6.0 起默认
    `socket_timeout=5s` 且内置 10 次重试；gpmq 此前未显式传
    `socket_timeout`，客户端超时与服务端 5s 的 `XREADGROUP BLOCK 5000`
    空回复竞态——受网络往返影响几乎必然先触发——随后整条阻塞命令被
    透明重发，空读从 5s 拖长到 10-60s，重试用尽后抛出 `TimeoutError`。
    现在连接显式从新增的 `redis_socket_timeout` 配置字段取值（默认
    `10.0` = 5s 阻塞 + 5s 余量），遵循 redis-py 官方指引"socket_timeout
    必须大于阻塞命令时长"。真实挂死（服务端僵死/半开连接）仍在有界
    超时内被检出。

### Added

- **`redis_socket_timeout` config field** (`GPMQConfig`, default `10.0`,
  must be `> 5.0`): seconds before the Redis client gives up on a socket
  read. Zero-migration — existing `common.yaml` files without the key
  automatically get the fixed default; special deployments can override
  it via `gpmq/common.yaml`.
  - 新增 `redis_socket_timeout` 配置字段（`GPMQConfig`，默认 `10.0`，必须
    `> 5.0`）：Redis 客户端单次 socket 读的超时秒数。零迁移——存量
    `common.yaml` 无此键即自动获得修复后的默认值；特殊部署可经
    `gpmq/common.yaml` 覆写。

---

## [0.6.0] - 2026-10-23

### ⚠️ Breaking Changes

- **Dependency constraint `gpconfig>=0.3.0` → `gpconfig>=0.5.1`** (`pyproject.toml`).
  gpconfig 0.4.0 made `get_config()` addressing dot-notation-only (literal
  `/` or `\` raise `IllegalPathError`), and 0.5.1 ships the `py.typed`
  marker this release consumes for strict typing. Environments resolving
  gpconfig 0.3.x / 0.4.x must upgrade alongside gpmq.
  - 依赖约束 `gpconfig>=0.3.0` 提升为 `gpconfig>=0.5.1`。gpconfig 0.4.0 起
    `get_config()` 寻址只接受点号路径（字面 `/`、`\` 会抛 `IllegalPathError`），
    0.5.1 起随包发布本版本严格类型检查所依赖的 `py.typed`。解析到
    gpconfig 0.3.x / 0.4.x 的环境需要随 gpmq 一并升级。

### Fixed

- **`get_subscriber()` / `get_client()` / the CLI crashed under gpconfig ≥ 0.4**
  with `IllegalPathError` before doing any work: gpmq fed its slash-style
  `default_cfg_path` straight into the now dot-only
  `GPConfigManager.get_config()`. `default_cfg_path` itself must stay
  slash-style — gpconfig ≥ 0.4 validates at class-definition time that it
  contains no `.` (it is a save-contract folder path) — so addressing now
  goes through a new derived accessor `GPMQConfig.dotted_default_cfg_path()`
  (folder path → dot cfg path, e.g. `"gpmq/common"` → `"gpmq.common"`; also
  drops empty segments and normalizes `\`), used by `get_subscriber()`,
  `get_client()` and both CLI entry points. Regression tests now exercise
  the real `GPConfigManager` end-to-end.
  - 修复 gpconfig ≥ 0.4 下 `get_subscriber()` / `get_client()` / CLI 启动即
    崩溃（`IllegalPathError`）的问题：gpmq 此前把斜杠形式的
    `default_cfg_path` 直接传给了只认点号的 `GPConfigManager.get_config()`。
    `default_cfg_path` 本身必须保持斜杠形式（gpconfig ≥ 0.4 在类定义期校验其
    不含 `.`——它是保存契约的文件夹路径），因此寻址改经新增的派生访问器
    `GPMQConfig.dotted_default_cfg_path()`（文件夹路径 → 点号 cfg 路径，如
    `"gpmq/common"` → `"gpmq.common"`，同时过滤空段并归一化 `\`）。回归
    测试改为驱动真实 `GPConfigManager` 端到端验证。
- **Anonymous-client `publish()` without an explicit `timeout`** no longer
  hits a latent `AttributeError` (`publish_timeout` only exists on
  `PublisherConfig`); it now means "no limit", matching the documented
  default. Surfaced — and fixed — as part of making the source
  `mypy --strict` clean under typed gpconfig.
  - 匿名 client 调用 `publish()` 且不传 `timeout` 时不再触发潜在的
    `AttributeError`（`publish_timeout` 仅存在于 `PublisherConfig`）；现在
    语义为"不限时"，与文档默认值一致。该问题在 gpconfig 提供 `py.typed`
    后做 `mypy --strict` 清理时暴露并修复。
- **`gpmq.log` no longer installs foreign objects into gpclog's
  process-global config.** The result of `get_config("logs")` is now
  type-checked: only a real `GPConfigFolder` is installed; anything else
  (a raw dict when `logs` is an unregistered yaml file, or a `MagicMock`
  from a spec-mock config manager in tests) takes the existing
  "no logger config" warning path instead of breaking every subsequent
  `gpclog.get_logger()` call with an uncached name. This also removes
  order-dependent failures when running test files in isolation.
  - `gpmq.log` 不再把未验证类型的对象装入 gpclog 的进程级全局配置。
    `get_config("logs")` 的返回值现在会做类型检查：只有真正的
    `GPConfigFolder` 才会被安装；其余情况（`logs` 是未注册 yaml 文件时
    返回的裸 dict，或测试中 spec-mock 配置管理器返回的 `MagicMock`）走
    既有的"无日志配置"告警路径，而不再让后续所有未缓存的
    `gpclog.get_logger()` 调用报错。同时消除了单文件运行测试时的
    顺序依赖失败。

### Changed

- Dropped the transitional mypy overrides for `gpconfig` (it ships
  `py.typed` since 0.5.1, now our dependency floor); the `gpclog` override
  remains until it ships its own `py.typed`.
  - 移除了针对 `gpconfig` 的过渡性 mypy override（gpconfig 0.5.1 起随包
    发布 `py.typed`，且已是本包依赖地板）；`gpclog` 的 override 保留至
    其发布自己的 `py.typed`。

---

## [0.5.1] - 2026-10-20

### Added

- **Ship the `py.typed` marker (PEP 561).** The wheel now includes
  `gpmq/py.typed`, so downstream type checkers (e.g. `mypy --strict`) use
  gpmq's inline annotations instead of falling back to `Any` (previously:
  `import-untyped` on `from gpmq import ...`). This also makes the package
  consistent with the `Typing :: Typed` trove classifier it already declared.
  - 随包发布 `py.typed` 标记（PEP 561）。下游类型检查器（如 `mypy --strict`）
    现在会使用 gpmq 的内联类型注解，而不再整体退化为 `Any`（此前
    `from gpmq import ...` 会报 `import-untyped`）。同时使包与已声明的
    `Typing :: Typed` trove 分类器保持一致。
- **`mypy --strict` clean source tree + project mypy config.** Added
  `[tool.mypy]` (strict, with the official `pydantic.mypy` plugin) and
  `mypy` to the `dev` extra; all 27 source modules now pass `uv run mypy
  src/gpmq` (previously 138 strict errors in 17 files). Most fixes are
  annotation-only (parametrized `dict`/`Callable` generics, `TypeVar`
  identity decorators, precise return types); the few behavior-visible
  changes: Redis/AuditStore operations before `connect()` now raise a clear
  `RedisConnectionError` / `sqlite3.ProgrammingError` instead of
  `AttributeError`, and `RedisAdapter.get_message(timeout=None)` is
  documented correctly as "block until a message arrives" (redis-py
  semantics) instead of the previous wrong "non-blocking" claim.
  Transitional per-module overrides keep `gpconfig`/`gpclog` imports (and
  `GPMQConfig` subclassing) working until those packages ship their own
  `py.typed` — remove them when they do (see
  `dev_docs/issues/py-typed.md`).
  - 源码达到 `mypy --strict` 零错误并新增项目级 mypy 配置
    （`[tool.mypy]`：strict + 官方 `pydantic.mypy` 插件；`mypy` 进入 `dev`
    依赖组）。此前 17 个文件共 138 个 strict 错误，现已全部修复。绝大多数
    修复仅涉及注解（`dict`/`Callable` 泛型参数化、`TypeVar` 恒等装饰器、
    精确返回类型）；少数可见的行为变化：未 `connect()` 就调用
    Redis/AuditStore 操作时改为抛出语义明确的 `RedisConnectionError` /
    `sqlite3.ProgrammingError`（原先为 `AttributeError`）；
    `RedisAdapter.get_message(timeout=None)` 的文档更正为"阻塞直到消息
    到达"（redis-py 语义），修正了原先错误的"非阻塞"描述。
    `gpconfig`/`gpclog` 相关的过渡性 per-module override 将在这两个包
    发布自己的 `py.typed` 后移除（见 `dev_docs/issues/py-typed.md`）。

---

## [0.5.0] - 2026-07-07

> 跟随依赖 **gpclog 0.3.0** 的破坏性变更（`log_path` 字段重命名为 `log_dir`）。
> GPMQ 自身源码无需改动，但示例日志配置和依赖约束需要同步更新。请阅读下方的
> **Breaking Changes**。
>
> Tracks the **breaking rename** of the `log_path` field to `log_dir` in the
> dependency **gpclog 0.3.0**. No GPMQ source code change was required, but the
> example log configs and the dependency constraint had to be updated in lockstep.
> See **Breaking Changes** below.

### ⚠️ Breaking Changes

- **`log_path` → `log_dir` in example log YAML configs.** All 12 files under
  `examples/cfgs_{linux,windows}/logs/*.yaml` used the old `log_path:` field,
  which gpclog 0.3.0 no longer recognizes (it now reads `log_dir`). Under
  gpclog 0.3.0's stricter validation these configs would silently fall back to
  defaults instead of honoring the configured directory. If you copied these
  example configs, rename `log_path:` to `log_dir:` in your own copies too.
  - **示例日志配置中 `log_path` 改为 `log_dir`**。`examples/cfgs_{linux,windows}/logs/`
    下的 12 个 YAML 全部使用了 gpclog 0.3.0 已废弃的 `log_path:` 字段。若你复制过这些
    配置，请同样把 `log_path:` 改成 `log_dir:`。

### Changed

- **Dependency constraint `gpclog>=0.2.0` → `gpclog>=0.3.0`** (`pyproject.toml`).
  GPMQ now requires gpclog 0.3.0 or newer; 0.2.0 is no longer supported.
  - 依赖约束 `gpclog>=0.2.0` 提升为 `gpclog>=0.3.0`，不再支持 0.2.0。

### Internal / Infrastructure

- Example log YAML comments updated to list gpclog 0.3.0's new `"cwd"` path mode
  alongside the existing `auto | env | home | 绝对路径`.
- `uv.lock` re-resolved against gpclog 0.3.0.

---

## [0.4.0] - 2026-07-06

Initial PyPI release of GPMQ (General Purpose Message Queue) — a lightweight
Python distributed message queue built on Redis Streams for personal projects
and MVPs.

### Highlights

- **Publisher/subscriber pipeline on Redis Streams**: publishers `xadd` messages
  that active subscribers consume via consumer groups; results flow back over
  Pub/Sub with timeout-based `MISSING`/`TIMEOUT` handling.
- **Multiprocess workers** spawned via `WorkerManager`, each running its own
  `XREADGROUP` loop with per-worker `sn`-isolated loggers (spawn start method
  enforced by `use_spawn()`).
- **`@message_handler` decorator + signature validation**: handlers validated at
  import time (1 or 2 params; `msg`/`context` naming); `ProcessResult` /
  `ProcessStatus` / `None` return-value semantics.
- **`RedisAdapter`** wrapping every Redis op in retry/backoff, and **`AuditStore`**
  (SQLite) persisting messages + results.
- **`gpconfig`-driven two-layer config merge** and **`gpclog`** category logging.
- **CLI**: `gpmq status|audit ...` and `gpmq-worker <module>` entry points.

GPMQ 的首个 PyPI 发布版本（基于 Redis Streams 的轻量级 Python 分布式消息队列）。

---

[0.7.0]: https://github.com/LinnetCodes/gpmq/compare/version-0.6.0...version-0.7.0
[0.6.0]: https://github.com/LinnetCodes/gpmq/compare/version-0.5.1...version-0.6.0
[0.5.0]: https://github.com/LinnetCodes/gpmq/compare/version-0.4.0...HEAD
[0.4.0]: https://github.com/LinnetCodes/gpmq/releases/tag/version-0.4.0
