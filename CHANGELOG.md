# Changelog

All notable changes to **GPMQ** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.5.0]: https://github.com/LinnetCodes/gpmq/compare/version-0.4.0...HEAD
[0.4.0]: https://github.com/LinnetCodes/gpmq/releases/tag/version-0.4.0
