# Changelog

All notable changes to **GPMQ** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
