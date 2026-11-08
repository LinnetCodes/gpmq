"""Subscriber lifecycle management."""

from __future__ import annotations

import importlib
import threading
import time
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from gpmq.exceptions import HandlerTypeMismatchError
from gpmq.log import get_logger
from gpmq.models.result import ProcessResult, ProcessStatus

if TYPE_CHECKING:
    from gpmq.config import SubscriberConfig
    from gpmq.storage.audit_store import AuditStore
    from gpmq.storage.redis_adapter import RedisAdapter
    from gpmq.subscriber.worker_manager import WorkerManager


# gpclog ships py.typed since 0.3.1; the module-global logger stays Any
# because it is re-initialized per Subscriber instance (same pattern as
# worker.py).
logger: Any = None


class Subscriber:
    """Manages a subscriber's lifecycle.

    Responsibilities:
    - Load and validate handler functions
    - Register/unregister in Redis
    - Create Redis consumer group
    - Maintain heartbeat via daemon thread
    - Start/stop WorkerManager
    """

    def __init__(self, config: SubscriberConfig) -> None:
        self._config = config
        self.name = config.name
        self.config = config
        self._redis: RedisAdapter | None = None
        self._worker_manager: WorkerManager | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()
        self._running = False
        self._next_trim_at = 0.0  # stream-retention trim throttle (epoch)
        self._audit: AuditStore | None = (
            None  # lazy: opened on the first abandoned PEL entry, closed in stop()
        )
        self._audit_lock = threading.Lock()  # guards lazy _audit open/close
        self._worker_ctx: dict[str, Any] = (
            dict()
        )  # worker context data that will be passed to worker processes

        global logger
        logger = get_logger(self.config.log_category)

    def apply_worker_context(self, **kwargs: Any) -> "Subscriber":
        """
        apply_worker_context() method is called to apply worker context to subscriber before calling Subscriber.start() method.
        This can be useful to pass parameters and configuration to worker processes of the subscriber.
        Data been applied will be visible in 'worker_context_processor' methods and 'message_handler' methods.
        This method can be called multiple times before calling Subscriber.start().
        """
        self._worker_ctx.update(kwargs)
        return self

    def start(self) -> None:
        """Start the subscriber."""
        from gpmq.storage.redis_adapter import RedisAdapter
        from gpmq.subscriber.handler import HandlerRegistry
        from gpmq.subscriber.worker_manager import WorkerManager

        try:
            # Connect to Redis
            self._redis = RedisAdapter(self._config)
            self._redis.connect()

            # Load handler functions
            loaded_handlers: list[tuple[Any, Callable[..., Any]]] = []
            for handler_cfg in self._config.handlers:
                func = HandlerRegistry.load_handler(handler_cfg.handler)
                loaded_handlers.append((handler_cfg, func))

            # Validate: config message_types must be subset of decorator-declared types
            self._validate_handler_types(loaded_handlers)

            # Register subscriber in Redis
            subscriber_info = {
                "message_types": self._config.get_message_types(),
                "workers": self._config.workers,
                "stream_name": self._config.stream_name,
            }
            self._redis.register_subscriber(self.name, subscriber_info)

            # Create consumer group
            self._redis.xgroup_create(self._config.stream_name, self.name)

            # Start heartbeat daemon thread
            self._heartbeat_stop.clear()
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name=f"{self.name}-heartbeat",
            )
            self._heartbeat_thread.start()

            # Start worker manager
            config_dict = self._config.model_dump()
            worker_ctx = dict(self._worker_ctx)
            worker_ctx["subscribe_message_types"] = self._config.get_message_types()

            self._worker_manager = WorkerManager(
                config_dict, worker_ctx, redis_conn=self._redis
            )
            self._worker_manager.start()
            self._running = True

            logger.info(
                "Subscriber {} started with {} workers", self.name, self._config.workers
            )
        except Exception:
            # Roll back any completed steps; never mask the original error.
            try:
                self.stop()
            except Exception:
                logger.error(
                    "Error rolling back after failed start of subscriber {}\n{}",
                    self.name,
                    traceback.format_exc(),
                )
            raise

    def stop(self) -> None:
        """Stop the subscriber."""
        self._running = False

        if self._worker_manager:
            self._worker_manager.stop(timeout=self._config.subscriber_timeout)
            self._worker_manager = None

        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=10)
        self._heartbeat_thread = None

        # Best-effort orphan sweep BEFORE the final trim: an abandoned
        # (xack'd) entry no longer pins the safe point, so this can let the
        # final trim actually advance. Independent of the trim flags —
        # abandoning is its own mechanism, gated only by
        # pel_abandon_idle_seconds and the adapter still being set. A down
        # Redis pays one retry cycle inside the scan (swallowed there),
        # exactly like the heartbeat write already does.
        self._maybe_abandon_pending()

        # Final retention trim when this is the last registered subscriber.
        # Runs before unregistering so our own group's cursor/PEL still
        # constrains the safe trim point. Heartbeat join may have timed out
        # (thread still alive) — concurrent XTRIM is idempotent, safe.
        # Config flags first (free), ping() last: it bypasses the adapter's
        # retry/backoff, so with Redis down the trim is skipped instead of
        # paying a full retry cycle on the registry read.
        if (
            self._redis
            and self._config.stream_trim_interval > 0
            and self._config.stream_trim_on_last_stop
            and self._redis.ping()
        ):
            try:
                registered = set(self._redis.get_registered_subscribers())
                if registered <= {self.name}:
                    trimmed = self._redis.trim_stream_to_registered_progress()
                    if trimmed:
                        logger.info(
                            "Final stream trim on stop removed {} entries",
                            trimmed,
                        )
            except Exception:
                logger.error(
                    "Final stream trim failed for {}\n{}",
                    self.name,
                    traceback.format_exc(),
                )

        with self._audit_lock:
            if self._audit is not None:
                try:
                    self._audit.close()
                except Exception:
                    logger.error(
                        "Error closing subscriber audit store for {}\n{}",
                        self.name,
                        traceback.format_exc(),
                    )
                self._audit = None

        if self._redis:
            self._redis.unregister_subscriber(self.name)
            self._redis.disconnect()
            self._redis = None

        logger.info("Subscriber {} stopped", self.name)

    def run_forever(self) -> None:
        """Start subscriber and block until interrupted.

        Starts the subscriber, then enters a blocking loop. If a
        ``run_forever_method`` is configured in SubscriberConfig, that function
        is called instead of the default sleep loop.

        Handles KeyboardInterrupt and SystemExit to ensure stop() is called.
        """
        try:
            logger.info(f"Starting subscriber '{self.name}' ...")
            self.start()

            if self._config.run_forever_method:
                self._run_custom_forever()
            else:
                while True:
                    time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info(f"\nStopping subscriber '{self.name}' ...")
        finally:
            try:
                self.stop()
            except Exception:
                logger.error(
                    "Error during run_forever teardown of subscriber {}\n{}",
                    self.name,
                    traceback.format_exc(),
                )

    def _run_custom_forever(self) -> None:
        """Load and invoke the configured run_forever_method function."""
        import_path = self._config.run_forever_method
        if import_path is None:
            # Unreachable via run_forever() (guarded there); kept as a guard.
            raise RuntimeError("run_forever_method is not configured")
        parts = import_path.rsplit(".", 1)
        if len(parts) == 1:
            raise ImportError(f"Invalid run_forever_method import path: {import_path}")
        module_path, func_name = parts
        module = importlib.import_module(module_path)
        func = getattr(module, func_name)

        if not getattr(func, "_gpmq_run_forever", False):
            raise TypeError(
                f"run_forever_method '{import_path}' must be decorated with "
                f"@run_forever_method"
            )

        func(subscriber=self, paras=self._config.run_forever_method_paras)

    def get_status(self) -> dict[str, Any]:
        """Return subscriber status information."""
        worker_statuses: list[dict[str, Any]] = []
        if self._worker_manager:
            worker_statuses = [
                ws.model_dump() for ws in self._worker_manager.get_worker_status()
            ]

        return {
            "name": self.name,
            "running": self._running,
            "workers": self._config.workers,
            "message_types": self._config.get_message_types(),
            "worker_statuses": worker_statuses,
        }

    def _validate_handler_types(
        self, loaded_handlers: list[tuple[Any, Callable[..., Any]]]
    ) -> None:
        """Validate each handler's config message_types is a subset of its
        decorator-declared types (per-handler, not set-union).

        Args:
            loaded_handlers: list of (handler_cfg, func) pairs from start().

        Raises:
            HandlerTypeMismatchError: if any handler config declares a type
                its decorator did not register.
        """
        for handler_cfg, func in loaded_handlers:
            declared = set(getattr(func, "_gpmq_message_types", []))
            configured = set(handler_cfg.message_types)
            over_claimed = configured - declared
            if over_claimed:
                raise HandlerTypeMismatchError(
                    handler_path=handler_cfg.handler,
                    declared_types=list(declared),
                    config_types=list(configured),
                    conflict_types=list(over_claimed),
                )

    def _heartbeat_loop(self) -> None:
        """Periodically update heartbeat in Redis."""
        interval = self._config.heartbeat_interval
        while not self._heartbeat_stop.wait(timeout=interval):
            try:
                if self._redis:
                    self._redis.update_heartbeat(self.name)
            except Exception:
                logger.error(
                    "Heartbeat update failed for {}\n{}",
                    self.name,
                    traceback.format_exc(),
                )
            self._maybe_abandon_pending()
            self._maybe_trim()

    def _maybe_trim(self) -> None:
        """Run the throttled stream-retention trim (heartbeat thread).

        Trim failures are logged and never propagated — retention must not
        jeopardize the heartbeat loop.
        """
        interval = self._config.stream_trim_interval
        if interval <= 0 or not self._redis:
            return
        now = time.time()
        if now < self._next_trim_at:
            return
        self._next_trim_at = now + interval
        try:
            trimmed = self._redis.trim_stream_to_registered_progress()
            if trimmed:
                logger.info("Stream retention trim removed {} entries", trimmed)
        except Exception:
            logger.error(
                "Stream retention trim failed for {}\n{}",
                self.name,
                traceback.format_exc(),
            )

    def _maybe_abandon_pending(self) -> None:
        """Abandon orphaned PEL entries idle beyond the configured threshold.

        A worker that dies between XREADGROUP and XACK leaves its entry in
        the group's PEL forever: besides losing the message, the entry
        pins the stream-retention safe point. Each heartbeat round, scan
        the group's PEL (server-side idle filter) and abandon every
        long-idle entry — see :meth:`_abandon_entry`. Nothing is ever
        redelivered: "interrupted mid-processing" is a terminal failure
        (at-most-once processing; see the design doc for the ruling).

        Failures are logged and never propagated — recovery must not
        jeopardize the heartbeat loop.
        """
        idle_seconds = self._config.pel_abandon_idle_seconds
        if idle_seconds <= 0 or not self._redis:
            return
        try:
            entries = self._redis.xpending_range(
                self._config.stream_name,
                self.name,
                count=100,
                idle_ms=int(idle_seconds * 1000),
            )
            for entry in entries:
                self._abandon_entry(entry)
        except Exception:
            logger.error(
                "Orphan PEL scan failed for {}\n{}",
                self.name,
                traceback.format_exc(),
            )

    def _abandon_entry(self, entry: dict[str, Any]) -> None:
        """Abandon one idle PEL entry: audit ABANDONED, XACK, warn.

        Per-entry order is audit-then-xack: unblocking the trim point
        outranks observability, so an audit failure is logged but the
        XACK still runs. Both steps are idempotent on re-sweep (the
        audit ladder ignores ABANDONED-over-ABANDONED; XACK on a cleared
        entry is a no-op).

        Args:
            entry: One ``xpending_range`` dict (message_id, consumer,
                time_since_delivered ms, times_delivered).
        """
        redis = self._redis
        assert redis is not None  # caller (_maybe_abandon_pending) guards this
        message_id = entry["message_id"]
        idle_s = entry.get("time_since_delivered", 0) / 1000.0

        # Log-only detail read; the fields may already be gone (manual
        # trim) — the abandon itself must not depend on it.
        detail = ""
        try:
            rows = redis.xrange(
                self._config.stream_name, start=message_id, end=message_id
            )
            if rows:
                fields = rows[0][1]
                detail = (
                    f", type={fields.get('type', '?')}, "
                    f"publisher={fields.get('publisher', '?')}"
                )
        except Exception:
            logger.error(
                "Failed to read fields of pending entry {} for abandon log\n{}",
                message_id,
                traceback.format_exc(),
            )

        process_result = ProcessResult(
            message_id=message_id,
            status=ProcessStatus.ABANDONED,
            subscriber_name=self.name,
            processing_time=0.0,
            error_message=(
                f"abandoned: idle {idle_s:.1f}s in PEL "
                f"(consumer={entry.get('consumer', '?')}, "
                f"deliveries={entry.get('times_delivered', '?')})"
            ),
        )
        try:
            if self._config.enable_audit:
                self._get_audit().save_result(message_id, process_result)
        except Exception:
            logger.error(
                "Failed to audit ABANDONED result for {} (xack still runs)\n{}",
                message_id,
                traceback.format_exc(),
            )

        redis.xack(self._config.stream_name, self.name, message_id)
        logger.warning(
            "Abandoned orphan PEL entry {} of {} (idle {:.1f}s, "
            "deliveries {}){} — processing was interrupted, not redelivered",
            message_id,
            self.name,
            idle_s,
            entry.get("times_delivered", "?"),
            detail,
        )

    def _get_audit(self) -> "AuditStore":
        """Return the subscriber-process audit store, connecting lazily.

        Opened on the first abandoned entry (not on start) so healthy
        operation costs nothing; closed in :meth:`stop`. Cached only after
        a successful connect: a failed connect is retried (and logged by
        the caller) on the next abandoned entry, instead of caching a dead
        store whose ``save_result`` would silently no-op from then on.
        The lazy open/close is guarded by ``_audit_lock``, so the stop-path
        sweep and an un-joined heartbeat thread cannot open two stores.

        Returns:
            The connected :class:`AuditStore` for this process.

        Raises:
            Whatever :meth:`AuditStore.connect` raises (expectedly
                ``sqlite3.Error``); nothing is cached and the per-entry
                caller logs the error and continues.
        """
        if self._audit is None:
            with self._audit_lock:
                if self._audit is None:
                    from gpmq.storage.audit_store import AuditStore

                    store = AuditStore(self._config.audit_db_path)
                    store.connect()
                    self._audit = store
        return self._audit
