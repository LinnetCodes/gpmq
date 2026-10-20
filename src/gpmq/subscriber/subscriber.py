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

if TYPE_CHECKING:
    from gpmq.config import SubscriberConfig
    from gpmq.storage.redis_adapter import RedisAdapter
    from gpmq.subscriber.worker_manager import WorkerManager


# gpclog does not ship py.typed yet (PEP 561), so its logger type resolves to
# Any; annotate the module-global logger as Any until it does.
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
