"""Worker process for message consumption."""

from __future__ import annotations

import importlib
import json
import multiprocessing
import time
import traceback
import inspect
from datetime import datetime
from collections.abc import Callable
from multiprocessing import Process
from typing import Any, Optional, TYPE_CHECKING

from gpconfig import GPConfigManager

from gpmq.log import get_logger
from gpmq.config import set_config_manager
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus

if TYPE_CHECKING:
    # The real Event type; module-level ``multiprocessing.Event`` is a factory
    # function bound to the default context, not a class.
    from multiprocessing.synchronize import Event as SyncEvent

# gpclog does not ship py.typed yet (PEP 561), so its logger type resolves to
# Any; annotate the module-global logger as Any until it does.
logger: Any = None


class Worker(Process):
    """Worker process that consumes messages from a Redis consumer group.

    Each Worker runs as an independent ``multiprocessing.Process`` with its
    own Redis and AuditStore connections.  It reads messages via
    ``XREADGROUP``, dispatches them to the appropriate handler, and publishes
    processing results.

    Args:
        subscriber_name: Name of the subscriber (also the consumer group).
        stream_name: Redis Stream to read from.
        message_types: List of message types this subscriber handles.
        worker_id: Unique identifier for this worker (e.g. ``"worker-0"``).
        config_dict: Serialised configuration dict (avoids sharing objects
            across processes).
        context: context data been passed to worker processes that help initialize worker environment.
        stop_event: ``multiprocessing.Event`` used to signal graceful
            shutdown.  ``None`` is accepted for unit-testing convenience.
    """

    def __init__(
        self,
        subscriber_name: str,
        stream_name: str,
        message_types: list[str],
        worker_id: str,
        hostname: str,
        config_dict: dict[str, Any],
        context: Optional[dict[str, Any]] = None,
        stop_event: Optional["SyncEvent"] = None,
    ) -> None:
        super().__init__()
        self._subscriber_name = subscriber_name
        self._stream_name = stream_name
        self._message_types = set(message_types)
        self._worker_id = worker_id
        self._hostname = hostname
        self._config_dict = dict(config_dict)
        self._worker_ctx = dict() if context is None else context
        self._ctx_proc_func: Callable[[dict[str, Any]], dict[str, Any]] | None = None
        self._stop_event = (
            stop_event if stop_event is not None else multiprocessing.Event()
        )

        self._cfg_mgr: GPConfigManager | None = None
        self._key_prefix: str = self._config_dict.get("registry_key_prefix", "gpmq:")
        self._read_batch_size: int = self._config_dict.get("worker_read_batch_size", 1)

        # Set by ``run()`` or injected in tests
        self._redis: Any = None
        self._audit: Any = None
        self._publisher_client: Any = None

        # internal heartbeat record
        self._last_heartbeat: float = 0.0

        global logger
        _sn = self._parse_worker_sn()
        logger = get_logger(self._config_dict["log_category"], _sn)

    # ------------------------------------------------------------------
    # Public helpers (used by tests)
    # ------------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        """Unique identifier for this worker."""
        return self._worker_id

    @property
    def consumer_name(self) -> str:
        """Redis consumer name used in XREADGROUP."""
        return f"{self._subscriber_name}:{self._worker_id}"

    def _parse_worker_sn(self) -> int:
        """Extract the numeric sequence number from ``worker_id``.

        The ``worker_id`` format is ``{name}-{hostname}-worker-{n}`` so we
        split on ``"-worker-"`` and take the part after it.
        """
        _suffix = self._worker_id.rsplit("-worker-", 1)[-1]
        return int(_suffix)

    def prepare_worker_context(self, context: dict[str, Any]) -> None:
        if self._cfg_mgr is None:
            _cfg_mgr_path = None
            _prj_name = self._config_dict["name"]

            _need_cfg_mgr = False
            if "config_manager_folder_path" in context:
                _cfg_mgr_path = context["config_manager_folder_path"]
                _need_cfg_mgr = True
            if "project_name" in context:
                _prj_name = context["project_name"]
                _need_cfg_mgr = True

            if _need_cfg_mgr:  # load project config manager
                self._cfg_mgr = GPConfigManager(_prj_name, _cfg_mgr_path)

        _name = self._config_dict.get("name", "subscriber")
        _sn = self._parse_worker_sn()

        self._worker_ctx["worker_id"] = self._worker_id
        self._worker_ctx["worker_sn"] = _sn
        self._worker_ctx["subscriber_name"] = _name

        if self._cfg_mgr:
            self._worker_ctx["config_manager"] = self._cfg_mgr
            set_config_manager(self._cfg_mgr)

        # reload logger in working process
        global logger
        logger = get_logger(self._config_dict["log_category"], _sn)
        self._worker_ctx["logger"] = logger

        # Create unconnected GPMQClient for embedded publisher
        if "publisher_cfg_path" in context:
            from gpmq.config import get_client

            client = get_client(context["publisher_cfg_path"])
            self._worker_ctx["publisher"] = client
            self._publisher_client = client

        # Load user-defined worker context processor if configured
        if "worker_context_processor" in context:
            _path = context.pop("worker_context_processor")
            parts = _path.rsplit(".", 1)
            if len(parts) == 1:
                raise ImportError(
                    f"Invalid worker_context_processor import path: {_path}"
                )
            module_path, func_name = parts
            module = importlib.import_module(module_path)
            _func = getattr(module, func_name)
            if not getattr(_func, "_gpmq_ctx_proc", False):
                raise TypeError(
                    f"worker context processor '{_path}' must be decorated with "
                    f"@worker_context_processor"
                )
            self._ctx_proc_func = _func

        # remove context items that no longer needed
        if "config_manager_folder_path" in self._worker_ctx:
            del self._worker_ctx["config_manager_folder_path"]
        if "publisher_cfg_path" in self._worker_ctx:
            del self._worker_ctx["publisher_cfg_path"]

        # additional worker context processing requested by configuration
        if self._ctx_proc_func:
            _in_ctx = dict(self._worker_ctx)
            _out_ctx = self._ctx_proc_func(_in_ctx)
            self._worker_ctx.update(_out_ctx)

        return

    # ------------------------------------------------------------------
    # multiprocessing.Process entry point
    # ------------------------------------------------------------------

    def run(self) -> None:  # noqa: C901  (complexity acceptable for a process loop)
        """Main loop executed in the child process."""
        from gpmq.config import SubscriberConfig
        from gpmq.storage.audit_store import AuditStore
        from gpmq.storage.redis_adapter import RedisAdapter
        from gpmq.subscriber.handler import HandlerRegistry

        self.prepare_worker_context(self._worker_ctx)

        logger.debug(f"Worker context: \n {self._worker_ctx}")
        logger.debug(f"Worker configuration: \n{self._config_dict}")

        config = SubscriberConfig(**self._config_dict)
        # Load handler functions
        for handler_cfg in config.handlers:
            HandlerRegistry.load_handler(handler_cfg.handler)

        try:
            self._redis = RedisAdapter(config)
            self._redis.connect()
            if config.enable_audit:
                self._audit = AuditStore(config.audit_db_path)
                self._audit.connect()
            else:
                self._audit = None

            logger.info(
                "Worker {} started for subscriber {} on stream {}",
                self._worker_id,
                self._subscriber_name,
                self._stream_name,
            )

            # Register this worker in the consumer group registry
            self._register_self()

            while not self._stop_event.is_set():
                self._read_and_process()

        except (KeyboardInterrupt, SystemExit):
            logger.info("Worker {} interrupted, shutting down...", self._worker_id)
        except Exception:
            logger.error(
                "Worker {} crashed\n{}", self._worker_id, traceback.format_exc()
            )
        finally:
            self._cleanup()

    # ------------------------------------------------------------------
    # Core message processing
    # ------------------------------------------------------------------

    def _read_and_process(self) -> None:
        """Read one batch of messages from the consumer group and process."""
        from gpmq.subscriber.handler import HandlerRegistry

        try:
            messages = self._redis.xreadgroup(
                group=self._subscriber_name,
                consumer=self.consumer_name,
                streams={self._stream_name: ">"},
                count=self._read_batch_size,
                block_ms=5000,
            )
            logger.debug(
                f"Read {len(messages)} messages from stream {self._stream_name}"
            )
        except Exception:
            logger.error(
                "XREADGROUP failed for worker {}, subscriber: {}, consumer: {}, stream: {} ",
                self._worker_id,
                self._subscriber_name,
                self.consumer_name,
                self._stream_name,
            )
            logger.error("trace back: \n{}", traceback.format_exc())
            return

        # Update heartbeat every loop iteration
        try:
            _now = time.time()
            if _now >= self._last_heartbeat + 5:
                self._redis.update_worker_heartbeat(
                    self._subscriber_name, self._worker_id
                )
                self._last_heartbeat = _now
        except Exception:
            logger.error(
                "Heartbeat update failed for worker {}\n{}",
                self._worker_id,
                traceback.format_exc(),
            )

        if not messages:
            return

        for _stream, entries in messages:
            for msg_id, fields in entries:
                msg_type = fields.get("type", "")
                handler = HandlerRegistry.get_handler(msg_type)
                if handler is None:
                    # Not our message type — ACK and skip
                    self._redis.xack(self._stream_name, self._subscriber_name, msg_id)
                    continue

                self._process_message(msg_id, fields, handler)

    def _handle_poison_message(
        self, msg_id: str, fields: dict[str, str], exc: BaseException
    ) -> None:
        """Handle an unparsable stream message without crashing the worker.

        Called when :meth:`_process_message` fails to parse ``fields`` into a
        :class:`Message` (bad JSON, missing/invalid timestamp, missing required
        fields, etc.). Builds a poison-message and an EXCEPTION result using
        ONLY the trusted ``msg_id`` and worker self-attributes (never the
        untrusted ``fields`` as a parsed object), attempts a single ACK, and
        publishes the result. Every step is wrapped so this method cannot
        itself raise.

        Args:
            msg_id: Trusted Redis stream ID of the broken message.
            fields: Raw broken fields (serialized into ``broken_msg``).
            exc: The exception that triggered poison handling.
        """
        try:
            try:
                broken_str = json.dumps(fields, ensure_ascii=False, default=str)
            except Exception:
                broken_str = str(fields)
            logger.error(
                "Worker {} received a malformed message id={}, treating as poison: {}\n{}",
                self._worker_id,
                msg_id,
                broken_str[:500],
                traceback.format_exc(),
            )

            process_result = ProcessResult(
                message_id=msg_id,
                status=ProcessStatus.EXCEPTION,
                subscriber_name=self._subscriber_name,
                processing_time=0.0,
                error_message=f"Malformed message: {exc}",
                error_traceback=traceback.format_exc(),
            )

            if self._audit is not None:
                try:
                    poison_message = Message(
                        id=msg_id,
                        type="__poison__",
                        timestamp=datetime.now(),
                        publisher="<unknown>",
                        payload={"broken_msg": broken_str},
                    )
                    self._audit.save_message(poison_message)
                    self._audit.save_result(msg_id, process_result)
                except Exception:
                    logger.error(
                        "Worker {} failed to audit poison message {}\n{}",
                        self._worker_id,
                        msg_id,
                        traceback.format_exc(),
                    )

            try:
                self._redis.xack(self._stream_name, self._subscriber_name, msg_id)
            except Exception:
                logger.error(
                    "Worker {} failed to ACK poison message {} (leaving it in PEL)\n{}",
                    self._worker_id,
                    msg_id,
                    traceback.format_exc(),
                )
                return

            self._publish_result(msg_id, process_result)
        except Exception:
            # Last-resort guard: poison handling must never crash the worker.
            logger.error(
                "Worker {} unexpected failure during poison handling for {}\n{}",
                self._worker_id,
                msg_id,
                traceback.format_exc(),
            )

    def _process_message(
        self,
        msg_id: str,
        fields: dict[str, str],
        handler: Callable[..., Any],
    ) -> None:
        """Process a single message through the handler pipeline.

        Steps:
        1. Parse the raw Redis Stream fields into a :class:`Message`.
        2. Filter by ``message_type`` — ACK and skip if not matching.
        3. Call *handler*, catching any exceptions.
        4. Build a :class:`ProcessResult` (SUCCESS / FAILURE / EXCEPTION).
        5. ``save_result`` to AuditStore.
        6. ``xack`` the message on the stream.
        7. ``publish`` the result on ``gpmq:result:{msg_id}:{subscriber_name}``.
        """
        # --- 1. Parse message (poison-safe: any failure -> _handle_poison_message) ---
        try:
            payload = fields.get("payload", "{}")
            if isinstance(payload, str):
                payload = json.loads(payload)

            target_raw = fields.get("target_subscribers")
            target_list = json.loads(target_raw) if target_raw else None

            # model_validate keeps pydantic's coercion semantics (e.g. ISO
            # timestamp strings) identical to keyword construction.
            message = Message.model_validate(
                {
                    "id": msg_id,
                    "type": fields.get("type", ""),
                    "timestamp": fields.get("timestamp", ""),
                    "publisher": fields.get("publisher", ""),
                    "payload": payload,
                    "correlation_id": fields.get("correlation_id", None),
                    "target_subscribers": target_list,
                }
            )
        except Exception as exc:
            self._handle_poison_message(msg_id, fields, exc)
            return

        logger.debug(
            f"subscriber '{self._subscriber_name}' received message from '{message.publisher}', "
            f"id: '{message.id}', type: '{message.type}', payload: '{message.payload}', "
            f"correlation_id: '{message.correlation_id}'"
        )

        # --- 2. Filter by message type ---
        if message.type not in self._message_types:
            self._redis.xack(self._stream_name, self._subscriber_name, msg_id)
            logger.debug(
                f"subscriber '{self._subscriber_name}' skipped message from '{message.publisher}', "
                f"id: '{message.id}'"
            )
            return

        # --- 2b. Filter by target subscribers (targeted publishing) ---
        if (
            message.target_subscribers is not None
            and self._subscriber_name not in message.target_subscribers
        ):
            self._redis.xack(self._stream_name, self._subscriber_name, msg_id)
            logger.debug(
                f"subscriber '{self._subscriber_name}' discarded message id '{message.id}': "
                f"not in target_subscribers {message.target_subscribers}"
            )
            return

        logger.debug(
            f"subscriber '{self._subscriber_name}' will process message from '{message.publisher}', "
            f"id: '{message.id}'"
        )
        if self._audit:
            self._audit.save_message(message)

        # --- 3. Call handler & measure time ---
        start = time.monotonic()
        try:
            sig = inspect.signature(handler)
            if len(sig.parameters) == 1:
                result = handler(message)  # do not need worker context
            elif len(sig.parameters) == 2:
                result = handler(message, self._worker_ctx)  # need worker context
            else:
                raise TypeError(
                    f"message handler only accept 1 or 2 parameters, got {len(sig.parameters)}"
                )
        except Exception as exc:
            elapsed = time.monotonic() - start
            tb_str = traceback.format_exc()
            process_result = ProcessResult(
                message_id=msg_id,
                status=ProcessStatus.EXCEPTION,
                subscriber_name=self._subscriber_name,
                processing_time=elapsed,
                error_message=str(exc),
                error_traceback=tb_str,
            )
            if self._audit:
                self._audit.save_result(msg_id, process_result)
            self._redis.xack(self._stream_name, self._subscriber_name, msg_id)
            self._publish_result(msg_id, process_result)
            logger.error(
                f"worker '{self._worker_id}' raised an exception when handling message: {msg_id}, "
                f"type: '{message.type}', from '{message.publisher}', payload: '{message.payload}'"
            )
            return

        elapsed = time.monotonic() - start

        # --- 4. Build ProcessResult ---
        if (
            result is ProcessStatus.FAILURE
        ):  # A failure defined in spec, but not an exception
            # message handler return ProcessStatus.FAILURE to explicitly declare a failure status defined in spec
            process_result = ProcessResult(
                message_id=msg_id,
                status=ProcessStatus.FAILURE,
                subscriber_name=self._subscriber_name,
                processing_time=elapsed,
            )
        elif isinstance(result, ProcessResult):
            # Ensure the message_id and processing_time are accurate
            process_result = ProcessResult(
                message_id=result.message_id or msg_id,
                status=result.status,
                subscriber_name=result.subscriber_name or self._subscriber_name,
                processing_time=result.processing_time or elapsed,
                data=result.data,
                error_message=result.error_message,
                error_traceback=result.error_traceback,
                timeout_seconds=result.timeout_seconds,
            )
        elif result is None:  # message handler succeed without a return value
            process_result = ProcessResult(
                message_id=msg_id,
                status=ProcessStatus.SUCCESS,
                subscriber_name=self._subscriber_name,
                processing_time=elapsed,
            )
        else:
            # Non-ProcessResult, non-None return — wrap as SUCCESS
            process_result = ProcessResult(
                message_id=msg_id,
                status=ProcessStatus.SUCCESS,
                subscriber_name=self._subscriber_name,
                processing_time=elapsed,
                data=result if isinstance(result, dict) else {"value": result},
            )

        # --- 5. Save to audit ---
        if self._audit:
            self._audit.save_result(msg_id, process_result)

        # --- 6. ACK the message ---
        self._redis.xack(self._stream_name, self._subscriber_name, msg_id)

        # --- 7. Publish result ---
        self._publish_result(msg_id, process_result)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _publish_result(self, msg_id: str, result: ProcessResult) -> None:
        """Publish processing result to the Pub/Sub channel."""
        channel = f"{self._key_prefix}result:{msg_id}:{self._subscriber_name}"
        self._redis.publish(channel, result.model_dump())

    def _register_self(self) -> None:
        """Register this worker in the Redis worker registry."""
        self._redis.register_worker(
            self._subscriber_name,
            self._worker_id,
            {
                "hostname": self._hostname,
                "pid": self.pid or 0,
                "status": "running",
                "started_at": str(time.time()),
                "last_heartbeat": str(time.time()),
            },
        )

    def _cleanup(self) -> None:
        """Close Redis and AuditStore connections."""
        # Unregister from worker registry
        if self._redis is not None:
            try:
                self._redis.unregister_worker(self._subscriber_name, self._worker_id)
            except Exception:
                logger.error(
                    "Error unregistering worker {}\n{}",
                    self._worker_id,
                    traceback.format_exc(),
                )

        # Close embedded publisher if created
        if self._publisher_client is not None:
            try:
                self._publisher_client.close()
            except Exception:
                logger.error(
                    "Error closing publisher client in worker {}\n{}",
                    self._worker_id,
                    traceback.format_exc(),
                )

        try:
            if self._audit is not None:
                self._audit.close()
        except Exception:
            logger.error(
                "Error closing audit store in worker {}\n{}",
                self._worker_id,
                traceback.format_exc(),
            )

        try:
            if self._redis is not None:
                self._redis.disconnect()
        except Exception:
            logger.error(
                "Error disconnecting Redis in worker {}\n{}",
                self._worker_id,
                traceback.format_exc(),
            )
