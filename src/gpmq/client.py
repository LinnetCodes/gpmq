"""GPMQ Client - main entry point for publishing messages."""

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from gpmq.async_batch import GPMQAsyncBatchHandler
    from gpmq.models.worker_info import ConsumerGroupWorkers
    from gpmq.progress import GPMQProgress

from gpmq.config import GPMQConfig
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult
from gpmq.publisher.publisher import Publisher
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter
from gpmq.log import get_logger


class GPMQClient:
    """Main entry point for GPMQ message publishing.

    Provides both synchronous and asynchronous message publishing
    with automatic result collection.

    Usage:
        client = GPMQClient(PublisherConfig(publisher_name="my_app"))
        client.connect()
        results = client.publish("user_created", {"user_id": 123})
        client.close()

        # Or use as context manager:
        with GPMQClient(PublisherConfig(publisher_name="my_app")) as client:
            results = client.publish("user_created", {"user_id": 123})
    """

    def __init__(self, config: GPMQConfig) -> None:
        """Initialize client with configuration.

        Args:
            config: GPMQ configuration. PublisherConfig recommended for
                    publishing (provides publisher_name). GPMQConfig works
                    with default publisher_name="anonymous".
        """
        self._config = config
        self._publisher_name = getattr(config, "publisher_name", "anonymous")
        self._redis = RedisAdapter(config)
        self._audit = AuditStore(config.audit_db_path) if config.enable_audit else None
        self._publisher = Publisher(self._redis, self._audit, config)
        self._connected = False
        self._logger = get_logger(getattr(config, "log_category", "gpmq_client"))

    @property
    def publisher_name(self) -> str:
        return self._publisher_name

    def __enter__(self) -> "GPMQClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def connect(self) -> None:
        """Connect to Redis and audit store.

        Reentrant: calling connect() when already connected is a no-op.

        Raises:
            RedisConnectionError: If connection fails after retries
        """
        if self._connected:
            return
        self._redis.connect()
        if self._audit:
            self._audit.connect()
        self._connected = True

    def close(self) -> None:
        """Close connections and cleanup resources.

        Reentrant: calling close() multiple times is safe.
        """
        if self._connected:
            self._redis.disconnect()
            self._connected = False
        if self._audit:
            self._audit.close()

    @staticmethod
    def _normalize_targets(
        target_subscribers: "str | list[str] | None",
    ) -> Optional[list[str]]:
        """Normalize the target_subscribers argument.

        - None  -> None (broadcast, legacy behavior)
        - str   -> [str]
        - list  -> de-duplicated, order-preserving list
        - empty str / empty list / list containing empty strings -> ValueError
        """
        if target_subscribers is None:
            return None
        if isinstance(target_subscribers, str):
            target_subscribers = [target_subscribers]
        if not isinstance(target_subscribers, list):
            raise TypeError(
                "target_subscribers must be str, list[str], or None; "
                f"got {type(target_subscribers).__name__}"
            )
        seen: set[str] = set()
        normalized: list[str] = []
        for name in target_subscribers:
            if not isinstance(name, str) or not name:
                raise ValueError(
                    "target_subscribers must contain non-empty strings"
                )
            if name not in seen:
                seen.add(name)
                normalized.append(name)
        if not normalized:
            raise ValueError("target_subscribers must not be empty")
        return normalized

    def publish(
        self,
        message_type: str,
        payload: dict,
        correlation_id: Optional[str] = None,
        timeout: Optional[float] = None,
        target_subscribers: Optional[Union[str, list[str]]] = None,
    ) -> dict[str, ProcessResult]:
        """Publish message and synchronously wait for results.

        Equivalent to publish_async() + handler.wait().

        Args:
            message_type: Type of message (e.g., "user_created")
            payload: Message payload dict
            correlation_id: Optional correlation ID for message chains
            timeout: Max seconds to wait for results. None = use config default.
            target_subscribers: Optional subscriber name or list of names.
                Only these subscribers will process the message; others
                discard it. None (default) broadcasts to all matching.

        Returns:
            Dict mapping subscriber_name to ProcessResult
        """
        handler = self.publish_async(
            message_type, payload, correlation_id, target_subscribers
        )
        effective_timeout = timeout or self._config.publish_timeout
        results = handler.wait(timeout=effective_timeout)

        return results

    def publish_async(
        self,
        message_type: str,
        payload: dict,
        correlation_id: Optional[str] = None,
        target_subscribers: Optional[Union[str, list[str]]] = None,
    ) -> ResultHandler:
        """Publish message without waiting for results.

        Args:
            message_type: Type of message
            payload: Message payload dict
            correlation_id: Optional correlation ID
            target_subscribers: Optional subscriber name or list of names.
                Only these subscribers will process the message; others
                discard it. None (default) broadcasts to all matching.

        Returns:
            ResultHandler for collecting results later
        """
        targets = self._normalize_targets(target_subscribers)
        message = Message(
            id="pending",
            type=message_type,
            timestamp=datetime.now(timezone.utc),
            publisher=self._publisher_name,
            payload=payload,
            correlation_id=correlation_id,
            target_subscribers=targets,
        )
        _, handler = self._publisher.publish_message(message)
        return handler

    def send(
        self,
        message_type: str,
        payload: dict,
        correlation_id: Optional[str] = None,
        target_subscribers: Optional[Union[str, list[str]]] = None,
    ) -> str:
        """Fire-and-forget publish: send message without collecting results.

        Publishes message to the stream but does not subscribe to result
        channels. No ResultHandler is created, avoiding Pub/Sub resource leaks.

        Args:
            message_type: Type of message (e.g., "user_created")
            payload: Message payload dict
            correlation_id: Optional correlation ID for message chains
            target_subscribers: Optional subscriber name or list of names.
                None (default) broadcasts to all matching.

        Returns:
            Redis-generated message ID for logging/tracking
        """
        targets = self._normalize_targets(target_subscribers)
        message = Message(
            id="pending",
            type=message_type,
            timestamp=datetime.now(timezone.utc),
            publisher=self._publisher_name,
            payload=payload,
            correlation_id=correlation_id,
            target_subscribers=targets,
        )
        return self._publisher.send_message(message)

    def wait_all(
        self,
        handlers: list[ResultHandler],
        timeout: Optional[float] = None,
        allow_timeout_retry: Optional[bool] = None,
    ) -> list[dict[str, ProcessResult]]:
        """Wait for results from multiple handlers.

        Args:
            handlers: List of ResultHandler instances
            timeout: Max seconds to wait per handler
            allow_timeout_retry: allow re-entry of this method (not save timeout result and unsubscribe ProcessResult)
        Returns:
            List of result dicts, one per handler
        """
        if not handlers:
            return []
        effective_timeout = timeout or self._config.publish_timeout
        with ThreadPoolExecutor(max_workers=min(len(handlers), 32)) as executor:
            futures = [
                executor.submit(
                    handler.wait,
                    timeout=effective_timeout,
                    allow_timeout_retry=allow_timeout_retry,
                )
                for handler in handlers
            ]
            results = [f.result() for f in futures]
        return results

    def get_audit_store(self) -> Optional[AuditStore]:
        """Get the audit store for querying.

        Returns:
            AuditStore instance, or None if audit is disabled
        """
        return self._audit

    def get_consumer_group_workers(self, subscriber_name: str) -> "ConsumerGroupWorkers":
        """Query all workers registered under a consumer group.

        Includes idle workers. Workers whose heartbeat exceeds
        ``heartbeat_interval * 3`` are marked as ``"stale"``.

        Args:
            subscriber_name: Name of the subscriber / consumer group.

        Returns:
            ConsumerGroupWorkers with worker list and count.
        """
        from gpmq.models.worker_info import WorkerInfo, ConsumerGroupWorkers

        raw_workers = self._redis.get_consumer_group_workers(subscriber_name)
        timeout = self._config.heartbeat_interval * 3
        now = time.time()

        workers = []
        for raw in raw_workers:
            status = raw.get("status", "running")
            last_hb = float(raw.get("last_heartbeat", 0))
            if now - last_hb > timeout:
                status = "stale"

            workers.append(WorkerInfo(
                worker_id=raw.get("worker_id") or "unknown",
                hostname=raw.get("hostname") or "unknown",
                pid=int(raw.get("pid", 0)),
                status=status,
                started_at=float(raw.get("started_at", 0)),
                last_heartbeat=last_hb,
            ))

        return ConsumerGroupWorkers(
            subscriber_name=subscriber_name,
            workers=workers,
        )

    def async_batch(
        self,
        progress: Optional["GPMQProgress"] = None,
        timeout: Optional[float] = None,
        queue_size: Optional[int] = None,
    ) -> "GPMQAsyncBatchHandler":
        """Create a batch async message handler.

        Args:
            progress: Optional progress indicator for tracking batch progress
            timeout: Max total seconds to wait for all results. None = wait forever.
            queue_size: Max concurrent messages in flight. None = use config default or 32.

        Returns:
            GPMQAsyncBatchHandler for sending messages and collecting results.
        """
        from gpmq.async_batch import GPMQAsyncBatchHandler

        if queue_size is None:
            queue_size = getattr(self._config, "async_batch_queue_size", 32)
        self._logger.info(f"async batch queue size: {queue_size}")

        return GPMQAsyncBatchHandler(self, progress, timeout, queue_size)
