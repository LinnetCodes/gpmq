"""Result handler for collecting async message processing results."""

import time
from typing import Optional

from gpmq import AuditStore
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.storage.redis_adapter import RedisAdapter


class ResultHandler:
    """Manage async result collection via Redis Pub/Sub.

    Subscribes to result channels and collects ProcessResult
    from subscribers as they complete processing.

    Channel format: {key_prefix}result:{message_id}:{subscriber_name}
    """

    def __init__(
        self,
        message_id: str,
        subscriber_names: list[str],
        redis_adapter: RedisAdapter,
        key_prefix: str,
        audit_store: Optional[AuditStore] = None
    ) -> None:
        """Initialize result handler and subscribe to result channels.

        Args:
            message_id: Message ID to collect results for
            subscriber_names: List of expected subscriber names
            redis_adapter: Redis adapter for Pub/Sub operations
            key_prefix: Redis key prefix for channel names
        """
        self._message_id = message_id
        self._expected: set[str] = set(subscriber_names)
        self._results: dict[str, ProcessResult] = {}
        self._redis = redis_adapter
        self._key_prefix = key_prefix
        self._audit_store = audit_store
        self._pubsub = None
        self._start_time = time.monotonic()

        if self._expected:
            self._channels = [
                f"{key_prefix}result:{message_id}:{name}" for name in subscriber_names
            ]
            self._pubsub = self._redis.subscribe(self._channels)

    @property
    def message_id(self) -> str:
        """Get the message ID this handler is tracking."""
        return self._message_id

    def wait(self, timeout: Optional[float] = None,
             allow_timeout_retry: Optional[bool] = None) -> dict[str, ProcessResult]:
        """Block until all results collected or timeout.

        Args:
            timeout: Max seconds to wait. None = use default polling.
            allow_timeout_retry: allow re-entry of this method (not save timeout result and unsubscribe ProcessResult)

        Returns:
            Dict mapping subscriber_name to ProcessResult.
            Missing subscribers get TIMEOUT status.
        """
        if not self._expected:
            return {}

        deadline = time.monotonic() + timeout if timeout else None

        while len(self._results) < len(self._expected):
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                poll_timeout = min(remaining, 1.0)
            else:
                poll_timeout = 1.0

            msg = self._redis.get_message(self._pubsub, timeout=poll_timeout)
            if msg and "data" in msg:
                result = ProcessResult.model_validate(msg["data"])
                self._results[result.subscriber_name] = result
                if self._audit_store:
                    self._audit_store.save_result(self._message_id, result)

        if len(self._results) == len(self._expected):
            self._cleanup()

        if allow_timeout_retry:
            # Generate TIMEOUT for missing subscribers
            elapsed = time.monotonic() - self._start_time
            _temp_results = dict(self._results)
            for name in self._expected - set(self._results.keys()):
                _temp_results[name] = ProcessResult(
                    message_id=self._message_id,
                    status=ProcessStatus.TIMEOUT,
                    subscriber_name=name,
                    processing_time=elapsed,
                    timeout_seconds=timeout,
                )
            return dict(_temp_results)

        # mark timeout subscribers and final cleanup
        # Generate TIMEOUT for missing subscribers
        elapsed = time.monotonic() - self._start_time
        for name in self._expected - set(self._results.keys()):
            self._results[name] = ProcessResult(
                message_id=self._message_id,
                status=ProcessStatus.TIMEOUT,
                subscriber_name=name,
                processing_time=elapsed,
                timeout_seconds=timeout,
            )

        if self._audit_store:
            for name, result in self._results.items():
                if result.status == ProcessStatus.TIMEOUT:
                    self._audit_store.save_result(self._message_id, result)

        self._cleanup()
        return dict(self._results)

    def wait_any(self, timeout: Optional[float] = None) -> dict[str, ProcessResult]:
        """Wait for at least one result.

        Args:
            timeout: Max seconds to wait. None = use default polling.

        Returns:
            Dict with at least one result. Empty dict if timeout with no results.
        """
        if not self._expected:
            return {}

        deadline = time.monotonic() + timeout if timeout else None

        while len(self._results) < 1:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                poll_timeout = min(remaining, 1.0)
            else:
                poll_timeout = 1.0

            msg = self._redis.get_message(self._pubsub, timeout=poll_timeout)
            if msg and "data" in msg:
                result = ProcessResult.model_validate(msg["data"])
                self._results[result.subscriber_name] = result
                if self._audit_store:
                    self._audit_store.save_result(self._message_id, result)

        if self._results:
            self._cleanup()

        return dict(self._results)

    def is_complete(self) -> bool:
        """Check if all expected results have been collected.

        Returns:
            True if all subscribers have responded
        """
        return len(self._results) >= len(self._expected)

    def get_completed_count(self) -> int:
        """Get number of completed subscriber results.

        Returns:
            Count of results received so far
        """
        return len(self._results)

    def get_results(self) -> dict[str, ProcessResult]:
        """Get current results without waiting.

        Returns:
            Dict of subscriber_name to ProcessResult collected so far
        """
        return dict(self._results)

    def _cleanup(self) -> None:
        """Unsubscribe from result channels."""
        if self._pubsub:
            try:
                self._redis.unsubscribe(self._pubsub)
            except Exception:
                pass
            self._pubsub = None
