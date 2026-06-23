"""GPMQAsyncBatchHandler - batch async message processing."""

import time
from typing import Optional

from gpmq.client import GPMQClient
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.progress import GPMQProgress
from gpmq.publisher.result_handler import ResultHandler


class GPMQAsyncBatchHandler:
    """Batch async message processor.

    Collects messages from send_message() calls and provides
    unified wait_all() with sliding window concurrency control,
    progress reporting and timeout control.
    """

    def __init__(
        self,
        client: GPMQClient,
        progress: Optional[GPMQProgress] = None,
        timeout: Optional[float] = None,
        queue_size: int = 32,
    ) -> None:
        if queue_size < 1:
            raise ValueError(f"queue_size must be >= 1, got {queue_size}")
        self._client = client
        self._progress = progress
        self._timeout = timeout
        self._queue_size = queue_size
        self._pending_messages: list[tuple[str, dict, Optional[str]]] = []
        self._all_results: list[dict[str, ProcessResult]] = []
        self._waited = False

    def send_message(
        self,
        message_type: str,
        payload: dict,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Store message for deferred publishing."""
        self._pending_messages.append((message_type, payload, correlation_id))

    def wait_all(self) -> list[dict[str, ProcessResult]]:
        """Wait for all messages to be sent and results collected.

        Uses a sliding window: sends up to queue_size messages initially,
        then replenishes one-for-one as results complete. This limits
        concurrent threads to queue_size regardless of total message count.

        Returns:
            List of result dicts, one per message, in send_message() order.
        """
        total = len(self._pending_messages)
        if total == 0:
            return []

        if self._waited:
            return self._all_results

        if self._progress:
            self._progress.create(total)
            self._progress.start()

        deadline = time.monotonic() + self._timeout if self._timeout else None

        # Send initial batch
        window_size = min(self._queue_size, total)
        active: list[tuple[int, ResultHandler]] = []  # (original_index, handler)
        next_idx = 0

        for i in range(window_size):
            msg_type, payload, corr_id = self._pending_messages[i]
            handler = self._client.publish_async(msg_type, payload, corr_id)
            active.append((i, handler))
            next_idx = i + 1

        result_map: dict[int, dict[str, ProcessResult]] = {}

        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break

            if not active:
                break

            # Wait on current active window
            active_handlers = [h for _, h in active]
            wait_results = self._client.wait_all(
                active_handlers, timeout=1.0, allow_timeout_retry=True
            )

            # Separate completed vs still-pending
            # With allow_timeout_retry=True, TIMEOUT in results means
            # "subscriber hasn't responded yet" — so "no TIMEOUT" = complete
            new_active = []
            for (orig_idx, _handler), result in zip(active, wait_results):
                if all(pr.status != ProcessStatus.TIMEOUT for pr in result.values()):
                    result_map[orig_idx] = result
                    # Refill: send next pending message
                    if next_idx < total:
                        msg_type, payload, corr_id = self._pending_messages[next_idx]
                        try:
                            new_handler = self._client.publish_async(
                                msg_type, payload, corr_id
                            )
                            new_active.append((next_idx, new_handler))
                        except Exception:
                            result_map[next_idx] = {}
                        next_idx += 1
                else:
                    new_active.append((orig_idx, _handler))

            active = new_active

            total_completed = len(result_map)
            if self._progress:
                self._progress.update(total_completed)

            if not active:
                break

        # Final cleanup: PubSub unsubscribe and save TIMEOUT results to audit
        if active:
            active_handlers = [h for _, h in active]
            active_indices = [idx for idx, _ in active]
            final_results = self._client.wait_all(
                active_handlers, timeout=1.0, allow_timeout_retry=False
            )
            for orig_idx, result in zip(active_indices, final_results):
                result_map[orig_idx] = result

        self._all_results = [result_map[i] for i in range(total)]

        if self._progress:
            self._progress.finish()

        self._waited = True
        return self._all_results

    def get_all_result(self) -> list[dict[str, ProcessResult]]:
        """Get all message processing results.

        Available after wait_all() or context manager exit.
        Returns empty list if called before wait_all().
        """
        return self._all_results

    def __enter__(self) -> "GPMQAsyncBatchHandler":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.wait_all()
