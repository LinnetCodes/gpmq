"""Multi-process worker management."""

from __future__ import annotations

import multiprocessing
import socket
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from gpclog import GPCLogger
from gpmq.log import get_logger
from gpmq.models.status import WorkerState, WorkerStatus
from gpmq.exceptions import InvalidMessageError

if TYPE_CHECKING:
    from gpmq.subscriber.worker import Worker

logger: GPCLogger | None = None


class WorkerManager:
    """Manages multiple :class:`Worker` processes.

    Starts, stops, and monitors worker processes.  Uses a
    ``multiprocessing.Event`` to signal graceful shutdown.
    """

    def __init__(self, config_dict: dict[str, Any], worker_context: dict[str, Any]) -> None:
        self._config = config_dict
        self._stop_event = multiprocessing.Event()
        self._workers: list[Worker] = []
        self._worker_ctx = worker_context

        global  logger
        logger = get_logger(self._config["log_category"])

    def start(self) -> None:
        """Start all worker processes."""
        from gpmq.subscriber.worker import Worker

        if multiprocessing.get_start_method() == "fork":
            logger.warning(
                "Running under 'fork' start method; parent-process global state "
                "(handler registry, log sinks, connections) may leak into workers "
                "and cause DuplicateMessageTypeError or log corruption. "
                "Call gpmq.use_spawn() at startup to use spawn semantics."
            )

        worker_count = self._config.get("workers", 2)
        name = self._config.get("name", "subscriber")
        stream_name = self._config.get("stream_name", "gpmq:messages")
        message_types = self._worker_ctx.get("subscribe_message_types", [])
        if len(message_types) == 0:
            raise InvalidMessageError("No Message Types", "No message types specified")

        hostname = socket.gethostname()

        for i in range(worker_count):
            worker = Worker(
                subscriber_name=name,
                stream_name=stream_name,
                message_types=message_types,
                worker_id=f"{name}-{hostname}-worker-{i}",
                hostname=hostname,
                config_dict=self._config,
                context=self._worker_ctx,
                stop_event=self._stop_event,
            )
            worker.start()
            self._workers.append(worker)
            logger.info("Started worker {} (pid={})", worker.worker_id, worker.pid)

    def stop(self, timeout: float = 30) -> None:
        """Gracefully stop all worker processes."""
        logger.info("Stopping {} workers...", len(self._workers))

        self._stop_event.set()

        for worker in self._workers:
            worker.join(timeout=timeout)
            if worker.is_alive():
                logger.warning(
                    "Worker {} did not stop in time, terminating",
                    worker.worker_id,
                )
                worker.terminate()
                worker.join(timeout=5)

        self._workers.clear()
        logger.info("All workers stopped")

    def get_worker_status(self) -> list[WorkerStatus]:
        """Return status info for all workers."""
        now = datetime.now(timezone.utc)
        statuses = []
        for worker in self._workers:
            state = WorkerState.BUSY if worker.is_alive() else WorkerState.STOPPED
            statuses.append(
                WorkerStatus(
                    worker_id=worker.worker_id,
                    process_id=worker.pid or 0,
                    status=state,
                    last_activity=now,
                )
            )
        return statuses
