"""Multi-process worker management."""

from __future__ import annotations

import multiprocessing
import socket
import time
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from gpmq.log import get_logger
from gpmq.models.status import WorkerState, WorkerStatus
from gpmq.exceptions import InvalidMessageError

if TYPE_CHECKING:
    from gpmq.subscriber.worker import Worker

# gpclog does not ship py.typed yet (PEP 561), so its logger type resolves to
# Any; annotate the module-global logger as Any until it does.
logger: Any = None


class WorkerManager:
    """Manages multiple :class:`Worker` processes.

    Starts, stops, and monitors worker processes.  Uses a
    ``multiprocessing.Event`` to signal graceful shutdown.
    """

    def __init__(
        self,
        config_dict: dict[str, Any],
        worker_context: dict[str, Any],
        redis_conn: Any = None,
    ) -> None:
        self._config = config_dict
        self._stop_event = multiprocessing.Event()
        self._workers: list[Worker] = []
        self._worker_ctx = worker_context
        self._redis = redis_conn
        self._subscriber_name = config_dict.get("name", "subscriber")
        self._heartbeat_interval = config_dict.get("heartbeat_interval", 10)

        global logger
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
        """Return status info for all workers (RUNNING/STOPPED/ERROR).

        Classification:
        - not is_alive() -> STOPPED
        - is_alive() AND worker-level heartbeat expired
          (now - last_heartbeat > heartbeat_interval * 3) -> ERROR (zombie)
        - is_alive() AND heartbeat fresh -> RUNNING

        Note: worker-level heartbeat stops updating while a worker is blocked
        in a slow handler, so "heartbeat expired" can mean zombie OR legitimately
        slow processing; precise distinction is deferred to R-001.
        """
        now = datetime.now(timezone.utc)
        # Read worker-level heartbeats from Redis (if a connection was provided).
        heartbeat_map: dict[str, float] = {}
        if self._redis is not None:
            try:
                raw_workers = self._redis.get_consumer_group_workers(
                    self._subscriber_name
                )
                for raw in raw_workers:
                    wid = raw.get("worker_id")
                    if wid:
                        try:
                            heartbeat_map[wid] = float(raw.get("last_heartbeat", 0))
                        except (TypeError, ValueError):
                            pass
            except Exception:
                logger.error(
                    "Failed to read worker heartbeats for {}\n{}",
                    self._subscriber_name,
                    traceback.format_exc(),
                )
        stale_threshold = self._heartbeat_interval * 3
        now_ts = time.time()

        statuses = []
        for worker in self._workers:
            if not worker.is_alive():
                state = WorkerState.STOPPED
            else:
                last_hb = heartbeat_map.get(worker.worker_id)
                if last_hb is not None and (now_ts - last_hb) > stale_threshold:
                    state = WorkerState.ERROR
                else:
                    state = WorkerState.RUNNING
            statuses.append(
                WorkerStatus(
                    worker_id=worker.worker_id,
                    process_id=worker.pid or 0,
                    status=state,
                    last_activity=now,
                )
            )
        return statuses
