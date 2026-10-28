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


def _as_float(value: Any) -> float | None:
    """Best-effort float parse; None on missing/malformed input."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def _as_int(value: Any) -> int:
    """Best-effort int parse; 0 on missing/malformed input."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _worker_activity_time(raw: dict[str, Any]) -> datetime | None:
    """Real activity time for a worker: last completed message, else its
    registration time. None when neither is usable (caller falls back to
    the query time)."""
    ts = _as_float(raw.get("last_activity"))
    if ts is None or ts <= 0:
        ts = _as_float(raw.get("started_at"))
    if ts is None or ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


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

        Live fields (messages_processed / last_message_id / last_activity)
        are read from the worker registry in Redis; they are flushed by
        each worker on its heartbeat cadence, so they can be a few seconds
        stale. When registry data is missing, last_activity degrades to
        the query time and counters default to zero.

        Note: worker-level heartbeat stops updating while a worker is blocked
        in a slow handler, so "heartbeat expired" can mean zombie OR
        legitimately slow processing; the stuck message itself is visible
        in the consumer's PEL (XPENDING), not in last_message_id (which
        holds the last COMPLETED message).
        """
        now = datetime.now(timezone.utc)
        # Worker info from the Redis registry, keyed by worker_id (empty
        # when no connection was provided or the read failed).
        info_map: dict[str, dict[str, Any]] = {}
        if self._redis is not None:
            try:
                raw_workers = self._redis.get_consumer_group_workers(
                    self._subscriber_name
                )
                for raw in raw_workers:
                    wid = raw.get("worker_id")
                    if wid:
                        info_map[wid] = raw
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
            raw = info_map.get(worker.worker_id)
            # Default 0.0 on a missing key preserves the original semantics
            # (missing heartbeat -> 0 -> stale -> ERROR); a malformed value
            # parses to None -> treated as fresh (RUNNING), also as before.
            last_hb = _as_float(raw.get("last_heartbeat", 0.0)) if raw else None
            if not worker.is_alive():
                state = WorkerState.STOPPED
            elif last_hb is not None and (now_ts - last_hb) > stale_threshold:
                state = WorkerState.ERROR
            else:
                state = WorkerState.RUNNING
            last_message_id = raw.get("last_message_id") if raw else None
            statuses.append(
                WorkerStatus(
                    worker_id=worker.worker_id,
                    process_id=worker.pid or 0,
                    status=state,
                    last_message_id=(
                        last_message_id if isinstance(last_message_id, str) else None
                    ),
                    messages_processed=_as_int(raw.get("messages_processed"))
                    if raw
                    else 0,
                    last_activity=(_worker_activity_time(raw) or now) if raw else now,
                )
            )
        return statuses
