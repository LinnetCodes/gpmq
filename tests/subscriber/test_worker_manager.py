"""Tests for subscriber worker_manager module."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from gpmq import SubscriberConfig
from gpmq.models.status import WorkerState, WorkerStatus


# --- Helper functions ---


def _make_subscriber_config(
    name: str = "test-sub",
    workers: int = 2,
    stream_name: str = "gpmq:messages",
    message_types: list[str] | None = None,
) -> dict:
    """Create test SubscriberConfig kwargs."""
    return {
        "redis_host": "127.0.0.1",
        "redis_port": 6379,
        "redis_db": 15,
        "redis_password": "123456",
        "stream_name": stream_name,
        "heartbeat_interval": 60,
        "name": name,
        "workers": workers,
        "handlers": [
            {
                "message_types": message_types or ["type_a"],
                "handler": "gpmq.subscriber.examples.on_msg_example_type_a",
            },
            {
                "message_types": message_types or ["type_b"],
                "handler": "gpmq.subscriber.examples.on_msg_example_type_b",
            },
        ],
        "log_category": "gpmq",
    }


@pytest.fixture(autouse=True)
def _patch_worker_run():
    """Patch Worker.run so tests don't need a live Redis instance.

    The no-op ``run`` simply waits on the stop event, keeping the process
    alive until ``stop_event.set()`` is called.
    """

    def _no_op_run(self):
        import time

        while not self._stop_event.is_set():
            time.sleep(0.05)

    with patch("gpmq.subscriber.worker.Worker.run", _no_op_run):
        yield


class TestWorkerManagerStart:
    """Tests for WorkerManager.start()."""

    def test_starts_correct_number_of_workers(self):
        """Should start the number of workers specified in config."""
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=3)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()

        assert len(manager._workers) == 3
        for w in manager._workers:
            assert w.is_alive()

        manager.stop()

    def test_single_worker(self):
        """Should start exactly 1 worker when config.workers=1."""
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()

        assert len(manager._workers) == 1
        assert manager._workers[0].is_alive()

        manager.stop()

    def test_worker_id_contains_hostname(self):
        """Worker IDs should contain the hostname for uniqueness across hosts."""
        import socket
        from gpmq.subscriber.worker_manager import WorkerManager

        hostname = socket.gethostname()
        config_dict = _make_subscriber_config(name="test-sub", workers=2)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()

        for w in manager._workers:
            assert hostname in w.worker_id
            assert "-worker-" in w.worker_id

        manager.stop()


class TestWorkerManagerStop:
    """Tests for WorkerManager.stop()."""

    def test_stop_sets_event_and_joins_workers(self):
        """Should set stop_event and join all workers."""
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=2)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()
        workers_before = list(manager._workers)
        manager.stop(timeout=5)

        assert workers_before  # non-empty, so the assertion is meaningful
        for w in workers_before:
            assert not w.is_alive()


class TestWorkerManagerGetStatus:
    """Tests for WorkerManager.get_worker_status()."""

    def test_returns_status_for_all_workers(self):
        """Should return WorkerStatus for each worker."""
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=2)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()

        statuses = manager.get_worker_status()

        assert len(statuses) == 2
        for status in statuses:
            assert isinstance(status, WorkerStatus)
            assert status.process_id > 0
            assert status.status in (
                WorkerState.RUNNING,
                WorkerState.STOPPED,
                WorkerState.ERROR,
            )

        manager.stop()

    def test_zombie_worker_reported_as_error(self):
        """3.3: an alive worker whose heartbeat expired is reported as ERROR."""
        import time
        from unittest.mock import MagicMock
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()
        wid = manager._workers[0].worker_id

        manager._redis = MagicMock()
        manager._redis.get_consumer_group_workers.return_value = [
            {"worker_id": wid, "last_heartbeat": time.time() - 1000},
        ]

        statuses = manager.get_worker_status()
        assert len(statuses) == 1
        assert statuses[0].status == WorkerState.ERROR
        manager.stop()

    def test_fresh_heartbeat_reported_as_running(self):
        """3.3: an alive worker with a fresh heartbeat is RUNNING."""
        import time
        from unittest.mock import MagicMock
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()
        wid = manager._workers[0].worker_id

        manager._redis = MagicMock()
        manager._redis.get_consumer_group_workers.return_value = [
            {"worker_id": wid, "last_heartbeat": time.time()},
        ]

        statuses = manager.get_worker_status()
        assert statuses[0].status == WorkerState.RUNNING
        manager.stop()

    def test_live_status_fields_assembled_from_redis(self):
        """R-001: counters and real last_activity come from the registry."""
        import time
        from datetime import datetime, timezone
        from unittest.mock import MagicMock
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()
        wid = manager._workers[0].worker_id

        raw_ts = time.time() - 60
        manager._redis = MagicMock()
        manager._redis.get_consumer_group_workers.return_value = [
            {
                "worker_id": wid,
                "last_heartbeat": time.time(),
                "messages_processed": 5,
                "last_activity": raw_ts,
                "last_message_id": "1690000000000-3",
                "started_at": time.time() - 3600,
            },
        ]

        statuses = manager.get_worker_status()
        assert statuses[0].messages_processed == 5
        assert statuses[0].last_message_id == "1690000000000-3"
        assert statuses[0].last_activity == datetime.fromtimestamp(
            raw_ts, tz=timezone.utc
        )
        manager.stop()

    def test_last_activity_falls_back_to_started_at(self):
        """A worker that never processed anything reports its start time."""
        import time
        from datetime import datetime, timezone
        from unittest.mock import MagicMock
        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        manager = WorkerManager(config_dict, ctx)
        manager.start()
        wid = manager._workers[0].worker_id

        started = time.time() - 3600
        manager._redis = MagicMock()
        manager._redis.get_consumer_group_workers.return_value = [
            {"worker_id": wid, "last_heartbeat": time.time(), "started_at": started},
        ]

        statuses = manager.get_worker_status()
        assert statuses[0].messages_processed == 0
        assert statuses[0].last_message_id is None
        assert statuses[0].last_activity == datetime.fromtimestamp(
            started, tz=timezone.utc
        )
        manager.stop()


class TestPendingPel:
    """Tests for per-consumer PEL depth in get_worker_status()."""

    def test_get_worker_status_fills_pending_pel(self):
        """Task 9: pending_pel is filled from one xpending group summary.

        Consumer names in the summary are ``{subscriber_name}:{worker_id}``;
        each worker's slot depth is looked up from that summary.
        """
        from unittest.mock import MagicMock, call

        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(name="s", workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        mock_redis = MagicMock()
        mock_redis.get_consumer_group_workers.return_value = []
        mock_redis.xpending.return_value = {
            "pending": 2,
            "consumers": [{"name": "s:s-host-worker-0", "pending": 2}],
        }

        manager = WorkerManager(config_dict, ctx, redis_conn=mock_redis)

        # No workers tracked -> no status rows.
        assert manager.get_worker_status() == []

        # Inject a (dead) worker without spawning a process.
        mock_worker = MagicMock()
        mock_worker.worker_id = "s-host-worker-0"
        mock_worker.pid = 1
        mock_worker.is_alive.return_value = False
        manager._workers = [mock_worker]

        statuses = manager.get_worker_status()
        assert len(statuses) == 1
        assert statuses[0].pending_pel == 2
        assert statuses[0].status == WorkerState.STOPPED
        # One xpending call per get_worker_status() invocation, on the
        # configured stream and the subscriber-name group.
        assert mock_redis.xpending.call_args == call("gpmq:messages", "s")

    def test_get_worker_status_pending_pel_zero_when_missing(self):
        """Task 9: a worker absent from the xpending summary reports 0."""
        from unittest.mock import MagicMock

        from gpmq.subscriber.worker_manager import WorkerManager

        config_dict = _make_subscriber_config(name="s", workers=1)
        subscrib_cfg = SubscriberConfig(**config_dict)
        ctx = {"subscribe_message_types": subscrib_cfg.get_message_types()}

        mock_redis = MagicMock()
        mock_redis.get_consumer_group_workers.return_value = []
        mock_redis.xpending.return_value = {"pending": 0, "consumers": []}

        manager = WorkerManager(config_dict, ctx, redis_conn=mock_redis)

        mock_worker = MagicMock()
        mock_worker.worker_id = "s-host-worker-0"
        mock_worker.pid = 1
        mock_worker.is_alive.return_value = False
        manager._workers = [mock_worker]

        statuses = manager.get_worker_status()
        assert statuses[0].pending_pel == 0
