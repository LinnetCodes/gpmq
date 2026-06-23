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
                "message_types":  message_types or ["type_a"],
                "handler": "gpmq.subscriber.examples.on_msg_example_type_a",
            },
            {
                "message_types":  message_types or ["type_b"],
                "handler": "gpmq.subscriber.examples.on_msg_example_type_b",
            }
        ],
        "log_category": "gpmq"
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
        manager.stop(timeout=5)

        for w in manager._workers:
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
            assert status.status in (WorkerState.IDLE, WorkerState.BUSY)

        manager.stop()
