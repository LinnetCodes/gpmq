"""Tests for WorkerInfo and ConsumerGroupWorkers models."""

from gpmq.models.worker_info import WorkerInfo, ConsumerGroupWorkers


class TestWorkerInfo:
    def test_create_worker_info(self):
        info = WorkerInfo(
            worker_id="notifier-server1-worker-0",
            hostname="server1",
            pid=12345,
            status="running",
            started_at=1713500000.0,
            last_heartbeat=1713500100.0,
        )
        assert info.worker_id == "notifier-server1-worker-0"
        assert info.hostname == "server1"
        assert info.pid == 12345
        assert info.status == "running"

    def test_status_defaults_to_running(self):
        info = WorkerInfo(
            worker_id="sub-host-w0",
            hostname="host",
            pid=100,
            started_at=1713500000.0,
            last_heartbeat=1713500100.0,
        )
        assert info.status == "running"


class TestConsumerGroupWorkers:
    def test_create_with_workers(self):
        w1 = WorkerInfo(
            worker_id="notifier-server1-worker-0",
            hostname="server1",
            pid=100,
            started_at=1713500000.0,
            last_heartbeat=1713500100.0,
        )
        w2 = WorkerInfo(
            worker_id="notifier-server2-worker-0",
            hostname="server2",
            pid=200,
            started_at=1713500050.0,
            last_heartbeat=1713500110.0,
        )
        group = ConsumerGroupWorkers(
            subscriber_name="notifier",
            workers=[w1, w2],
        )
        assert group.subscriber_name == "notifier"
        assert group.worker_count == 2
        assert len(group.workers) == 2

    def test_worker_count_is_auto(self):
        group = ConsumerGroupWorkers(
            subscriber_name="sub",
            workers=[],
        )
        assert group.worker_count == 0
