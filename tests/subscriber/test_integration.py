"""Integration tests for the subscriber layer.

Uses real Redis server (127.0.0.1:6379, db=15).
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter
from gpmq.subscriber.handler import HandlerRegistry, message_handler
from gpmq.subscriber.worker import Worker
from multiprocessing import Event


class TestWorkerMessageProcessing:
    """Integration test: Worker processes a message from Redis stream."""

    def test_worker_processes_message_end_to_end(
        self, redis_adapter: RedisAdapter, audit_store: AuditStore, tmp_path
    ):
        """Worker reads from stream, calls handler, saves audit, publishes result."""
        HandlerRegistry.clear()

        results = []

        @message_handler(message_types=["integ_test_type"])
        def test_handler(msg: Message) -> ProcessResult:
            results.append(msg)
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="integ-test-sub",
                processing_time=0.01,
                data={"handled": True},
            )

        HandlerRegistry.register(test_handler)

        stream_name = "gpmq:messages"
        group_name = "integ-test-sub"
        db_path = str(tmp_path / "test_audit.db")

        try:
            # Create consumer group
            redis_adapter.xgroup_create(stream_name, group_name)

            # Publish a test message to the stream
            redis_adapter.xadd(
                stream_name,
                {
                    "type": "integ_test_type",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "publisher": "integ-test-pub",
                    "payload": json.dumps({"test": "data"}),
                },
            )

            # Create and configure worker
            config_dict = {
                "name": group_name,
                "handlers": [
                    {
                        "message_types": ["integ_test_type"],
                        "handler": "dummy.handler",
                    }
                ],
                "stream_name": stream_name,
                "redis_host": "127.0.0.1",
                "redis_port": 6379,
                "redis_password": "123456",
                "redis_db": 15,
                "heartbeat_interval": 300,
                "audit_db_path": db_path,
                "log_category": "gpmq",
            }

            worker = Worker(
                subscriber_name=group_name,
                stream_name=stream_name,
                message_types=["integ_test_type"],
                worker_id=f"{group_name}-worker-0",
                hostname="test-host",
                config_dict=config_dict,
            )

            # Override redis with the fixture connection
            worker._redis = redis_adapter

            # Use an Event to signal the worker thread to stop after processing
            stop_event = Event()

            def _worker_loop():
                """Run _read_and_process in this thread (creating AuditStore here
                so SQLite does not complain about cross-thread usage)."""
                thread_audit = AuditStore(db_path)
                thread_audit.connect()
                worker._audit = thread_audit
                try:
                    for _ in range(5):
                        if stop_event.is_set():
                            break
                        worker._read_and_process()
                        time.sleep(0.2)
                finally:
                    thread_audit.close()

            # Run worker loop in a thread
            worker_thread = threading.Thread(target=_worker_loop, daemon=True)
            worker_thread.start()

            # Wait for message to be processed
            worker_thread.join(timeout=5)
            stop_event.set()

            # Verify handler was called
            assert len(results) == 1
            assert results[0].type == "integ_test_type"

            # Verify audit result was saved (create a new connection in main thread)
            verify_audit = AuditStore(db_path)
            verify_audit.connect()
            stats = verify_audit.stats()
            assert stats.get("total_results", 0) >= 1
            verify_audit.close()

        finally:
            HandlerRegistry.clear()
            try:
                redis_adapter.clear_stream(stream_name)
                redis_adapter.xgroup_destroy(stream_name, group_name)
            except Exception:
                pass

    def test_worker_acknowledges_unknown_message_type(
        self, redis_adapter: RedisAdapter, tmp_path
    ):
        """Worker ACKs and skips messages whose type has no registered handler."""
        HandlerRegistry.clear()

        stream_name = "gpmq:messages"
        group_name = "integ-test-ack-sub"

        try:
            # Create consumer group
            redis_adapter.xgroup_create(stream_name, group_name)

            # Publish a message with an unregistered type
            redis_adapter.xadd(
                stream_name,
                {
                    "type": "unknown_integ_type",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "publisher": "integ-test-pub",
                    "payload": json.dumps({"test": "data"}),
                },
            )

            config_dict = {
                "name": group_name,
                "handlers": [
                    {
                        "message_types": ["registered_type"],
                        "handler": "dummy.handler",
                    }
                ],
                "stream_name": stream_name,
                "redis_host": "127.0.0.1",
                "redis_port": 6379,
                "redis_password": "123456",
                "redis_db": 15,
                "heartbeat_interval": 300,
                "audit_db_path": str(tmp_path / "ack_test.db"),
                "log_category": "gpmq",
            }

            worker = Worker(
                subscriber_name=group_name,
                stream_name=stream_name,
                message_types=["registered_type"],
                worker_id=f"{group_name}-worker-0",
                hostname="test-host",
                config_dict=config_dict,
            )
            worker._redis = redis_adapter

            # Process once
            worker._read_and_process()

            # Verify the pending entry list is empty (message was ACKed)
            pending = redis_adapter.xpending(stream_name, group_name)
            assert pending["count"] == 0

        finally:
            HandlerRegistry.clear()
            try:
                redis_adapter.clear_stream(stream_name)
                redis_adapter.xgroup_destroy(stream_name, group_name)
            except Exception:
                pass

    def test_worker_handles_handler_exception_end_to_end(
        self, redis_adapter: RedisAdapter, tmp_path
    ):
        """Worker catches handler exception, saves EXCEPTION result, ACKs message."""
        HandlerRegistry.clear()

        @message_handler(message_types=["integ_exc_type"])
        def failing_handler(msg: Message) -> ProcessResult:
            raise RuntimeError("integration test failure")

        HandlerRegistry.register(failing_handler)

        stream_name = "gpmq:messages"
        group_name = "integ-test-exc-sub"
        db_path = str(tmp_path / "exc_test.db")

        try:
            redis_adapter.xgroup_create(stream_name, group_name)

            redis_adapter.xadd(
                stream_name,
                {
                    "type": "integ_exc_type",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "publisher": "integ-test-pub",
                    "payload": json.dumps({"test": "data"}),
                },
            )

            config_dict = {
                "name": group_name,
                "handlers": [
                    {
                        "message_types": ["integ_exc_type"],
                        "handler": "dummy.handler",
                    }
                ],
                "stream_name": stream_name,
                "redis_host": "127.0.0.1",
                "redis_port": 6379,
                "redis_password": "123456",
                "redis_db": 15,
                "heartbeat_interval": 300,
                "audit_db_path": db_path,
                "log_category": "gpmq",
            }

            test_audit = AuditStore(db_path)
            test_audit.connect()

            worker = Worker(
                subscriber_name=group_name,
                stream_name=stream_name,
                message_types=["integ_exc_type"],
                worker_id=f"{group_name}-worker-0",
                hostname="test-host",
                config_dict=config_dict,
            )
            worker._redis = redis_adapter
            worker._audit = test_audit

            worker._read_and_process()

            # Verify audit saved an EXCEPTION result
            stats = test_audit.stats()
            assert stats.get("total_results", 0) >= 1
            assert "exception" in stats.get("results_by_status", {})

            # Verify message was ACKed
            pending = redis_adapter.xpending(stream_name, group_name)
            assert pending["count"] == 0

            test_audit.close()

        finally:
            HandlerRegistry.clear()
            try:
                redis_adapter.clear_stream(stream_name)
                redis_adapter.xgroup_destroy(stream_name, group_name)
            except Exception:
                pass


class TestWorkerHeartbeatStatusFlush:
    """Integration: live-status fields ride the heartbeat into Redis (R-001)."""

    def test_heartbeat_flush_carries_counters(
        self, redis_adapter: RedisAdapter, tmp_path
    ):
        """After processing a message, the next heartbeat flush carries the
        live-status fields; get_consumer_group_workers reads them back."""
        HandlerRegistry.clear()

        @message_handler(message_types=["integ_hb_type"])
        def _hb_handler(msg: Message) -> None:
            return None

        HandlerRegistry.register(_hb_handler)

        group_name = "integ-hb-sub"
        stream_name = "gpmq:messages"
        worker_id = f"{group_name}-worker-0"

        config_dict = {
            "name": group_name,
            "handlers": [
                {
                    "message_types": ["integ_hb_type"],
                    "handler": "dummy.handler",
                }
            ],
            "stream_name": stream_name,
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "redis_password": "123456",
            "redis_db": 15,
            "heartbeat_interval": 300,
            "log_category": "gpmq",
        }

        worker = Worker(
            subscriber_name=group_name,
            stream_name=stream_name,
            message_types=["integ_hb_type"],
            worker_id=worker_id,
            hostname="test-host",
            config_dict=config_dict,
        )
        worker._redis = redis_adapter

        try:
            # The consumer group must exist before XREADGROUP (workers do
            # not create it; the Subscriber does in production).
            redis_adapter.xgroup_create(stream_name, group_name)

            worker._register_self()

            msg_id = redis_adapter.xadd(
                stream_name,
                {
                    "type": "integ_hb_type",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "publisher": "integ-test-pub",
                    "payload": json.dumps({"n": 1}),
                },
            )

            # One loop: reads + processes the message. The first heartbeat
            # flush (counters still zero) happens before processing.
            worker._read_and_process()
            assert worker._messages_processed == 1

            # Force the next throttled flush deterministically.
            worker._last_heartbeat = 0.0
            worker._maybe_update_heartbeat()

            workers = redis_adapter.get_consumer_group_workers(group_name)
            assert len(workers) == 1
            info = workers[0]
            assert info["messages_processed"] == 1
            assert info["last_message_id"] == msg_id
            assert float(info["last_activity"]) > 0.0
        finally:
            redis_adapter.unregister_worker(group_name, worker_id)
            HandlerRegistry.clear()


class TestWorkerContextFinalizerE2E:
    """Integration: a spawned worker process runs the context finalizer on
    graceful stop (real spawn, real Redis, full run() -> finally -> _cleanup()
    chain — unlike the unit tests, which call _cleanup() directly)."""

    def test_spawned_worker_runs_finalizer_on_graceful_stop(
        self, redis_adapter: RedisAdapter, tmp_path
    ):
        """stop_event -> read-loop exit -> finally _cleanup() -> finalizer
        called with the live worker context in the child process."""
        from gpmq.process import use_spawn

        # Production semantics: fresh interpreter per worker (no-op on
        # Windows, where spawn is already the default).
        use_spawn()

        group_name = "integ-fin-sub"
        stream_name = "gpmq:messages"
        worker_id = f"{group_name}-test-host-worker-0"
        marker_path = tmp_path / "finalizer_marker.json"

        config_dict = {
            "name": group_name,
            "handlers": [
                {
                    "message_types": ["type_a"],
                    # Must be a REAL importable handler: run() loads it via
                    # importlib in the child process (the "dummy.handler"
                    # placeholder used by unit configs would not import).
                    "handler": "gpmq.subscriber.examples.on_msg_example_type_a",
                }
            ],
            "stream_name": stream_name,
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "redis_password": "123456",
            "redis_db": 15,
            "heartbeat_interval": 300,
            "enable_audit": False,
            "log_category": "gpmq",
        }

        worker = Worker(
            subscriber_name=group_name,
            stream_name=stream_name,
            message_types=["type_a"],
            worker_id=worker_id,
            hostname="test-host",
            config_dict=config_dict,
            context={
                "worker_context_finalizer": (
                    "tests.subscriber.finalizer_helper.record_finalizer_call"
                ),
                "finalizer_marker_path": str(marker_path),
            },
        )

        try:
            # Workers do not create the consumer group (the Subscriber does
            # in production); XREADGROUP would spin on NOGROUP without it.
            redis_adapter.xgroup_create(stream_name, group_name)

            worker.start()

            # Wait until the child process has booted, connected, and
            # registered itself (proves run() passed prepare_worker_context).
            deadline = time.monotonic() + 30
            registered = False
            while time.monotonic() < deadline:
                if not worker.is_alive():
                    break
                info = {
                    w["worker_id"]
                    for w in redis_adapter.get_consumer_group_workers(group_name)
                }
                if worker_id in info:
                    registered = True
                    break
                time.sleep(0.2)
            assert registered, (
                f"worker never registered itself in Redis (exitcode={worker.exitcode})"
            )

            # Graceful stop: read loop notices the event within one 5s
            # XREADGROUP block, then run()'s finally calls _cleanup().
            worker._stop_event.set()
            worker.join(timeout=30)

            assert not worker.is_alive()
            assert worker.exitcode == 0, "worker must exit cleanly, not terminated"

            # The finalizer ran in the CHILD process and wrote the marker
            # from live-context data.
            assert marker_path.exists(), (
                "finalizer marker not written — finalizer never ran"
            )
            payload = json.loads(marker_path.read_text(encoding="utf-8"))
            assert payload["worker_id"] == worker_id
            assert payload["subscriber_name"] == group_name
        finally:
            if worker.is_alive():
                worker._stop_event.set()
                worker.join(timeout=10)
            try:
                redis_adapter.clear_stream(stream_name)
                redis_adapter.xgroup_destroy(stream_name, group_name)
            except Exception:
                pass
