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
