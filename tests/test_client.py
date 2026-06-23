"""Tests for GPMQClient."""

from __future__ import annotations

from typing import TYPE_CHECKING

from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    from gpmq.client import GPMQClient

import pytest

from gpmq.config import GPMQConfig, PublisherConfig
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.publisher.result_handler import ResultHandler


@pytest.fixture
def config() -> PublisherConfig:
    """Create test publisher config."""
    return PublisherConfig(
        redis_host="localhost",
        publisher_name="test_publisher",
        subscriber_timeout=30.0,
    )


@pytest.fixture
def client(config) -> "GPMQClient":
    """Create GPMQClient with mocked dependencies."""
    from gpmq.client import GPMQClient

    with (
        patch("gpmq.client.RedisAdapter") as mock_redis_cls,
        patch("gpmq.client.AuditStore") as mock_audit_cls,
        patch("gpmq.client.Publisher") as mock_publisher_cls,
    ):
        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_publisher = MagicMock()
        mock_redis_cls.return_value = mock_redis
        mock_audit_cls.return_value = mock_audit
        mock_publisher_cls.return_value = mock_publisher

        c = GPMQClient(config)
        # Attach mocks for test access
        c._redis = mock_redis
        c._audit = mock_audit
        c._publisher = mock_publisher
        yield c


class TestGPMQClientInit:
    """Tests for GPMQClient constructor."""

    def test_creates_components(self, config):
        """Should create Redis adapter, audit store, and publisher."""
        from gpmq.client import GPMQClient

        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis_cls.return_value = MagicMock()
            mock_audit_cls.return_value = MagicMock()
            mock_publisher_cls.return_value = MagicMock()

            client = GPMQClient(config)

        assert client._config is config
        assert client._redis is not None
        assert client._audit is not None
        assert client._publisher is not None

    def test_extracts_publisher_name_from_config(self, config):
        """Should extract publisher_name from config."""
        from gpmq.client import GPMQClient

        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis_cls.return_value = MagicMock()
            mock_audit_cls.return_value = MagicMock()
            mock_publisher_cls.return_value = MagicMock()

            client = GPMQClient(config)

        assert client._publisher_name == "test_publisher"

    def test_defaults_publisher_name_for_base_config(self):
        """Should default to 'anonymous' for GPMQConfig without publisher_name."""
        from gpmq.client import GPMQClient

        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis_cls.return_value = MagicMock()
            mock_audit_cls.return_value = MagicMock()
            mock_publisher_cls.return_value = MagicMock()

            base_config = GPMQConfig(redis_host="localhost")
            client = GPMQClient(base_config)

        assert client._publisher_name == "anonymous"


class TestGPMQClientConnect:
    """Tests for connect/close lifecycle."""

    def test_connect_delegates_to_redis(self, client):
        """Should call redis_adapter.connect()."""
        client.connect()

        client._redis.connect.assert_called_once()

    def test_close_disconnects_redis(self, client):
        """Should call redis_adapter.disconnect() and audit.close()."""
        client.connect()
        client.close()

        client._redis.disconnect.assert_called_once()
        client._audit.close.assert_called_once()

    def test_close_without_connect(self, client):
        """Should not error when close called without connect."""
        client.close()  # Should not raise


class TestGPMQClientContextManager:
    """Tests for context manager support."""

    def test_enter_calls_connect_and_returns_self(self, client):
        """__enter__ should call connect() and return self."""
        result = client.__enter__()

        client._redis.connect.assert_called_once()
        assert result is client

    def test_exit_calls_close(self, client):
        """__exit__ should call close()."""
        client.connect()
        client.__exit__(None, None, None)

        client._redis.disconnect.assert_called_once()
        client._audit.close.assert_called_once()

    def test_with_statement_lifecycle(self, config):
        """with statement should call connect on enter, close on exit."""
        from gpmq.client import GPMQClient

        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis = MagicMock()
            mock_audit = MagicMock()
            mock_redis_cls.return_value = mock_redis
            mock_audit_cls.return_value = mock_audit
            mock_publisher_cls.return_value = MagicMock()

            with GPMQClient(config) as c:
                assert c._connected is True
                mock_redis.connect.assert_called_once()

            mock_redis.disconnect.assert_called_once()
            mock_audit.close.assert_called_once()

    def test_exit_suppresses_no_exceptions(self, client):
        """__exit__ should not suppress exceptions (returns None)."""
        result = client.__exit__(None, None, None)
        assert result is None


class TestGPMQClientPublish:
    """Tests for sync publish() method."""

    def test_publish_returns_results(self, client):
        """Should return dict of subscriber results."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {
            "sub-a": ProcessResult(
                message_id="msg-1",
                status=ProcessStatus.SUCCESS,
                subscriber_name="sub-a",
                processing_time=0.1,
            )
        }
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        results = client.publish("test_type", {"key": "val"})

        assert "sub-a" in results
        assert results["sub-a"].status == ProcessStatus.SUCCESS

    def test_publish_uses_config_timeout(self, client):
        """Should use config subscriber_timeout when no timeout given."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {}
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        client.publish("test_type", {"key": "val"})

        mock_handler.wait.assert_called_once_with(timeout=30.0)

    def test_publish_with_custom_timeout(self, client):
        """Should use custom timeout when provided."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {}
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        client.publish("test_type", {"key": "val"}, timeout=10.0)

        mock_handler.wait.assert_called_once_with(timeout=10.0)

    def test_publish_with_correlation_id(self, client):
        """Should pass correlation_id to message."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {}
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        client.publish("test_type", {"key": "val"}, correlation_id="corr-1")

        msg_arg = client._publisher.publish_message.call_args[0][0]
        assert msg_arg.correlation_id == "corr-1"


class TestGPMQClientPublishAsync:
    """Tests for async publish_async() method."""

    def test_returns_result_handler(self, client):
        """Should return ResultHandler without waiting."""
        mock_handler = MagicMock(spec=ResultHandler)
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        handler = client.publish_async("test_type", {"key": "val"})

        assert handler is mock_handler
        mock_handler.wait.assert_not_called()

    def test_creates_message_with_correct_fields(self, client):
        """Should create Message with correct type, publisher, and payload."""
        mock_handler = MagicMock(spec=ResultHandler)
        client._publisher.publish_message.return_value = ("msg-1", mock_handler)

        client.publish_async("test_type", {"key": "val"})

        msg_arg = client._publisher.publish_message.call_args[0][0]
        assert msg_arg.type == "test_type"
        assert msg_arg.publisher == "test_publisher"
        assert msg_arg.payload == {"key": "val"}


class TestGPMQClientSend:
    """Tests for fire-and-forget send() method."""

    def test_returns_message_id(self, client):
        """Should return message_id string."""
        client._publisher.send_message.return_value = "msg-123"

        result = client.send("test_type", {"key": "val"})

        assert result == "msg-123"

    def test_creates_message_with_correct_fields(self, client):
        """Should create Message with correct type, publisher, and payload."""
        client._publisher.send_message.return_value = "msg-123"

        client.send("test_type", {"key": "val"})

        msg_arg = client._publisher.send_message.call_args[0][0]
        assert msg_arg.type == "test_type"
        assert msg_arg.publisher == "test_publisher"
        assert msg_arg.payload == {"key": "val"}

    def test_passes_correlation_id(self, client):
        """Should pass correlation_id to message."""
        client._publisher.send_message.return_value = "msg-123"

        client.send("test_type", {"key": "val"}, correlation_id="corr-1")

        msg_arg = client._publisher.send_message.call_args[0][0]
        assert msg_arg.correlation_id == "corr-1"

    def test_calls_publisher_send_message_not_publish_message(self, client):
        """Should call send_message, not publish_message."""
        client._publisher.send_message.return_value = "msg-123"

        client.send("test_type", {"key": "val"})

        client._publisher.send_message.assert_called_once()
        client._publisher.publish_message.assert_not_called()


class TestGPMQClientWaitAll:
    """Tests for wait_all() method."""

    def test_waits_for_all_handlers(self, client):
        """Should call wait on all handlers and save results."""
        mock_handler1 = MagicMock(spec=ResultHandler)
        mock_handler1.wait.return_value = {
            "sub-a": ProcessResult(
                message_id="msg-1",
                status=ProcessStatus.SUCCESS,
                subscriber_name="sub-a",
                processing_time=0.1,
            )
        }
        mock_handler2 = MagicMock(spec=ResultHandler)
        mock_handler2.wait.return_value = {}

        results = client.wait_all([mock_handler1, mock_handler2], timeout=10.0)

        assert len(results) == 2
        mock_handler1.wait.assert_called_once_with(
            timeout=10.0, allow_timeout_retry=None
        )
        mock_handler2.wait.assert_called_once_with(
            timeout=10.0, allow_timeout_retry=None
        )

    def test_passes_allow_timeout_retry_to_handler_wait(self, client):
        """Should pass allow_timeout_retry=True to each handler.wait()."""
        mock_handler1 = MagicMock(spec=ResultHandler)
        mock_handler1.wait.return_value = {}
        mock_handler2 = MagicMock(spec=ResultHandler)
        mock_handler2.wait.return_value = {}

        client.wait_all(
            [mock_handler1, mock_handler2], timeout=5.0, allow_timeout_retry=True
        )

        mock_handler1.wait.assert_called_once_with(
            timeout=5.0, allow_timeout_retry=True
        )
        mock_handler2.wait.assert_called_once_with(
            timeout=5.0, allow_timeout_retry=True
        )

    def test_allow_timeout_retry_false_passes_to_handler(self, client):
        """Should pass allow_timeout_retry=False to each handler.wait()."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {}

        client.wait_all([mock_handler], timeout=5.0, allow_timeout_retry=False)

        mock_handler.wait.assert_called_once_with(
            timeout=5.0, allow_timeout_retry=False
        )

    def test_allow_timeout_retry_none_passes_to_handler(self, client):
        """Should pass allow_timeout_retry=None to handler.wait() when not specified."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {}

        client.wait_all([mock_handler], timeout=5.0)

        mock_handler.wait.assert_called_once_with(timeout=5.0, allow_timeout_retry=None)

    def test_allow_timeout_retry_returns_temporary_timeout_results(self, client):
        """With allow_timeout_retry=True, should return results without finalizing handlers."""
        mock_handler = MagicMock(spec=ResultHandler)
        mock_handler.wait.return_value = {
            "sub-a": ProcessResult(
                message_id="msg-1",
                status=ProcessStatus.TIMEOUT,
                subscriber_name="sub-a",
                processing_time=5.0,
                timeout_seconds=5.0,
            )
        }

        results = client.wait_all([mock_handler], timeout=5.0, allow_timeout_retry=True)

        assert len(results) == 1
        assert results[0]["sub-a"].status == ProcessStatus.TIMEOUT


class TestGPMQClientConnectCloseSymmetry:
    """Tests for connect/close reentrancy and audit store symmetry."""

    def test_connect_also_connects_audit(self, client):
        """connect() should call both redis.connect() and audit.connect()."""
        client.connect()

        client._redis.connect.assert_called_once()
        client._audit.connect.assert_called_once()

    def test_connect_is_reentrant(self, client):
        """Double connect() should only call redis.connect() once."""
        client.connect()
        client.connect()

        client._redis.connect.assert_called_once()

    def test_close_is_reentrant(self, client):
        """Double close() should only call redis.disconnect() once."""
        client.connect()
        client.close()
        client.close()

        client._redis.disconnect.assert_called_once()

    def test_close_without_connect_closes_audit(self, client):
        """close() without connect() should still close audit."""
        client.close()

        client._audit.close.assert_called_once()
        client._redis.disconnect.assert_not_called()

    def test_context_manager_connects_and_closes_audit(self, config):
        """with statement should connect both redis and audit, then close both."""
        from gpmq.client import GPMQClient

        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis = MagicMock()
            mock_audit = MagicMock()
            mock_redis_cls.return_value = mock_redis
            mock_audit_cls.return_value = mock_audit
            mock_publisher_cls.return_value = MagicMock()

            with GPMQClient(config) as c:
                assert c._connected is True
                mock_redis.connect.assert_called_once()
                mock_audit.connect.assert_called_once()

            mock_redis.disconnect.assert_called_once()
            mock_audit.close.assert_called_once()


class TestGPMQClientGetAuditStore:
    """Tests for get_audit_store() method."""

    def test_returns_audit_store(self, client):
        """Should return the audit store instance."""
        assert client.get_audit_store() is client._audit


def test_client_no_audit_when_disabled():
    """Client should not create AuditStore when enable_audit=False."""
    from gpmq.client import GPMQClient

    config = PublisherConfig(
        publisher_name="test",
        enable_audit=False,
        redis_host="127.0.0.1",
        redis_password="123456",
    )
    client = GPMQClient(config)
    assert client._audit is None


def test_client_has_audit_when_enabled():
    """Client should create AuditStore when enable_audit=True (default)."""
    from gpmq.client import GPMQClient

    config = PublisherConfig(
        publisher_name="test",
        redis_host="127.0.0.1",
        redis_password="123456",
    )
    client = GPMQClient(config)
    assert client._audit is not None
    client.close()


class TestGPMQClientGetConsumerGroupWorkers:
    """Tests for get_consumer_group_workers() method."""

    def test_returns_consumer_group_workers(self, client):
        """Should return ConsumerGroupWorkers with worker info."""
        from gpmq.models.worker_info import ConsumerGroupWorkers

        client._redis.get_consumer_group_workers.return_value = [
            {
                "worker_id": "notifier-server1-worker-0",
                "hostname": "server1",
                "pid": "12345",
                "status": "running",
                "started_at": "1713500000.0",
                "last_heartbeat": "1713500100.0",
            },
        ]

        result = client.get_consumer_group_workers("notifier")

        assert isinstance(result, ConsumerGroupWorkers)
        assert result.subscriber_name == "notifier"
        assert result.worker_count == 1
        assert result.workers[0].worker_id == "notifier-server1-worker-0"
        assert result.workers[0].hostname == "server1"

    def test_marks_stale_workers(self, client):
        """Workers with stale heartbeats should be marked as 'stale'."""
        import time

        old_ts = time.time() - 120  # 2 minutes ago
        client._redis.get_consumer_group_workers.return_value = [
            {
                "worker_id": "sub-host-w0",
                "hostname": "host",
                "pid": "100",
                "status": "running",
                "started_at": str(old_ts),
                "last_heartbeat": str(old_ts),
            },
        ]

        result = client.get_consumer_group_workers("sub")
        assert result.workers[0].status == "stale"

    def test_returns_empty_for_no_workers(self, client):
        """Should return empty ConsumerGroupWorkers when no workers found."""
        from gpmq.models.worker_info import ConsumerGroupWorkers

        client._redis.get_consumer_group_workers.return_value = []

        result = client.get_consumer_group_workers("nonexistent")
        assert isinstance(result, ConsumerGroupWorkers)
        assert result.worker_count == 0
        assert result.workers == []


class TestGPMQClientAsyncBatch:
    """Tests for async_batch() factory method."""

    def test_returns_async_batch_handler(self, client):
        """Should return GPMQAsyncBatchHandler instance."""
        from gpmq.async_batch import GPMQAsyncBatchHandler

        batch = client.async_batch()
        assert isinstance(batch, GPMQAsyncBatchHandler)
        assert batch._client is client

    def test_passes_progress_and_timeout(self, client):
        """Should pass progress and timeout to handler."""
        from gpmq.progress import GPMQProgress

        progress = GPMQProgress()
        batch = client.async_batch(progress=progress, timeout=30.0)
        assert batch._progress is progress
        assert batch._timeout == 30.0

    def test_defaults_to_none(self, client):
        """Should default progress and timeout to None."""
        batch = client.async_batch()
        assert batch._progress is None
        assert batch._timeout is None

    def test_queue_size_from_config(self, client):
        """Should use config async_batch_queue_size when queue_size not given."""
        batch = client.async_batch()
        assert batch._queue_size == client._config.async_batch_queue_size

    def test_explicit_queue_size_overrides_config(self, client):
        """Explicit queue_size should override config default."""
        batch = client.async_batch(queue_size=16)
        assert batch._queue_size == 16

    def test_queue_size_fallback_for_base_config(self):
        """Should fallback to 32 when config has no async_batch_queue_size."""
        from gpmq.client import GPMQClient

        base_config = GPMQConfig(redis_host="localhost")
        with (
            patch("gpmq.client.RedisAdapter") as mock_redis_cls,
            patch("gpmq.client.AuditStore") as mock_audit_cls,
            patch("gpmq.client.Publisher") as mock_publisher_cls,
        ):
            mock_redis_cls.return_value = MagicMock()
            mock_audit_cls.return_value = MagicMock()
            mock_publisher_cls.return_value = MagicMock()

            c = GPMQClient(base_config)
            batch = c.async_batch()
            assert batch._queue_size == 32
