"""Tests for Publisher."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from gpmq.config import GPMQConfig
from gpmq.models.message import Message
from gpmq.models.result import ProcessStatus
from gpmq.publisher.publisher import Publisher


@pytest.fixture
def mock_config() -> GPMQConfig:
    """Create test config."""
    return GPMQConfig(
        redis_host="localhost",
        stream_name="gpmq:messages",
        heartbeat_interval=10,
        registry_key_prefix="gpmq:",
    )


@pytest.fixture
def mock_redis() -> MagicMock:
    """Create mock Redis adapter."""
    return MagicMock()


@pytest.fixture
def mock_audit() -> MagicMock:
    """Create mock audit store."""
    return MagicMock()


@pytest.fixture
def publisher(mock_redis, mock_audit, mock_config) -> Publisher:
    """Create Publisher with mocked dependencies."""
    return Publisher(mock_redis, mock_audit, mock_config)


def _make_message(message_type: str = "test_type", publisher: str = "test_pub") -> Message:
    """Helper to create a test message."""
    return Message(
        id="pending",
        type=message_type,
        timestamp=datetime.now(timezone.utc),
        publisher=publisher,
        payload={"key": "value"},
    )


class TestPublisherPublishMessage:
    """Tests for publish_message() method."""

    def test_publishes_message_to_stream(self, publisher, mock_redis, mock_audit):
        """Should xadd message fields to Redis Stream."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        message_id, handler = publisher.publish_message(msg)

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args[0][0] == "gpmq:messages"
        fields = call_args[0][1]
        assert fields["type"] == "test_type"
        assert fields["publisher"] == "test_pub"
        assert "payload" in fields
        assert message_id == "1234567890-0"

    def test_updates_message_id_after_xadd(self, publisher, mock_redis, mock_audit):
        """Should update message.id with Redis-generated ID."""
        mock_redis.xadd.return_value = "9876543210-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        message_id, _ = publisher.publish_message(msg)

        assert msg.id == "9876543210-0"
        assert message_id == "9876543210-0"

    def test_saves_message_to_audit(self, publisher, mock_redis, mock_audit):
        """Should save message to audit store."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        publisher.publish_message(msg)

        mock_audit.save_message.assert_called_once_with(msg)

    def test_creates_handler_with_matching_subscribers(self, publisher, mock_redis, mock_audit):
        """Should create ResultHandler for subscribers matching message type."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = ["sub-a", "sub-b"]
        mock_redis.get_subscriber_info.side_effect = [
            {"message_types": ["test_type", "other"]},
            {"message_types": ["other_type"]},
        ]

        msg = _make_message("test_type")
        message_id, handler = publisher.publish_message(msg)

        assert handler.message_id == "1234567890-0"

    def test_no_matching_subscribers(self, publisher, mock_redis, mock_audit):
        """Should create handler with empty subscriber list when no matches."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = ["sub-a"]
        mock_redis.get_subscriber_info.return_value = {"message_types": ["other_type"]}

        msg = _make_message("test_type")
        message_id, handler = publisher.publish_message(msg)

        assert message_id == "1234567890-0"
        assert handler.is_complete()

    def test_includes_correlation_id_in_fields(self, publisher, mock_redis, mock_audit):
        """Should include correlation_id in stream fields when present."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        msg.correlation_id = "corr-123"
        publisher.publish_message(msg)

        fields = mock_redis.xadd.call_args[0][1]
        assert fields["correlation_id"] == "corr-123"

    def test_no_correlation_id_in_fields(self, publisher, mock_redis, mock_audit):
        """Should not include correlation_id when None."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        publisher.publish_message(msg)

        fields = mock_redis.xadd.call_args[0][1]
        assert "correlation_id" not in fields


class TestPublisherGetActiveSubscribers:
    """Tests for get_active_subscribers() method."""

    def test_returns_active_subscribers(self, publisher, mock_redis, mock_config):
        """Should delegate to redis adapter with heartbeat timeout."""
        mock_redis.get_active_subscribers.return_value = ["sub-a", "sub-b"]

        result = publisher.get_active_subscribers()

        assert result == ["sub-a", "sub-b"]
        mock_redis.get_active_subscribers.assert_called_once_with(
            timeout=mock_config.heartbeat_interval * 3
        )

    def test_empty_when_no_subscribers(self, publisher, mock_redis):
        """Should return empty list when no active subscribers."""
        mock_redis.get_active_subscribers.return_value = []

        result = publisher.get_active_subscribers()
        assert result == []


class TestPublisherWaitForResults:
    """Tests for wait_for_results() convenience method."""

    def test_creates_handler_and_waits(self, publisher, mock_redis):
        """Should create ResultHandler and call wait."""
        mock_redis.get_message.return_value = None

        results = publisher.wait_for_results("msg-1", ["sub-a"], timeout=5.0)

        assert "sub-a" in results
        assert results["sub-a"].status == ProcessStatus.TIMEOUT


def test_publisher_handles_none_audit():
    """Publisher should skip audit operations when audit_store is None."""
    from gpmq.config import PublisherConfig

    config = PublisherConfig(
        publisher_name="test",
        redis_host="127.0.0.1",
        redis_password="123456",
    )
    redis_mock = MagicMock()
    redis_mock.xadd.return_value = "1234567890-0"
    redis_mock.get_active_subscribers.return_value = []

    publisher = Publisher(redis_mock, None, config)
    msg = Message(
        id="pending",
        type="test",
        timestamp=datetime.now(timezone.utc),
        publisher="test",
        payload={},
    )
    # Should not raise
    msg_id, handler = publisher.publish_message(msg)
    assert msg_id == "1234567890-0"


class TestPublisherSendMessage:
    """Tests for fire-and-forget send_message() method."""

    def test_returns_message_id(self, publisher, mock_redis, mock_audit):
        """Should return message_id without creating ResultHandler."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = ["sub-a"]
        mock_redis.get_subscriber_info.return_value = {"message_types": ["test_type"]}

        msg = _make_message("test_type")
        result = publisher.send_message(msg)

        assert result == "1234567890-0"

    def test_xadds_to_stream(self, publisher, mock_redis, mock_audit):
        """Should call xadd with correct stream and fields."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        publisher.send_message(msg)

        mock_redis.xadd.assert_called_once()
        fields = mock_redis.xadd.call_args[0][1]
        assert fields["type"] == "test_type"

    def test_updates_message_id(self, publisher, mock_redis):
        """Should update message.id with Redis-generated ID."""
        mock_redis.xadd.return_value = "9999999999-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        publisher.send_message(msg)

        assert msg.id == "9999999999-0"

    def test_saves_to_audit(self, publisher, mock_redis, mock_audit):
        """Should save message to audit store."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        msg = _make_message()
        publisher.send_message(msg)

        mock_audit.save_message.assert_called_once_with(msg)

    def test_skips_audit_when_none(self, mock_redis):
        """Should not fail when audit_store is None."""
        from gpmq.config import PublisherConfig

        config = PublisherConfig(
            publisher_name="test",
            redis_host="localhost",
        )
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = []

        pub = Publisher(mock_redis, None, config)
        msg = _make_message()
        result = pub.send_message(msg)

        assert result == "1234567890-0"

    def test_does_not_subscribe_to_pubsub(self, publisher, mock_redis):
        """Should never call subscribe on redis adapter."""
        mock_redis.xadd.return_value = "1234567890-0"
        mock_redis.get_active_subscribers.return_value = ["sub-a"]
        mock_redis.get_subscriber_info.return_value = {"message_types": ["test_type"]}

        msg = _make_message("test_type")
        publisher.send_message(msg)

        mock_redis.subscribe.assert_not_called()
