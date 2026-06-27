"""Tests for Publisher."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from gpmq.config import GPMQConfig
from gpmq.exceptions import InvalidTargetSubscriberError
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


class TestPublisherTargetSubscribersFields:
    """Tests for target_subscribers serialization in _message_to_fields."""

    def test_fields_include_target_subscribers_when_set(self):
        """Should serialize target_subscribers as JSON when present."""
        msg = _make_message()
        msg.target_subscribers = ["sub-a", "sub-b"]

        fields = Publisher._message_to_fields(msg)

        assert json.loads(fields["target_subscribers"]) == ["sub-a", "sub-b"]

    def test_fields_omit_target_subscribers_when_none(self):
        """Should NOT include target_subscribers field when None."""
        msg = _make_message()

        fields = Publisher._message_to_fields(msg)

        assert "target_subscribers" not in fields


class TestPublisherValidateTargetSubscribers:
    """Tests for Publisher._validate_target_subscribers()."""

    def test_passes_when_all_targets_handle_type(self, publisher, mock_redis):
        mock_redis.get_subscriber_info.side_effect = [
            {"message_types": ["order_created"]},
            {"message_types": ["order_created", "other"]},
        ]
        # Should not raise
        publisher._validate_target_subscribers("order_created", ["sub-a", "sub-b"])

    def test_raises_for_unknown_subscriber(self, publisher, mock_redis):
        mock_redis.get_subscriber_info.return_value = None

        with pytest.raises(InvalidTargetSubscriberError) as exc_info:
            publisher._validate_target_subscribers("order_created", ["ghost-sub"])
        assert ("ghost-sub", "not registered") in exc_info.value.failures

    def test_raises_for_subscriber_not_handling_type(self, publisher, mock_redis):
        mock_redis.get_subscriber_info.return_value = {"message_types": ["other_type"]}

        with pytest.raises(InvalidTargetSubscriberError) as exc_info:
            publisher._validate_target_subscribers("order_created", ["wrong-sub"])
        assert ("wrong-sub", "does not handle message type") in exc_info.value.failures

    def test_aggregates_multiple_failures(self, publisher, mock_redis):
        mock_redis.get_subscriber_info.side_effect = [
            None,  # ghost-sub: not registered
            {"message_types": ["other"]},  # wrong-sub: wrong type
            {"message_types": ["order_created"]},  # ok-sub: OK
        ]
        with pytest.raises(InvalidTargetSubscriberError) as exc_info:
            publisher._validate_target_subscribers(
                "order_created", ["ghost-sub", "wrong-sub", "ok-sub"]
            )
        failures = exc_info.value.failures
        assert len(failures) == 2
        assert ("ghost-sub", "not registered") in failures
        assert ("wrong-sub", "does not handle message type") in failures


class TestPublisherPublishMessageTargeting:
    """Tests for target_subscribers handling in publish_message()."""

    def test_publish_message_uses_target_list_as_expected(self, publisher, mock_redis):
        """With target_subscribers set, ResultHandler should expect exactly the targets."""
        mock_redis.xadd.return_value = "111-0"
        mock_redis.get_subscriber_info.return_value = {
            "message_types": ["test_type"]
        }
        # get_active_subscribers should NOT be consulted when targeting
        mock_redis.get_active_subscribers.return_value = ["sub-a", "sub-b", "sub-c"]

        msg = _make_message("test_type")
        msg.target_subscribers = ["sub-a", "sub-b"]

        _, handler = publisher.publish_message(msg)

        mock_redis.xadd.assert_called_once()
        # Validation queried only the two targets
        assert mock_redis.get_subscriber_info.call_count == 2
        assert mock_redis.get_active_subscribers.call_count == 0
        # Fields include target_subscribers
        fields = mock_redis.xadd.call_args[0][1]
        assert json.loads(fields["target_subscribers"]) == ["sub-a", "sub-b"]

    def test_publish_message_validates_before_xadd(self, publisher, mock_redis):
        """Validation failure must prevent xadd entirely."""
        mock_redis.get_subscriber_info.return_value = {"message_types": ["other_type"]}

        msg = _make_message("test_type")
        msg.target_subscribers = ["bad-sub"]

        with pytest.raises(InvalidTargetSubscriberError):
            publisher.publish_message(msg)

        mock_redis.xadd.assert_not_called()

    def test_publish_message_without_target_uses_legacy_path(self, publisher, mock_redis):
        """No target_subscribers -> original _get_matching_subscribers path."""
        mock_redis.xadd.return_value = "222-0"
        mock_redis.get_active_subscribers.return_value = ["sub-a"]
        mock_redis.get_subscriber_info.return_value = {"message_types": ["test_type"]}

        msg = _make_message("test_type")  # target_subscribers is None
        publisher.publish_message(msg)

        assert mock_redis.get_active_subscribers.call_count == 1
        # Fields must NOT carry target_subscribers
        fields = mock_redis.xadd.call_args[0][1]
        assert "target_subscribers" not in fields


class TestPublisherSendMessageTargeting:
    """Tests for target_subscribers validation in send_message()."""

    def test_send_message_validates_before_xadd(self, publisher, mock_redis):
        mock_redis.xadd.return_value = "333-0"
        mock_redis.get_subscriber_info.return_value = {"message_types": ["test_type"]}

        msg = _make_message("test_type")
        msg.target_subscribers = ["sub-a"]

        publisher.send_message(msg)

        mock_redis.get_subscriber_info.assert_called_once_with("sub-a")
        mock_redis.xadd.assert_called_once()

    def test_send_message_validation_failure_prevents_xadd(self, publisher, mock_redis):
        mock_redis.get_subscriber_info.return_value = None

        msg = _make_message("test_type")
        msg.target_subscribers = ["ghost-sub"]

        with pytest.raises(InvalidTargetSubscriberError):
            publisher.send_message(msg)

        mock_redis.xadd.assert_not_called()
