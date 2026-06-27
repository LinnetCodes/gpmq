"""Tests for GPMQ exception classes."""

from gpmq.exceptions import (
    DuplicateMessageTypeError,
    GPMQError,
    HandlerTypeMismatchError,
    InvalidMessageError,
    InvalidTargetSubscriberError,
    RedisConnectionError,
)


class TestGPMQError:
    """Tests for base GPMQError."""

    def test_is_exception(self):
        """GPMQError should be a subclass of Exception."""
        assert issubclass(GPMQError, Exception)

    def test_message(self):
        """GPMQError should store and return the error message."""
        error = GPMQError("Test error")
        assert str(error) == "Test error"


class TestRedisConnectionError:
    """Tests for RedisConnectionError."""

    def test_attributes(self):
        """RedisConnectionError should store connection details."""
        error = RedisConnectionError(
            host="localhost", port=6379, attempts=3, message="Connection refused"
        )
        assert error.host == "localhost"
        assert error.port == 6379
        assert error.attempts == 3

    def test_message_format(self):
        """RedisConnectionError should format message with connection details."""
        error = RedisConnectionError(
            host="localhost", port=6379, attempts=3, message="Connection refused"
        )
        assert "localhost:6379" in str(error)
        assert "3 attempts" in str(error)


class TestInvalidMessageError:
    """Tests for InvalidMessageError."""

    def test_attributes(self):
        """InvalidMessageError should store message type and reason."""
        error = InvalidMessageError(
            message_type="user_created", reason="Missing required field"
        )
        assert error.message_type == "user_created"
        assert error.reason == "Missing required field"

    def test_message_format(self):
        """InvalidMessageError should format message with type and reason."""
        error = InvalidMessageError(
            message_type="user_created", reason="Missing required field"
        )
        assert "user_created" in str(error)
        assert "Missing required field" in str(error)

    def test_message_without_detail(self):
        """InvalidMessageError should handle empty message type."""
        error = InvalidMessageError(message_type="", reason="Missing required field")
        assert str(error) == "Invalid message (type=): Missing required field"


class TestDuplicateMessageTypeError:
    """Tests for DuplicateMessageTypeError."""

    def test_attributes(self):
        """DuplicateMessageTypeError should store subscriber and message type."""
        error = DuplicateMessageTypeError(
            subscriber_name="data_processor", message_type="user_created"
        )
        assert error.subscriber_name == "data_processor"
        assert error.message_type == "user_created"

    def test_message_format(self):
        """DuplicateMessageTypeError should format message with details."""
        error = DuplicateMessageTypeError(
            subscriber_name="data_processor", message_type="user_created"
        )
        assert "user_created" in str(error)
        assert "data_processor" in str(error)


class TestHandlerTypeMismatchError:
    """Tests for HandlerTypeMismatchError."""

    def test_attributes(self):
        """HandlerTypeMismatchError should store all type information."""
        error = HandlerTypeMismatchError(
            handler_path="myapp.handlers.on_user",
            declared_types=["user_created", "user_updated"],
            config_types=["user_created", "order_created"],
            conflict_types=["order_created"],
        )
        assert error.handler_path == "myapp.handlers.on_user"
        assert error.declared_types == ["user_created", "user_updated"]
        assert error.config_types == ["user_created", "order_created"]
        assert error.conflict_types == ["order_created"]

    def test_message_format(self):
        """HandlerTypeMismatchError should format message with all details."""
        error = HandlerTypeMismatchError(
            handler_path="myapp.handlers.on_user",
            declared_types=["user_created", "user_updated"],
            config_types=["user_created", "order_created"],
            conflict_types=["order_created"],
        )
        assert "myapp.handlers.on_user" in str(error)
        assert "order_created" in str(error)


class TestInvalidTargetSubscriberError:
    """Tests for InvalidTargetSubscriberError."""

    def test_is_gpmq_error(self):
        assert issubclass(InvalidTargetSubscriberError, GPMQError)

    def test_attributes(self):
        failures = [("ghost-sub", "not registered"), ("wrong-sub", "does not handle message type")]
        error = InvalidTargetSubscriberError("order_created", failures)
        assert error.message_type == "order_created"
        assert error.failures == failures

    def test_message_format_lists_all_failures(self):
        error = InvalidTargetSubscriberError(
            "order_created",
            [("ghost-sub", "not registered"), ("wrong-sub", "does not handle message type")],
        )
        msg = str(error)
        assert "order_created" in msg
        assert "ghost-sub" in msg
        assert "not registered" in msg
        assert "wrong-sub" in msg
        assert "does not handle message type" in msg

    def test_message_format_single_failure(self):
        error = InvalidTargetSubscriberError("ping", [("a", "not registered")])
        assert "ping" in str(error)
        assert "a" in str(error)
