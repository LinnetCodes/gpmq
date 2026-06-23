"""Tests for subscriber handler module."""

from __future__ import annotations

import pytest

from gpmq.exceptions import DuplicateMessageTypeError
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.subscriber.handler import HandlerRegistry, message_handler

# --- Helper functions ---


def _make_message(
    message_type: str = "test_type", payload: dict | None = None
) -> Message:
    """Create a test Message."""
    from datetime import datetime, timezone

    return Message(
        id="msg-1",
        type=message_type,
        timestamp=datetime.now(timezone.utc),
        publisher="test-publisher",
        payload=payload or {"key": "value"},
    )


# --- @message_handler decorator tests ---


class TestMessageHandlerDecorator:
    """Tests for the @message_handler decorator."""

    def test_attaches_message_types_attribute(self):
        """Decorator should attach _gpmq_message_types to function."""

        @message_handler(message_types=["user_created"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        assert handler._gpmq_message_types == ["user_created"]

    def test_preserves_function_identity(self):
        """Decorator should not wrap the function — return it unchanged."""

        @message_handler(message_types=["order_placed"])
        def my_handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        assert my_handler.__name__ == "my_handler"

    def test_multiple_message_types(self):
        """Decorator should accept multiple message types."""

        @message_handler(message_types=["type_a", "type_b", "type_c"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        assert handler._gpmq_message_types == ["type_a", "type_b", "type_c"]

    def test_function_still_callable(self):
        """Decorated function should still be callable."""

        @message_handler(message_types=["test"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.05,
            )

        msg = _make_message()
        result = handler(msg)
        assert result.status == ProcessStatus.SUCCESS
        assert result.message_id == "msg-1"


class TestHandlerRegistryRegister:
    """Tests for HandlerRegistry.register()."""

    def setup_method(self):
        HandlerRegistry.clear()

    def teardown_method(self):
        HandlerRegistry.clear()

    def test_register_single_handler(self):

        @message_handler(message_types=["email_sent"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler)
        assert HandlerRegistry.get_handler("email_sent") is handler

    def test_register_multiple_handlers_different_types(self):

        @message_handler(message_types=["type_a"])
        def handler_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        @message_handler(message_types=["type_b"])
        def handler_b(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler_a)
        HandlerRegistry.register(handler_b)

        assert HandlerRegistry.get_handler("type_a") is handler_a
        assert HandlerRegistry.get_handler("type_b") is handler_b

    def test_register_duplicate_type_raises_error(self):

        @message_handler(message_types=["user_created"])
        def handler_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        @message_handler(message_types=["user_created"])
        def handler_b(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler_a)

        with pytest.raises(DuplicateMessageTypeError):
            HandlerRegistry.register(handler_b)

    def test_register_handler_without_decorator_raises_error(self):

        def plain_handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        with pytest.raises(AttributeError):
            HandlerRegistry.register(plain_handler)

    def test_register_empty_types_raises_error(self):
        with pytest.raises(ValueError):

            @message_handler(message_types=[])
            def handler(msg: Message) -> ProcessResult:
                return ProcessResult(
                    message_id=msg.id,
                    status=ProcessStatus.SUCCESS,
                    subscriber_name="test",
                    processing_time=0.01,
                )


class TestHandlerRegistryGetHandler:
    """Tests for HandlerRegistry.get_handler()."""

    def setup_method(self):
        HandlerRegistry.clear()

    def teardown_method(self):
        HandlerRegistry.clear()

    def test_get_handler_returns_none_for_unknown_type(self):
        assert HandlerRegistry.get_handler("unknown_type") is None

    def test_get_handler_returns_correct_handler(self):

        @message_handler(message_types=["target_type"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler)
        assert HandlerRegistry.get_handler("target_type") is handler


class TestHandlerRegistryGetAllHandlers:
    """Tests for HandlerRegistry.get_all_handlers()."""

    def setup_method(self):
        HandlerRegistry.clear()

    def teardown_method(self):
        HandlerRegistry.clear()

    def test_returns_empty_dict_when_empty(self):
        assert HandlerRegistry.get_all_handlers() == {}

    def test_returns_all_registered_handlers(self):

        @message_handler(message_types=["type_a"])
        def handler_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        @message_handler(message_types=["type_b"])
        def handler_b(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler_a)
        HandlerRegistry.register(handler_b)

        all_handlers = HandlerRegistry.get_all_handlers()
        assert len(all_handlers) == 2
        assert all_handlers["type_a"] is handler_a
        assert all_handlers["type_b"] is handler_b


class TestHandlerRegistryClear:
    """Tests for HandlerRegistry.clear()."""

    def setup_method(self):
        HandlerRegistry.clear()

    def test_clear_removes_all_handlers(self):

        @message_handler(message_types=["test_type"])
        def handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        HandlerRegistry.register(handler)
        HandlerRegistry.clear()

        assert HandlerRegistry.get_all_handlers() == {}


class TestHandlerRegistryLoadHandler:
    """Tests for HandlerRegistry.load_handler()."""

    def setup_method(self):
        HandlerRegistry.clear()

    def teardown_method(self):
        HandlerRegistry.clear()

    def test_load_handler_by_import_path(self):
        import sys
        import types

        mod = types.ModuleType("gpmq_test_handler_dummy")
        mod.message = _make_message

        @message_handler(message_types=["dynamic_type"])
        def dynamic_handler(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.01,
            )

        mod.dynamic_handler = dynamic_handler
        sys.modules["gpmq_test_handler_dummy"] = mod

        try:
            HandlerRegistry.load_handler("gpmq_test_handler_dummy.dynamic_handler")
            assert HandlerRegistry.get_handler("dynamic_type") is dynamic_handler
        finally:
            del sys.modules["gpmq_test_handler_dummy"]

    def test_load_handler_invalid_path_raises_error(self):
        with pytest.raises(ImportError):
            HandlerRegistry.load_handler("nonexistent.module.handler")
