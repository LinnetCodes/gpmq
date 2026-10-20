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


class TestHandlerPositionalValidation:
    """Regression for 3.2: validator must enforce position, not just name."""

    def test_reversed_param_order_rejected(self):
        """A (context, msg) handler must be rejected at decoration time."""
        from typing import Any, Optional

        with pytest.raises(TypeError):

            @message_handler(message_types=["reverse"])
            def handler(context: Optional[dict[str, Any]], msg: Message) -> Any:
                return None

    def test_first_param_msg_accepted(self):
        @message_handler(message_types=["ok1"])
        def handler(msg: Message):
            return None

        assert handler._gpmq_message_types == ["ok1"]

    def test_first_param_message_accepted(self):
        @message_handler(message_types=["ok2"])
        def handler(message: Message):
            return None

        assert handler._gpmq_message_types == ["ok2"]

    def test_second_param_ctx_accepted(self):
        from typing import Any, Optional

        @message_handler(message_types=["ok3"])
        def handler(msg: Message, ctx: Optional[dict[str, Any]] = None):
            return None

        assert handler._gpmq_message_types == ["ok3"]

    def test_first_param_wrong_name_rejected(self):
        with pytest.raises(TypeError):

            @message_handler(message_types=["bad1"])
            def handler(data: Message):
                return None

    def test_second_param_wrong_name_rejected(self):
        from typing import Any, Optional

        with pytest.raises(TypeError):

            @message_handler(message_types=["bad2"])
            def handler(msg: Message, extra: Optional[dict[str, Any]] = None):
                return None


class TestHandlerAnnotationTolerance:
    """Regression for 2.4: validator must accept real-type annotations
    (not just string forms), so handlers work without __future__ import.

    Because this test file has `from __future__ import annotations`, written
    annotations become strings. To test the REAL-type code path we inject
    real annotations via __annotations__ after function definition.
    """

    def test_real_message_annotation_accepted(self):
        """msg annotated with the real Message class -> accepted."""

        def raw_handler(msg):  # noqa: ANN001
            return None

        raw_handler.__annotations__ = {"msg": Message}
        decorated = message_handler(message_types=["real1"])(raw_handler)
        assert decorated._gpmq_message_types == ["real1"]

    def test_string_message_annotation_accepted(self):
        """msg annotated with the string 'Message' -> accepted."""

        def raw_handler(msg):  # noqa: ANN001
            return None

        raw_handler.__annotations__ = {"msg": "Message"}
        decorated = message_handler(message_types=["str1"])(raw_handler)
        assert decorated._gpmq_message_types == ["str1"]

    def test_real_ctx_annotation_accepted(self):
        """context annotated with real Optional[dict[str, Any]] -> accepted."""
        from typing import Any, Optional

        _ctx_type = Optional[dict[str, Any]]

        def raw_handler(msg, context):  # noqa: ANN001
            return None

        raw_handler.__annotations__ = {"msg": Message, "context": _ctx_type}
        decorated = message_handler(message_types=["real2"])(raw_handler)
        assert decorated._gpmq_message_types == ["real2"]

    def test_unannotated_params_accepted(self):
        """Bare params with no annotations -> accepted (existing behavior)."""

        @message_handler(message_types=["bare"])
        def handler(msg, context=None):  # noqa: ANN001
            return None

        assert handler._gpmq_message_types == ["bare"]

    def test_wrong_msg_annotation_rejected(self):
        """msg annotated with a non-Message type -> rejected."""
        with pytest.raises(TypeError):

            @message_handler(message_types=["bad3"])
            def handler(msg: int):
                return None

    def test_annotation_error_echoes_offending_value(self):
        """Annotation-mismatch errors should echo the offending annotation (debuggability)."""
        import pytest

        with pytest.raises(TypeError, match=r"<class 'int'>"):
            # real int annotation injected so the message contains the repr
            def raw_handler(msg):  # noqa: ANN001
                return None

            raw_handler.__annotations__ = {"msg": int}
            message_handler(message_types=["echo"])(raw_handler)
