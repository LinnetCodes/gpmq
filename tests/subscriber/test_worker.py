"""Tests for subscriber worker module."""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus


# --- Helper functions ---


def _make_subscriber_config(
    name: str = "test-subscriber",
    workers: int = 1,
    message_types: list[str] | None = None,
    stream_name: str = "gpmq:messages",
) -> dict:
    """Create test SubscriberConfig kwargs."""
    return {
        "name": name,
        "handlers": [
            {
                "message_types": message_types or ["test_type"],
                "handler": "dummy.handler",
            }
        ],
        "workers": workers,
        "stream_name": stream_name,
        "redis_host": "localhost",
        "redis_port": 6379,
        "redis_db": 15,
        "heartbeat_interval": 60,
        "log_category": "gpmq",
    }


def _make_stream_message(
    msg_id: str = "1680000000000-0",
    msg_type: str = "test_type",
    payload: dict | None = None,
) -> tuple[str, dict[str, str]]:
    """Create a raw Redis Stream message tuple."""
    fields = {
        "type": msg_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "publisher": "test-pub",
        "payload": json.dumps(payload or {"key": "val"}),
    }
    return (msg_id, fields)


class TestWorkerProcessMessage:
    """Tests for Worker._process_message()."""

    def test_calls_handler_and_returns_success_result(self):
        """Should call handler, save result, ack message, and publish result."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["order_created"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock(
            return_value=ProcessResult(
                message_id="msg-1",
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.05,
                data={"result": "ok"},
            )
        )

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["order_created"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-1", msg_type="order_created")

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_called_once()
        call_arg = mock_handler.call_args[0][0]
        assert isinstance(call_arg, Message)
        assert call_arg.type == "order_created"
        mock_audit.save_result.assert_called_once()
        mock_redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "msg-1"
        )
        mock_redis.publish.assert_called_once()
        publish_channel = mock_redis.publish.call_args[0][0]
        assert "gpmq:result:msg-1:test-subscriber" == publish_channel

    def test_handler_exception_creates_exception_result(self):
        """Should catch handler exceptions, create EXCEPTION result, and still ACK."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["failing_type"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock(side_effect=RuntimeError("handler crashed"))

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["failing_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-2", msg_type="failing_type")

        worker._process_message(msg_id, fields, mock_handler)

        saved_result = mock_audit.save_result.call_args[0][1]
        assert saved_result.status == ProcessStatus.EXCEPTION
        assert "handler crashed" in saved_result.error_message
        mock_redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "msg-2"
        )
        mock_redis.publish.assert_called_once()

    def test_skips_non_matching_message_type_and_acks(self):
        """Should ACK and skip messages not in this subscriber's types."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["order_created"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock()

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["order_created"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-3", msg_type="email_sent")

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_not_called()
        mock_redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "msg-3"
        )
        mock_redis.publish.assert_not_called()
        mock_audit.save_result.assert_not_called()

    def test_handler_returns_non_process_result_wrapped_as_success(self):
        """Should wrap non-ProcessResult return value as SUCCESS."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["type_a"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock(return_value={"custom": "data"})

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["type_a"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-4", msg_type="type_a")

        worker._process_message(msg_id, fields, mock_handler)

        saved_result = mock_audit.save_result.call_args[0][1]
        assert saved_result.status == ProcessStatus.SUCCESS
        assert saved_result.data == {"custom": "data"}
        mock_redis.xack.assert_called_once()
        mock_redis.publish.assert_called_once()

    def test_handler_returns_none_wrapped_as_success(self):
        """Should wrap None return as SUCCESS."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["type_b"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock(return_value=None)

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["type_b"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-5", msg_type="type_b")

        worker._process_message(msg_id, fields, mock_handler)

        saved_result = mock_audit.save_result.call_args[0][1]
        assert saved_result.status == ProcessStatus.SUCCESS
        mock_redis.xack.assert_called_once()
        mock_redis.publish.assert_called_once()

    def test_handler_returns_process_status_failure(self):
        """Should treat ProcessStatus.FAILURE return as FAILURE result."""
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["type_c"])
        config = SubscriberConfig(**config_dict)

        mock_redis = MagicMock()
        mock_audit = MagicMock()
        mock_handler = MagicMock(return_value=ProcessStatus.FAILURE)

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["type_c"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = mock_redis
        worker._audit = mock_audit

        msg_id, fields = _make_stream_message(msg_id="msg-6", msg_type="type_c")

        worker._process_message(msg_id, fields, mock_handler)

        saved_result = mock_audit.save_result.call_args[0][1]
        assert saved_result.status == ProcessStatus.FAILURE
        mock_redis.xack.assert_called_once()
        mock_redis.publish.assert_called_once()


class TestWorkerEmbeddedPublisher:
    """Tests for embedded publisher in worker context."""

    def test_prepare_worker_context_creates_unconnected_publisher(self):
        """When publisher_cfg_path is in context, prepare_worker_context should create unconnected GPMQClient."""
        from gpmq.subscriber.worker import Worker
        from gpmq.client import GPMQClient
        from unittest.mock import patch

        config_dict = _make_subscriber_config()

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
            context={"publisher_cfg_path": "gpmq.publisher.main"},
        )

        mock_client = MagicMock(spec=GPMQClient)
        mock_client._connected = False
        with patch("gpmq.config.get_client", return_value=mock_client):
            worker.prepare_worker_context({"publisher_cfg_path": "gpmq.publisher.main"})

        assert "publisher" in worker._worker_ctx
        assert worker._worker_ctx["publisher"] is mock_client
        assert worker._publisher_client is mock_client
        mock_client.connect.assert_not_called()

    def test_prepare_worker_context_without_publisher_cfg(self):
        """When no publisher_cfg_path in context, no publisher should be created."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config()
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        worker.prepare_worker_context({})
        assert "publisher" not in worker._worker_ctx
        assert worker._publisher_client is None

    def test_cleanup_closes_publisher_client(self):
        """_cleanup should close publisher client if it exists."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config()
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        mock_publisher = MagicMock()
        worker._publisher_client = mock_publisher
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        worker._cleanup()
        mock_publisher.close.assert_called_once()

    def test_cleanup_without_publisher_is_safe(self):
        """_cleanup should be safe when no publisher client exists."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config()
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        worker._publisher_client = None
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        worker._cleanup()  # should not raise


def test_worker_config_contains_enable_audit():
    """Worker config dict should propagate enable_audit setting."""
    from gpmq.subscriber.worker import Worker

    config_dict = {
        "name": "test_sub",
        "redis_host": "127.0.0.1",
        "redis_password": "123456",
        "stream_name": "test:stream",
        "handlers": [{"message_types": ["test"], "handler": "mod.func"}],
        "enable_audit": False,
        "registry_key_prefix": "gpmq:",
        "log_category": "gpmq",
    }
    worker = Worker(
        subscriber_name="test_sub",
        stream_name="test:stream",
        message_types=["test"],
        worker_id="test_sub-worker-0",
        hostname="test-host",
        config_dict=config_dict,
    )
    assert worker._config_dict.get("enable_audit") is False


class TestWorkerContextProcessorDecorator:
    """Tests for the worker_context_processor decorator."""

    def test_sets_marker_attribute(self):
        """Decorated function should have _gpmq_ctx_proc = True."""
        from gpmq.subscriber.decorators import worker_context_processor

        @worker_context_processor
        def my_proc(context: dict[str, Any]) -> dict[str, Any]:
            return context

        assert my_proc._gpmq_ctx_proc is True

    def test_preserves_function_identity(self):
        """Decorated function should keep its original __name__."""
        from gpmq.subscriber.decorators import worker_context_processor

        @worker_context_processor
        def my_proc(context: dict[str, Any]) -> dict[str, Any]:
            return context

        assert my_proc.__name__ == "my_proc"

    def test_function_still_callable(self):
        """Decorated function should still be callable and work normally."""
        from gpmq.subscriber.decorators import worker_context_processor

        @worker_context_processor
        def my_proc(context: dict[str, Any]) -> dict[str, Any]:
            context["added"] = True
            return context

        result = my_proc({"key": "val"})
        assert result == {"key": "val", "added": True}

    def test_rejects_wrong_param_count(self):
        """Should raise TypeError for functions with != 1 parameter."""
        from gpmq.subscriber.decorators import worker_context_processor

        with pytest.raises(TypeError, match="parameter"):

            @worker_context_processor
            def no_params() -> dict[str, Any]:
                return {}

        with pytest.raises(TypeError, match="parameter"):

            @worker_context_processor
            def two_params(context: dict[str, Any], extra: str) -> dict[str, Any]:
                return context

    def test_rejects_wrong_param_name(self):
        """Should raise TypeError if parameter is not named 'context'."""
        from gpmq.subscriber.decorators import worker_context_processor

        with pytest.raises(TypeError, match="context"):

            @worker_context_processor
            def my_proc(data: dict[str, Any]) -> dict[str, Any]:
                return data

    def test_rejects_wrong_param_annotation(self):
        """Should raise TypeError for non-dict[str, Any] parameter annotation."""
        from gpmq.subscriber.decorators import worker_context_processor

        with pytest.raises(TypeError, match="annotation"):

            @worker_context_processor
            def my_proc(context: str) -> dict[str, Any]:
                return {}

    def test_rejects_wrong_return_annotation(self):
        """Should raise TypeError for non-dict[str, Any] return annotation."""
        from gpmq.subscriber.decorators import worker_context_processor

        with pytest.raises(TypeError, match="return"):

            @worker_context_processor
            def my_proc(context: dict[str, Any]) -> str:
                return "bad"

    def test_accepts_no_annotations(self):
        """Bare func(context) with no annotations should be accepted."""
        from gpmq.subscriber.decorators import worker_context_processor

        @worker_context_processor
        def my_proc(context):
            return context

        assert my_proc._gpmq_ctx_proc is True
        result = my_proc({"a": 1})
        assert result == {"a": 1}


# --- Helper for TestWorkerContextProcessorLoading ---


def _make_ctx_proc_module(func, module_name="gpmq_test_ctx_proc_dummy"):
    """Create a temporary module with a decorated context processor function."""
    mod = types.ModuleType(module_name)
    mod.my_processor = func
    sys.modules[module_name] = mod
    return module_name


class TestWorkerRegistry:
    """Tests for worker registration, heartbeat, and unregistration."""

    def test_register_self_calls_redis(self):
        """_register_self() should call register_worker with correct arguments."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["test_type"])

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-hostA-worker-0",
            hostname="hostA",
            config_dict=config_dict,
        )
        mock_redis = MagicMock()
        worker._redis = mock_redis

        worker._register_self()

        mock_redis.register_worker.assert_called_once()
        call_args = mock_redis.register_worker.call_args
        assert call_args[0][0] == "test-subscriber"
        assert call_args[0][1] == "test-subscriber-hostA-worker-0"
        info = call_args[0][2]
        assert info["hostname"] == "hostA"
        assert info["status"] == "running"

    def test_cleanup_unregisters_worker(self):
        """Worker._cleanup() should call unregister_worker."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["test_type"])

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-hostA-worker-0",
            hostname="hostA",
            config_dict=config_dict,
        )
        mock_redis = MagicMock()
        worker._redis = mock_redis
        worker._audit = MagicMock()

        worker._cleanup()

        mock_redis.unregister_worker.assert_called_once_with(
            "test-subscriber", "test-subscriber-hostA-worker-0"
        )

    def test_read_and_process_updates_heartbeat(self):
        """Each loop iteration should call update_worker_heartbeat."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["test_type"])

        mock_redis = MagicMock()
        mock_redis.xreadgroup.return_value = []

        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-hostA-worker-0",
            hostname="hostA",
            config_dict=config_dict,
        )
        worker._redis = mock_redis

        worker._read_and_process()

        mock_redis.update_worker_heartbeat.assert_called_once_with(
            "test-subscriber",
            "test-subscriber-hostA-worker-0",
            extra={
                "messages_processed": 0,
                "last_activity": 0.0,
                "last_message_id": None,
            },
        )


class TestWorkerContextProcessorLoading:
    """Tests for loading worker_context_processor in prepare_worker_context."""

    def test_loads_decorated_processor_and_calls_it(self):
        """Should load a decorated processor, assign to _ctx_proc_func, and call it."""
        from gpmq.subscriber.worker import Worker
        from gpmq.subscriber.decorators import worker_context_processor

        @worker_context_processor
        def my_processor(context: dict[str, Any]) -> dict[str, Any]:
            return {"custom_key": "custom_value"}

        mod_name = _make_ctx_proc_module(my_processor)
        try:
            config_dict = _make_subscriber_config()
            worker = Worker(
                subscriber_name="test-subscriber",
                stream_name="gpmq:messages",
                message_types=["test_type"],
                worker_id="test-subscriber-worker-0",
                hostname="test-host",
                config_dict=config_dict,
            )
            worker.prepare_worker_context(
                {"worker_context_processor": f"{mod_name}.my_processor"}
            )
            assert worker._ctx_proc_func is my_processor
            assert worker._worker_ctx.get("custom_key") == "custom_value"
        finally:
            del sys.modules[mod_name]

    def test_rejects_undecorated_processor(self):
        """Should raise TypeError if function lacks _gpmq_ctx_proc attribute."""
        from gpmq.subscriber.worker import Worker

        def plain_processor(context: dict[str, Any]) -> dict[str, Any]:
            return {}

        mod_name = _make_ctx_proc_module(plain_processor)
        try:
            config_dict = _make_subscriber_config()
            worker = Worker(
                subscriber_name="test-subscriber",
                stream_name="gpmq:messages",
                message_types=["test_type"],
                worker_id="test-subscriber-worker-0",
                hostname="test-host",
                config_dict=config_dict,
            )
            with pytest.raises(TypeError, match="@worker_context_processor"):
                worker.prepare_worker_context(
                    {"worker_context_processor": f"{mod_name}.my_processor"}
                )
        finally:
            del sys.modules[mod_name]

    def test_no_processor_when_not_in_context(self):
        """When no worker_context_processor in context, _ctx_proc_func stays None."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config()
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        worker.prepare_worker_context({})
        assert worker._ctx_proc_func is None

    def test_invalid_import_path_raises_error(self):
        """Should raise ImportError for invalid import path."""
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config()
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["test_type"],
            worker_id="test-subscriber-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        with pytest.raises(ImportError):
            worker.prepare_worker_context(
                {"worker_context_processor": "nonexistent.module.func"}
            )


class TestWorkerTargetSubscribersFilter:
    """Tests for target_subscribers discard logic in _process_message()."""

    def _make_worker(self, message_types, subscriber_name="test-subscriber"):
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(
            name=subscriber_name, message_types=message_types
        )
        config = SubscriberConfig(**config_dict)
        worker = Worker(
            subscriber_name=subscriber_name,
            stream_name="gpmq:messages",
            message_types=message_types,
            worker_id=f"{subscriber_name}-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        return worker

    def test_discards_when_not_in_target_list(self):
        """Subscriber not in target_subscribers should ACK and skip, no result."""
        worker = self._make_worker(["order_created"])
        mock_handler = MagicMock()

        msg_id, fields = _make_stream_message(msg_id="t-1", msg_type="order_created")
        fields["target_subscribers"] = json.dumps(["other-sub"])

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "t-1"
        )
        worker._redis.publish.assert_not_called()
        worker._audit.save_result.assert_not_called()

    def test_processes_when_in_target_list(self):
        """Subscriber in target_subscribers should process normally."""
        worker = self._make_worker(["order_created"])
        mock_handler = MagicMock(return_value={"ok": True})

        msg_id, fields = _make_stream_message(msg_id="t-2", msg_type="order_created")
        fields["target_subscribers"] = json.dumps(["test-subscriber", "other-sub"])

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_called_once()
        worker._redis.publish.assert_called_once()

    def test_processes_when_target_field_absent(self):
        """No target_subscribers field -> legacy behavior (process)."""
        worker = self._make_worker(["order_created"])
        mock_handler = MagicMock(return_value=None)

        msg_id, fields = _make_stream_message(msg_id="t-3", msg_type="order_created")
        # No 'target_subscribers' key in fields

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_called_once()
        worker._redis.publish.assert_called_once()

    def test_target_filter_applies_after_type_filter(self):
        """Wrong type AND not targeted -> ACK once (type filter wins)."""
        worker = self._make_worker(["order_created"])
        mock_handler = MagicMock()

        msg_id, fields = _make_stream_message(msg_id="t-4", msg_type="email_sent")
        fields["target_subscribers"] = json.dumps(["other-sub"])

        worker._process_message(msg_id, fields, mock_handler)

        mock_handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "t-4"
        )


class TestWorkerPoisonMessage:
    """Regression for 1.1: worker must not crash on a malformed stream message."""

    def _make_worker(self):
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["order_created"])
        config = SubscriberConfig(**config_dict)
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["order_created"],
            worker_id="test-subscriber-hostA-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        return worker

    def test_invalid_payload_json_does_not_crash(self):
        """Non-JSON payload -> poison handling, EXCEPTION result, ACK, no raise."""
        worker = self._make_worker()
        handler = MagicMock()
        fields = {
            "type": "order_created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "publisher": "pub",
            "payload": "this is not json {{{",
        }
        worker._process_message("bad-1", fields, handler)  # must not raise
        handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "bad-1"
        )
        worker._redis.publish.assert_called_once()
        result = worker._redis.publish.call_args[0][1]
        assert result["status"] == "exception"
        worker._audit.save_message.assert_called_once()
        poison = worker._audit.save_message.call_args[0][0]
        assert poison.id == "bad-1"
        assert poison.type == "__poison__"
        assert poison.payload == {
            "broken_msg": json.dumps(fields, ensure_ascii=False, default=str)
        }

    def test_invalid_target_subscribers_json_does_not_crash(self):
        """target_subscribers non-JSON -> poison handling, no crash."""
        worker = self._make_worker()
        handler = MagicMock()
        fields = {
            "type": "order_created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "publisher": "pub",
            "payload": json.dumps({"k": 1}),
            "target_subscribers": "not a list json",
        }
        worker._process_message("bad-2", fields, handler)
        handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "bad-2"
        )
        worker._redis.publish.assert_called_once()

    def test_invalid_timestamp_does_not_crash(self):
        """Invalid timestamp -> Message construction fails -> poison handling."""
        worker = self._make_worker()
        handler = MagicMock()
        fields = {
            "type": "order_created",
            "timestamp": "",  # empty string fails datetime parse
            "publisher": "pub",
            "payload": json.dumps({"k": 1}),
        }
        worker._process_message("bad-3", fields, handler)
        handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "bad-3"
        )

    def test_missing_required_fields_does_not_crash(self):
        """Missing type/publisher/timestamp -> Message construction fails -> poison."""
        worker = self._make_worker()
        handler = MagicMock()
        fields = {"payload": json.dumps({"k": 1})}  # no type/publisher/timestamp
        worker._process_message("bad-4", fields, handler)
        handler.assert_not_called()
        worker._redis.xack.assert_called_once_with(
            "gpmq:messages", "test-subscriber", "bad-4"
        )

    def test_ack_failure_does_not_crash_worker(self):
        """If xack raises during poison handling, worker still does not crash."""
        worker = self._make_worker()
        worker._redis.xack.side_effect = RuntimeError("redis down")
        handler = MagicMock()
        fields = {
            "type": "order_created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "publisher": "pub",
            "payload": "not json",
        }
        worker._process_message("bad-5", fields, handler)  # must not raise
        worker._redis.xack.assert_called_once()
        # publish NOT called because ACK failed and we returned early
        worker._redis.publish.assert_not_called()

    def test_no_audit_still_publishes_exception(self):
        """When audit is disabled, poison branch still publishes EXCEPTION + ACKs."""
        worker = self._make_worker()
        worker._audit = None
        handler = MagicMock()
        fields = {
            "type": "order_created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "publisher": "pub",
            "payload": "not json",
        }
        worker._process_message("bad-6", fields, handler)
        worker._redis.xack.assert_called_once()
        worker._redis.publish.assert_called_once()

    def test_continues_consuming_after_poison_message(self):
        """After handling one poison message, the worker must still process the next valid one.

        This is the headline 1.1 property: a single malformed message does not
        permanently stop consumption.
        """
        from gpmq.models.result import ProcessResult, ProcessStatus

        worker = self._make_worker()
        valid_handler = MagicMock(
            return_value=ProcessResult(
                message_id="good-1",
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )
        )

        # First: a poison message (invalid payload JSON).
        poison_fields = {
            "type": "order_created",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "publisher": "pub",
            "payload": "this is not json {{{",
        }
        worker._process_message("bad-1", poison_fields, valid_handler)  # must not raise

        # Second: a valid message of the same type.
        good_id, good_fields = _make_stream_message(
            msg_id="good-1", msg_type="order_created"
        )
        worker._process_message(good_id, good_fields, valid_handler)  # must not raise

        # The handler was called exactly once — for the valid message, NOT the poison one.
        valid_handler.assert_called_once()
        # The poison message was ACKed, and the valid message was ACKed too.
        assert worker._redis.xack.call_count == 2
        # Two publishes: one EXCEPTION (poison) + one SUCCESS (valid).
        assert worker._redis.publish.call_count == 2


class TestWorkerReadBatch:
    """Tests for configurable worker_read_batch_size (4.1)."""

    def _make_worker(self, read_batch_size: int):
        from gpmq.config import SubscriberConfig
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=["order_created"])
        config_dict["worker_read_batch_size"] = read_batch_size
        config = SubscriberConfig(**config_dict)
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=["order_created"],
            worker_id="test-subscriber-hostA-worker-0",
            hostname="test-host",
            config_dict=config.model_dump(),
        )
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        return worker

    def test_reads_configured_batch_size(self):
        """4.1: xreadgroup count kwarg should match worker_read_batch_size."""
        worker = self._make_worker(read_batch_size=3)

        from gpmq.subscriber.handler import HandlerRegistry

        HandlerRegistry.clear()
        mock_handler = MagicMock()
        try:
            HandlerRegistry._handlers["order_created"] = mock_handler
            # Return a batch of 3 messages.
            m1_id, m1_fields = _make_stream_message(
                msg_id="m1", msg_type="order_created"
            )
            m2_id, m2_fields = _make_stream_message(
                msg_id="m2", msg_type="order_created"
            )
            m3_id, m3_fields = _make_stream_message(
                msg_id="m3", msg_type="order_created"
            )
            worker._redis.xreadgroup.return_value = [
                (
                    "gpmq:messages",
                    [(m1_id, m1_fields), (m2_id, m2_fields), (m3_id, m3_fields)],
                ),
            ]
            worker._read_and_process()

            # count kwarg propagated to xreadgroup.
            assert worker._redis.xreadgroup.call_args.kwargs["count"] == 3
            # All 3 messages processed and ACKed individually.
            assert worker._redis.xack.call_count == 3
            assert mock_handler.call_count == 3
        finally:
            HandlerRegistry.clear()

    def test_batch_with_one_malformed_does_not_abort_others(self):
        """4.1 (relies on 1.1): a malformed message in a batch is isolated; siblings still process."""
        worker = self._make_worker(read_batch_size=2)

        from gpmq.subscriber.handler import HandlerRegistry

        HandlerRegistry.clear()
        mock_handler = MagicMock(return_value={"ok": True})
        try:
            HandlerRegistry._handlers["order_created"] = mock_handler
            # First message: poison (invalid payload JSON). Second: valid.
            poison_fields = {
                "type": "order_created",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "publisher": "pub",
                "payload": "this is not json {{{",
            }
            good_id, good_fields = _make_stream_message(
                msg_id="good-1", msg_type="order_created"
            )
            worker._redis.xreadgroup.return_value = [
                ("gpmq:messages", [("bad-1", poison_fields), (good_id, good_fields)]),
            ]
            worker._read_and_process()

            # Both messages ACKed (poison-handled + processed).
            assert worker._redis.xack.call_count == 2
            # The valid message was processed; the malformed one was NOT.
            mock_handler.assert_called_once()
            # Poison handling published an EXCEPTION result.
            assert worker._redis.publish.call_count == 2
        finally:
            HandlerRegistry.clear()


class TestWorkerLiveStatus:
    """Tests for in-process live-status counters (R-001)."""

    @staticmethod
    def _make_worker(message_types: list[str]) -> Any:
        from gpmq.subscriber.worker import Worker

        config_dict = _make_subscriber_config(message_types=message_types)
        worker = Worker(
            subscriber_name="test-subscriber",
            stream_name="gpmq:messages",
            message_types=message_types,
            worker_id="test-subscriber-hostA-worker-0",
            hostname="hostA",
            config_dict=config_dict,
        )
        worker._redis = MagicMock()
        worker._audit = MagicMock()
        return worker

    def test_success_records_activity(self):
        """A processed message increments counters and sets last_message_id."""
        msg_id, fields = _make_stream_message(msg_id="111-0", msg_type="test_type")
        worker = self._make_worker(["test_type"])
        handler = MagicMock(return_value=None)

        worker._process_message(msg_id, fields, handler)

        assert worker._messages_processed == 1
        assert worker._last_message_id == "111-0"
        assert worker._last_activity > 0.0

    def test_handler_exception_records_activity(self):
        """EXCEPTION outcomes also count as processed."""
        msg_id, fields = _make_stream_message(msg_id="222-0", msg_type="test_type")
        worker = self._make_worker(["test_type"])
        handler = MagicMock(side_effect=RuntimeError("boom"))

        worker._process_message(msg_id, fields, handler)

        assert worker._messages_processed == 1
        assert worker._last_message_id == "222-0"

    def test_poison_message_records_activity(self):
        """Poison dispositions count as processed."""
        fields = {"type": "test_type", "payload": "{not json"}
        worker = self._make_worker(["test_type"])
        handler = MagicMock()

        worker._process_message("333-0", fields, handler)

        handler.assert_not_called()
        assert worker._messages_processed == 1
        assert worker._last_message_id == "333-0"

    def test_filtered_message_not_counted(self):
        """Messages filtered out by type are not recorded."""
        msg_id, fields = _make_stream_message(msg_id="444-0", msg_type="other_type")
        worker = self._make_worker(["test_type"])
        handler = MagicMock()

        worker._process_message(msg_id, fields, handler)

        assert worker._messages_processed == 0
        assert worker._last_message_id is None
        assert worker._last_activity == 0.0

    def test_heartbeat_carries_status_fields(self):
        """The throttled heartbeat flushes the live-status fields via extra."""
        worker = self._make_worker(["test_type"])
        worker._redis.xreadgroup.return_value = []

        worker._read_and_process()

        worker._redis.update_worker_heartbeat.assert_called_once_with(
            "test-subscriber",
            "test-subscriber-hostA-worker-0",
            extra={
                "messages_processed": 0,
                "last_activity": 0.0,
                "last_message_id": None,
            },
        )

    def test_heartbeat_throttle_is_four_seconds(self):
        """Heartbeat re-fires 4s after the last flush (not the old 5s)."""
        import time as _time

        worker = self._make_worker(["test_type"])
        worker._redis.xreadgroup.return_value = []

        # 4.1s since last flush: fires under the new 4s throttle,
        # would NOT have fired under the old hardcoded 5s.
        worker._last_heartbeat = _time.time() - 4.1
        worker._read_and_process()
        worker._redis.update_worker_heartbeat.assert_called_once()

        # Within the window: stays silent.
        worker._last_heartbeat = _time.time()
        worker._read_and_process()
        assert worker._redis.update_worker_heartbeat.call_count == 1
