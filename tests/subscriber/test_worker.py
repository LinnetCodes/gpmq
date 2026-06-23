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
            "test-subscriber", "test-subscriber-hostA-worker-0"
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
            worker.prepare_worker_context({
                "worker_context_processor": f"{mod_name}.my_processor"
            })
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
                worker.prepare_worker_context({
                    "worker_context_processor": f"{mod_name}.my_processor"
                })
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
            worker.prepare_worker_context({
                "worker_context_processor": "nonexistent.module.func"
            })
