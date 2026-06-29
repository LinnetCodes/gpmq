"""Tests for subscriber module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus


# --- Helper functions ---


def _make_subscriber_config(
    name: str = "test-subscriber",
    handlers: list[dict] | None = None,
    workers: int = 1,
    run_forever_method: str | None = None,
    run_forever_method_paras: dict | None = None,
) -> SubscriberConfig:
    """Create a test SubscriberConfig."""
    if handlers is None:
        handlers = [
            {
                "message_types": ["type_a"],
                "handler": "myapp.handlers.handle_a",
            }
        ]
    kwargs = dict(
        name=name,
        handlers=[HandlerConfig(**h) for h in handlers],
        workers=workers,
        redis_host="localhost",
        redis_port=6379,
        redis_db=15,
        heartbeat_interval=60,
    )
    if run_forever_method is not None:
        kwargs["run_forever_method"] = run_forever_method
    if run_forever_method_paras is not None:
        kwargs["run_forever_method_paras"] = run_forever_method_paras
    return SubscriberConfig(**kwargs)


class TestSubscriberInit:
    """Tests for Subscriber.__init__()."""

    def test_creates_subscriber_with_config(self):
        """Should store config and initialize components."""
        from gpmq.subscriber.subscriber import Subscriber

        config = _make_subscriber_config(name="my-sub")
        with patch("gpmq.storage.redis_adapter.RedisAdapter"):
            sub = Subscriber(config)
            assert sub.name == "my-sub"
            assert sub.config is config


class TestSubscriberStart:
    """Tests for Subscriber.start()."""

    def test_loads_handlers_and_validates_types(self):
        """Should load handlers and validate message types match config."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config(
            handlers=[
                {"message_types": ["type_a"], "handler": "myapp.handlers.handle_a"}
            ],
        )

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            sub = Subscriber(config)
            sub.start()

            mock_redis.register_subscriber.assert_called_once()
            mock_redis.xgroup_create.assert_called_once()
            assert sub._worker_manager is not None

            sub.stop()

        HandlerRegistry.clear()

    def test_start_validates_handler_types_match_config(self):
        """Should raise if config types don't match decorator-declared types."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_b"])
        def handle_b(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        config = _make_subscriber_config(
            handlers=[
                {"message_types": ["type_a"], "handler": "myapp.handlers.handle_b"}
            ],
        )

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler") as mock_load,
        ):
            mock_load.return_value = handle_b
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            sub = Subscriber(config)

            with pytest.raises(Exception):
                sub.start()

        HandlerRegistry.clear()

    def test_start_registers_and_creates_consumer_group(self):
        """Should call register_subscriber and xgroup_create on Redis."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["event_x"])
        def handle_x(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-sub",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_x)

        config = _make_subscriber_config(
            name="test-sub",
            handlers=[{"message_types": ["event_x"], "handler": "mod.handle_x"}],
        )

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            sub = Subscriber(config)
            sub.start()

            mock_redis.register_subscriber.assert_called_once()
            reg_call_kwargs = mock_redis.register_subscriber.call_args
            assert reg_call_kwargs[0][0] == "test-sub"

            mock_redis.xgroup_create.assert_called_once_with(
                "gpmq:messages", "test-sub"
            )

            sub.stop()

        HandlerRegistry.clear()

    def test_start_failure_rolls_back_registration(self):
        """5.2: a failed start() must roll back (unregister) and propagate the original error."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="rollback-sub",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config(
            name="rollback-sub",
            handlers=[{"message_types": ["type_a"], "handler": "myapp.handlers.handle_a"}],
        )

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            # Let connect/register succeed, but make xgroup_create (a late step) raise.
            mock_redis.xgroup_create.side_effect = RuntimeError("xgroup boom")
            sub = Subscriber(config)
            # The original RuntimeError must propagate (not masked by rollback).
            with pytest.raises(RuntimeError, match="xgroup boom"):
                sub.start()
            # Rollback: unregister_subscriber was called (register ran before the failure).
            mock_redis.unregister_subscriber.assert_called_once_with("rollback-sub")

        HandlerRegistry.clear()


class TestSubscriberStop:
    """Tests for Subscriber.stop()."""

    def test_stop_unregisters_and_cleans_up(self):
        """Should stop worker manager and unregister from Redis."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config()

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager") as mock_wm_cls,
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            mock_wm_instance = MagicMock()
            mock_wm_cls.return_value = mock_wm_instance
            sub = Subscriber(config)
            sub.start()

            sub.stop()

            mock_redis.unregister_subscriber.assert_called_once_with("test-subscriber")
            mock_wm_instance.stop.assert_called_once()

        HandlerRegistry.clear()

    def test_stop_without_start_does_not_raise(self):
        """Stopping before start should be a no-op."""
        from gpmq.subscriber.subscriber import Subscriber

        config = _make_subscriber_config()
        with patch("gpmq.storage.redis_adapter.RedisAdapter"):
            sub = Subscriber(config)
            sub.stop()


class TestSubscriberGetStatus:
    """Tests for Subscriber.get_status()."""

    def test_returns_status_dict(self):
        """Should return dict with subscriber info."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config()

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis
            sub = Subscriber(config)
            sub.start()

            status = sub.get_status()
            assert isinstance(status, dict)
            assert status["name"] == "test-subscriber"
            assert "workers" in status

            sub.stop()

        HandlerRegistry.clear()


class TestSubscriberRunForever:
    """Tests for Subscriber.run_forever() method."""

    def test_run_forever_calls_start_and_stop(self):
        """run_forever should call start() and stop() on KeyboardInterrupt."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config()

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis

            with patch("gpmq.subscriber.subscriber.time.sleep", side_effect=KeyboardInterrupt()):
                sub = Subscriber(config)
                sub.run_forever()

            mock_redis.register_subscriber.assert_called_once()
            mock_redis.unregister_subscriber.assert_called_once()

        HandlerRegistry.clear()

    def test_run_forever_default_loop_sleep(self):
        """run_forever with no run_forever_method should use time.sleep loop."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config()

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter"),
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
            patch("gpmq.subscriber.subscriber.time.sleep", side_effect=KeyboardInterrupt()),
        ):
            sub = Subscriber(config)
            sub.run_forever()

        HandlerRegistry.clear()

    def test_run_forever_calls_configured_method(self):
        """run_forever should call configured run_forever_method function."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config(
            run_forever_method="test_module.custom_forever",
            run_forever_method_paras={"interval": 5},
        )

        mock_forever_func = MagicMock(side_effect=KeyboardInterrupt())
        mock_forever_func._gpmq_run_forever = True

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis

            with patch("gpmq.subscriber.subscriber.importlib.import_module") as mock_import:
                mock_module = MagicMock()
                mock_module.custom_forever = mock_forever_func
                mock_import.return_value = mock_module

                sub = Subscriber(config)
                sub.run_forever()

                mock_forever_func.assert_called_once_with(
                    subscriber=sub, paras={"interval": 5}
                )

        HandlerRegistry.clear()

    def test_run_forever_method_not_decorated_raises(self):
        """run_forever should raise TypeError if function not decorated."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config(
            run_forever_method="test_module.undecorated_func",
        )

        mock_func = MagicMock(spec=[])
        # Ensure no _gpmq_run_forever attribute
        if hasattr(mock_func, "_gpmq_run_forever"):
            del mock_func._gpmq_run_forever

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter"),
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
            patch("gpmq.subscriber.subscriber.importlib.import_module") as mock_import,
        ):
            mock_module = MagicMock()
            mock_module.undecorated_func = mock_func
            mock_import.return_value = mock_module

            sub = Subscriber(config)
            with pytest.raises(TypeError, match="run_forever_method"):
                sub.run_forever()

        HandlerRegistry.clear()

    def test_run_forever_handles_system_exit(self):
        """run_forever should handle SystemExit and still call stop()."""
        from gpmq.subscriber.subscriber import Subscriber
        from gpmq.subscriber.handler import HandlerRegistry, message_handler

        HandlerRegistry.clear()

        @message_handler(message_types=["type_a"])
        def handle_a(msg: Message) -> ProcessResult:
            return ProcessResult(
                message_id=msg.id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="test-subscriber",
                processing_time=0.01,
            )

        HandlerRegistry.register(handle_a)

        config = _make_subscriber_config()

        with (
            patch("gpmq.storage.redis_adapter.RedisAdapter") as mock_redis_cls,
            patch("gpmq.subscriber.handler.HandlerRegistry.load_handler"),
            patch("gpmq.subscriber.worker_manager.WorkerManager"),
        ):
            mock_redis = MagicMock()
            mock_redis_cls.return_value = mock_redis

            with patch("gpmq.subscriber.subscriber.time.sleep", side_effect=SystemExit()):
                sub = Subscriber(config)
                sub.run_forever()

            mock_redis.unregister_subscriber.assert_called_once()

        HandlerRegistry.clear()
