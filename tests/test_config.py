"""Tests for GPMQ configuration classes."""

from pathlib import Path

import pytest
from gpconfig import GPConfigManager

import gpmq.config as cfg_module
from gpmq.config import (
    GPMQConfig,
    HandlerConfig,
    PublisherConfig,
    SubscriberConfig,
    get_client,
    get_subscriber,
    set_config_manager,
)


class TestHandlerConfig:
    """Tests for HandlerConfig."""

    def test_create_handler_config(self):
        """HandlerConfig should be created with required fields."""
        config = HandlerConfig(
            message_types=["user_created", "user_updated"],
            handler="myapp.handlers.on_user",
        )
        assert config.message_types == ["user_created", "user_updated"]
        assert config.handler == "myapp.handlers.on_user"

    def test_handler_config_requires_message_types(self):
        """HandlerConfig should require at least one message type."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            HandlerConfig(message_types=[], handler="myapp.handlers.on_user")

    def test_handler_config_requires_handler(self):
        """HandlerConfig should require a handler path."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            HandlerConfig(message_types=["user_created"], handler="")


class TestGPMQConfig:
    """Tests for GPMQConfig."""

    def test_create_gpmq_config_with_defaults(self):
        """GPMQConfig should be created with default values."""
        config = GPMQConfig()
        assert config.redis_host == "localhost"
        assert config.redis_port == 6379
        assert config.redis_db == 0
        assert config.redis_password == ""
        assert config.redis_max_retries == 3
        assert config.redis_retry_delay == 1.0
        assert config.stream_name == "gpmq:messages"
        assert config.heartbeat_interval == 10
        assert config.registry_key_prefix == "gpmq:"
        assert config.audit_db_path == "gpmq_audit.db"
        assert config.log_category == "gpmq"

    def test_enable_audit_default(self):
        """enable_audit should default to True."""
        config = GPMQConfig()
        assert config.enable_audit is True

    def test_enable_audit_can_be_disabled(self):
        """enable_audit can be set to False."""
        config = GPMQConfig(enable_audit=False)
        assert config.enable_audit is False

    def test_create_gpmq_config_with_custom_values(self):
        """GPMQConfig should accept custom values."""
        config = GPMQConfig(
            redis_host="127.0.0.1",
            redis_port=6380,
            redis_password="secret",
            stream_name="custom:stream",
        )
        assert config.redis_host == "127.0.0.1"
        assert config.redis_port == 6380
        assert config.redis_password == "secret"
        assert config.stream_name == "custom:stream"

    def test_redis_port_validation(self):
        """GPMQConfig should validate port range."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GPMQConfig(redis_port=0)
        with pytest.raises(Exception):  # Pydantic ValidationError
            GPMQConfig(redis_port=70000)

    def test_heartbeat_interval_minimum(self):
        """GPMQConfig should reject heartbeat_interval below 5 (cadence coupling)."""
        with pytest.raises(Exception):  # Pydantic ValidationError / ValueError
            GPMQConfig(heartbeat_interval=4)
        # Boundary: 5 is accepted.
        assert GPMQConfig(heartbeat_interval=5).heartbeat_interval == 5
        # And the default 10 still works.
        assert GPMQConfig().heartbeat_interval == 10


class TestRedisSocketTimeout:
    """Tests for the redis_socket_timeout field (redis-py >=6 default race fix)."""

    def test_default_socket_timeout(self):
        """Default must be 10.0 = worker block (5s) + margin."""
        config = GPMQConfig()
        assert config.redis_socket_timeout == 10.0

    def test_socket_timeout_custom_value(self):
        """Explicit values must pass through."""
        config = GPMQConfig(redis_socket_timeout=30.0)
        assert config.redis_socket_timeout == 30.0

    def test_socket_timeout_must_exceed_worker_block(self):
        """Values <= 5.0 (hardcoded worker XREADGROUP block_ms=5000) must be rejected."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            GPMQConfig(redis_socket_timeout=5.0)
        with pytest.raises(Exception):  # Pydantic ValidationError
            GPMQConfig(redis_socket_timeout=1.0)
        # Boundary: anything strictly above 5.0 is accepted.
        assert GPMQConfig(redis_socket_timeout=5.5).redis_socket_timeout == 5.5


class TestPublisherConfig:
    """Tests for PublisherConfig."""

    def test_create_publisher_config(self):
        """PublisherConfig should be created with required fields."""
        config = PublisherConfig(publisher_name="my_publisher")
        assert config.publisher_name == "my_publisher"
        assert config.publish_timeout is None

    def test_publisher_config_with_timeout(self):
        """PublisherConfig should accept custom timeout."""
        config = PublisherConfig(publisher_name="my_publisher", publish_timeout=30.0)
        assert config.publish_timeout == 30.0

    def test_publisher_config_requires_name(self):
        """PublisherConfig should require publisher_name."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            PublisherConfig()

    def test_publisher_inherits_gpmq_config(self):
        """PublisherConfig should inherit GPMQConfig defaults."""
        config = PublisherConfig(publisher_name="test")
        assert config.redis_host == "localhost"
        assert config.stream_name == "gpmq:messages"

    def test_publisher_inherits_enable_audit(self):
        """PublisherConfig should inherit enable_audit."""
        config = PublisherConfig(publisher_name="test", enable_audit=False)
        assert config.enable_audit is False

    def test_async_batch_queue_size_default(self):
        """async_batch_queue_size should default to 32."""
        config = PublisherConfig(publisher_name="test")
        assert config.async_batch_queue_size == 32

    def test_async_batch_queue_size_custom(self):
        """async_batch_queue_size should accept custom value."""
        config = PublisherConfig(publisher_name="test", async_batch_queue_size=64)
        assert config.async_batch_queue_size == 64

    def test_async_batch_queue_size_validation(self):
        """async_batch_queue_size should reject values < 1."""
        with pytest.raises(Exception):
            PublisherConfig(publisher_name="test", async_batch_queue_size=0)


class TestSubscriberConfig:
    """Tests for SubscriberConfig."""

    def test_create_subscriber_config(self):
        """SubscriberConfig should be created with required fields."""
        config = SubscriberConfig(
            name="my_subscriber",
            handlers=[
                HandlerConfig(
                    message_types=["user_created"],
                    handler="myapp.handlers.on_user",
                )
            ],
        )
        assert config.name == "my_subscriber"
        assert len(config.handlers) == 1
        assert config.workers == 1

    def test_subscriber_timeout_lives_on_subscriber_config(self):
        """2.2: subscriber_timeout belongs to SubscriberConfig, not the base."""
        config = SubscriberConfig(
            name="test-sub",
            handlers=[HandlerConfig(message_types=["t"], handler="m.f")],
        )
        assert config.subscriber_timeout == 60.0  # default
        base = GPMQConfig()
        assert not hasattr(base, "subscriber_timeout")
        assert not hasattr(base, "enable_registry")

    def test_get_message_types(self):
        """SubscriberConfig.get_message_types should aggregate and deduplicate types."""
        config = SubscriberConfig(
            name="test_subscriber",
            handlers=[
                HandlerConfig(
                    message_types=["user_created", "user_updated"],
                    handler="myapp.handlers.on_user",
                ),
                HandlerConfig(
                    message_types=["user_created", "order_created"],
                    handler="myapp.handlers.on_order",
                ),
            ],
        )
        types = config.get_message_types()
        assert set(types) == {"user_created", "user_updated", "order_created"}
        assert len(types) == 3  # No duplicates

    def test_subscriber_config_requires_name(self):
        """SubscriberConfig should require name."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            SubscriberConfig(
                handlers=[
                    HandlerConfig(
                        message_types=["user_created"],
                        handler="myapp.handlers.on_user",
                    )
                ]
            )

    def test_subscriber_config_requires_handlers(self):
        """SubscriberConfig should require at least one handler."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            SubscriberConfig(name="test", handlers=[])

    def test_subscriber_inherits_gpmq_config(self):
        """SubscriberConfig should inherit GPMQConfig defaults."""
        config = SubscriberConfig(
            name="test",
            handlers=[
                HandlerConfig(
                    message_types=["user_created"],
                    handler="myapp.handlers.on_user",
                )
            ],
        )
        assert config.redis_host == "localhost"
        assert config.stream_name == "gpmq:messages"

    def test_subscriber_inherits_enable_audit(self):
        """SubscriberConfig should inherit enable_audit."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            enable_audit=False,
        )
        assert config.enable_audit is False

    def test_embedded_publisher_default_none(self):
        """embedded_publisher should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.embedded_publisher is None

    def test_embedded_publisher_accepts_path(self):
        """embedded_publisher should accept a config path string."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            embedded_publisher="gpmq.publisher.main",
        )
        assert config.embedded_publisher == "gpmq.publisher.main"

    def test_worker_context_processor_default_none(self):
        """worker_context_processor should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.worker_context_processor is None

    def test_worker_context_processor_accepts_path(self):
        """worker_context_processor should accept an import path string."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context_processor="myapp.processors.init_context",
        )
        assert config.worker_context_processor == "myapp.processors.init_context"

    def test_worker_context_finalizer_default_none(self):
        """worker_context_finalizer should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.worker_context_finalizer is None

    def test_worker_context_finalizer_accepts_path(self):
        """worker_context_finalizer should accept an import path string."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context_finalizer="myapp.processors.close_context",
        )
        assert config.worker_context_finalizer == "myapp.processors.close_context"

    def test_worker_context_default_none(self):
        """worker_context should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.worker_context is None

    def test_worker_context_accepts_dict(self):
        """worker_context should accept a dict."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context={"db_host": "localhost", "api_key": "abc123"},
        )
        assert config.worker_context == {"db_host": "localhost", "api_key": "abc123"}

    def test_run_forever_method_default_none(self):
        """run_forever_method should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.run_forever_method is None

    def test_run_forever_method_accepts_path(self):
        """run_forever_method should accept an import path string."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            run_forever_method="myapp.forever.custom_loop",
        )
        assert config.run_forever_method == "myapp.forever.custom_loop"

    def test_run_forever_method_paras_default_none(self):
        """run_forever_method_paras should default to None."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        assert config.run_forever_method_paras is None

    def test_run_forever_method_paras_accepts_dict(self):
        """run_forever_method_paras should accept a dict."""
        config = SubscriberConfig(
            name="test",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            run_forever_method_paras={"interval": 5, "mode": "active"},
        )
        assert config.run_forever_method_paras == {"interval": 5, "mode": "active"}


class TestRequireCfgMgr:
    """Tests for _require_cfg_mgr helper."""

    def test_raises_runtime_error_when_not_set(self):
        """Should raise RuntimeError when _cfg_mgr is not set."""
        import gpmq.config as cfg_module

        original = cfg_module._gpmq_cfg_mgr
        cfg_module._gpmq_cfg_mgr = None
        try:
            from gpmq.config import _require_cfg_mgr

            with pytest.raises(RuntimeError, match="GPConfigManager"):
                _require_cfg_mgr()
        finally:
            cfg_module._gpmq_cfg_mgr = original

    def test_returns_cfg_mgr_when_set(self):
        """Should return the configured GPConfigManager."""
        from unittest.mock import MagicMock

        from gpconfig import GPConfigManager

        import gpmq.config as cfg_module

        mock_mgr = MagicMock(spec=GPConfigManager)
        original = cfg_module._gpmq_cfg_mgr
        cfg_module._gpmq_cfg_mgr = mock_mgr
        try:
            from gpmq.config import _require_cfg_mgr

            result = _require_cfg_mgr()
            assert result is mock_mgr
        finally:
            cfg_module._gpmq_cfg_mgr = original


class TestGetSubscriber:
    """Tests for get_subscriber factory function."""

    def _setup_mock_mgr(self, common_config, specific_config):
        """Helper to set up mock GPConfigManager."""
        from unittest.mock import MagicMock

        from gpconfig import GPConfigManager

        import gpmq.config as cfg_module

        mock_mgr = MagicMock(spec=GPConfigManager)
        mock_mgr.get_config = MagicMock(
            side_effect=lambda path: {
                "gpmq.common": common_config,
                "gpmq.subscribers.notifier": specific_config,
                "logs": MagicMock(),  # needed by gpmq.log.get_logger
            }.get(path, MagicMock())
        )
        cfg_module._gpmq_cfg_mgr = mock_mgr
        return mock_mgr

    def teardown_method(self):
        """Reset _cfg_mgr after each test."""
        import gpmq.config as cfg_module

        cfg_module._gpmq_cfg_mgr = None

    def test_raises_runtime_error_when_no_cfg_mgr(self):
        """Should raise RuntimeError when GPConfigManager not set."""
        import gpmq.config as cfg_module

        cfg_module._gpmq_cfg_mgr = None

        from gpmq.config import get_subscriber

        with pytest.raises(RuntimeError, match="GPConfigManager"):
            get_subscriber("gpmq.subscribers.notifier")

    def test_returns_subscriber_instance(self):
        """Should return a Subscriber instance."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber
        from gpmq.subscriber.subscriber import Subscriber

        common = GPMQConfig(redis_host="127.0.0.1", redis_password="123456")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[
                HandlerConfig(message_types=["user_created"], handler="mod.func")
            ],
        )
        self._setup_mock_mgr(common, specific)

        result = get_subscriber("gpmq.subscribers.notifier")
        assert isinstance(result, Subscriber)

    def test_merges_common_config_into_subscriber(self):
        """Subscriber config should inherit values from common config."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(
            redis_host="127.0.0.1",
            redis_password="secret",
            stream_name="custom:stream",
        )
        specific = SubscriberConfig(
            name="notifier",
            handlers=[
                HandlerConfig(message_types=["user_created"], handler="mod.func")
            ],
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        # Subscriber inherits common config values
        assert sub.config.redis_host == "127.0.0.1"
        assert sub.config.redis_password == "secret"
        assert sub.config.stream_name == "custom:stream"
        # Subscriber-specific fields preserved
        assert sub.config.name == "notifier"

    def test_specific_overrides_common(self):
        """Specific config values should override common config."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(
            redis_host="127.0.0.1",
            enable_audit=True,
        )
        specific = SubscriberConfig(
            name="notifier",
            handlers=[
                HandlerConfig(message_types=["user_created"], handler="mod.func")
            ],
            enable_audit=False,
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert sub.config.enable_audit is False
        assert sub.config.redis_host == "127.0.0.1"

    def test_embedded_publisher_passes_to_worker_context(self):
        """When embedded_publisher is set, it should be in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            embedded_publisher="gpmq.publisher.main",
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert sub._worker_ctx.get("publisher_cfg_path") == "gpmq.publisher.main"

    def test_no_embedded_publisher_no_publisher_in_context(self):
        """When embedded_publisher is not set, no publisher_cfg_path in context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert "publisher_cfg_path" not in sub._worker_ctx

    def test_worker_context_processor_passes_to_worker_context(self):
        """When worker_context_processor is set, it should be in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context_processor="myapp.processors.init_context",
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert (
            sub._worker_ctx.get("worker_context_processor")
            == "myapp.processors.init_context"
        )

    def test_no_worker_context_processor_no_key_in_context(self):
        """When worker_context_processor is not set, no key in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert "worker_context_processor" not in sub._worker_ctx

    def test_worker_context_finalizer_passes_to_worker_context(self):
        """When worker_context_finalizer is set, it should be in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context_finalizer="myapp.processors.close_context",
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert (
            sub._worker_ctx.get("worker_context_finalizer")
            == "myapp.processors.close_context"
        )

    def test_no_worker_context_finalizer_no_key_in_context(self):
        """When worker_context_finalizer is not set, no key in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert "worker_context_finalizer" not in sub._worker_ctx

    def test_worker_context_applied_to_subscriber(self):
        """When worker_context is set in config, it should be applied to subscriber."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
            worker_context={"db_host": "localhost", "api_key": "abc123"},
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert sub._worker_ctx.get("db_host") == "localhost"
        assert sub._worker_ctx.get("api_key") == "abc123"

    def test_no_worker_context_no_extra_keys(self):
        """When worker_context is None, no extra keys should be in worker context."""
        from gpmq.config import GPMQConfig, SubscriberConfig, get_subscriber

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = SubscriberConfig(
            name="notifier",
            handlers=[HandlerConfig(message_types=["test"], handler="mod.func")],
        )
        self._setup_mock_mgr(common, specific)

        sub = get_subscriber("gpmq.subscribers.notifier")
        assert "db_host" not in sub._worker_ctx


class TestGetClient:
    """Tests for get_client factory function."""

    def _setup_mock_mgr(self, common_config, specific_config=None):
        """Helper to set up mock GPConfigManager."""
        from unittest.mock import MagicMock

        from gpconfig import GPConfigManager

        import gpmq.config as cfg_module

        mock_mgr = MagicMock(spec=GPConfigManager)

        def get_config(path):
            if path == "gpmq.common":
                return common_config
            if specific_config and path == "gpmq.publisher.main":
                return specific_config
            return MagicMock()  # fallback for "logs" etc.

        mock_mgr.get_config = MagicMock(side_effect=get_config)
        cfg_module._gpmq_cfg_mgr = mock_mgr
        return mock_mgr

    def teardown_method(self):
        """Reset _cfg_mgr after each test."""
        import gpmq.config as cfg_module

        cfg_module._gpmq_cfg_mgr = None

    def test_raises_runtime_error_when_no_cfg_mgr(self):
        """Should raise RuntimeError when GPConfigManager not set."""
        import gpmq.config as cfg_module

        cfg_module._gpmq_cfg_mgr = None

        from gpmq.config import get_client

        with pytest.raises(RuntimeError, match="GPConfigManager"):
            get_client()

    def test_anonymous_client_from_common_only(self):
        """Should create anonymous GPMQClient from common config only."""
        from gpmq.client import GPMQClient
        from gpmq.config import GPMQConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1", redis_password="123456")
        self._setup_mock_mgr(common)

        client = get_client()
        assert isinstance(client, GPMQClient)
        assert client.publisher_name == "anonymous"

    def test_client_with_publisher_name(self):
        """Should create GPMQClient with given publisher_name."""
        from gpmq.config import GPMQConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1", redis_password="123456")
        self._setup_mock_mgr(common)

        client = get_client(publisher_name="my_app")
        assert client.publisher_name == "my_app"
        assert client._config.redis_host == "127.0.0.1"

    def test_client_with_cfg_path(self):
        """Should create GPMQClient from merged common + PublisherConfig."""
        from gpmq.config import GPMQConfig, PublisherConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1", redis_password="123456")
        specific = PublisherConfig(publisher_name="my_publisher", publish_timeout=30.0)
        self._setup_mock_mgr(common, specific)

        client = get_client(cfg_path="gpmq.publisher.main")
        assert client.publisher_name == "my_publisher"
        assert client._config.publish_timeout == 30.0
        assert client._config.redis_host == "127.0.0.1"

    def test_client_with_matching_publisher_name(self):
        """cfg_path + publisher_name should work when they match."""
        from gpmq.config import GPMQConfig, PublisherConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = PublisherConfig(publisher_name="my_publisher")
        self._setup_mock_mgr(common, specific)

        client = get_client(
            cfg_path="gpmq.publisher.main", publisher_name="my_publisher"
        )
        assert client.publisher_name == "my_publisher"

    def test_client_with_conflicting_publisher_name_raises(self):
        """cfg_path + publisher_name should raise when they conflict."""
        from gpmq.config import GPMQConfig, PublisherConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = PublisherConfig(publisher_name="config_publisher")
        self._setup_mock_mgr(common, specific)

        with pytest.raises(ValueError, match="publisher_name"):
            get_client(cfg_path="gpmq.publisher.main", publisher_name="different_name")

    def test_timeout_overrides_config(self):
        """timeout parameter should override publish_timeout."""
        from gpmq.config import GPMQConfig, PublisherConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1")
        specific = PublisherConfig(publisher_name="my_publisher", publish_timeout=30.0)
        self._setup_mock_mgr(common, specific)

        client = get_client(cfg_path="gpmq.publisher.main", timeout=60.0)
        assert client._config.publish_timeout == 60.0

    def test_timeout_with_publisher_name_only(self):
        """timeout should work with publisher_name-only mode."""
        from gpmq.config import GPMQConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1")
        self._setup_mock_mgr(common)

        client = get_client(publisher_name="my_app", timeout=45.0)
        assert client.publisher_name == "my_app"
        assert client._config.publish_timeout == 45.0

    def test_timeout_raises_for_anonymous_client(self):
        """timeout should raise ValueError for anonymous client."""
        from gpmq.config import GPMQConfig, get_client

        common = GPMQConfig(redis_host="127.0.0.1")
        self._setup_mock_mgr(common)

        with pytest.raises(ValueError, match="timeout"):
            get_client(timeout=30.0)


class TestDottedDefaultCfgPath:
    """Regression tests for slash-vs-dot cfg addressing under gpconfig >= 0.5.

    gpconfig >= 0.5 rejects literal '/' or '\\' in get_config() paths
    (IllegalPathError), and gpconfig >= 0.4 rejects '.' in default_cfg_path
    at class-definition time (ValueError via GPConfig.__init_subclass__).
    gpmq therefore keeps default_cfg_path slash-style (save contract) and
    addresses configs via the derived dotted form.
    See dev_docs/issues/gpmq-default-cfg-path.md.
    """

    def test_default_cfg_paths_stay_folder_style(self):
        """Guard: dot form here would crash import under gpconfig >= 0.4."""
        assert GPMQConfig.default_cfg_path == "gpmq/common"
        assert PublisherConfig.default_cfg_path == "gpmq/publisher/main"
        assert SubscriberConfig.default_cfg_path == "gpmq/subscribers"
        # Generic guard: any future GPMQConfig subclass with a dotted
        # default_cfg_path would crash import under gpconfig >= 0.4.
        for cfg_cls in [GPMQConfig, *GPMQConfig.__subclasses__()]:
            assert "." not in (cfg_cls.default_cfg_path or "")

    def test_dotted_default_cfg_path_derivation(self):
        """The derived accessor converts folder paths to dot cfg paths."""
        assert GPMQConfig.dotted_default_cfg_path() == "gpmq.common"
        assert PublisherConfig.dotted_default_cfg_path() == "gpmq.publisher.main"
        assert SubscriberConfig.dotted_default_cfg_path() == "gpmq.subscribers"

    def test_dotted_default_cfg_path_normalizes_backslashes(self):
        """Windows-style folder paths derive the same dot cfg_path."""

        class WinStyleCfg(GPMQConfig):
            cfg_class_name = "WinStyleCfg"
            default_cfg_path = "gpmq\\publisher\\main"

        assert WinStyleCfg.dotted_default_cfg_path() == "gpmq.publisher.main"

    def test_dotted_default_cfg_path_drops_empty_segments(self):
        """Double and edge slashes must not produce empty dot segments."""

        class MessyCfg(GPMQConfig):
            cfg_class_name = "MessyCfg"
            default_cfg_path = "/gpmq//common/"

        assert MessyCfg.dotted_default_cfg_path() == "gpmq.common"

    @staticmethod
    def _make_cfg_folder(tmp_path: Path) -> Path:
        """Build a real gpconfig folder: global_env.yaml + gpmq yamls.

        enable_audit is disabled so get_client() creates no SQLite file.
        """
        gpmq_dir = tmp_path / "gpmq"
        (gpmq_dir / "subscribers").mkdir(parents=True)
        (gpmq_dir / "publisher").mkdir(parents=True)
        (tmp_path / "global_env.yaml").write_text("debug: true\n", encoding="utf-8")
        (gpmq_dir / "common.yaml").write_text(
            "cfg_class_name: GPMQConfig\nredis_host: 127.0.0.1\nenable_audit: false\n",
            encoding="utf-8",
        )
        (gpmq_dir / "subscribers" / "notifier.yaml").write_text(
            "cfg_class_name: SubscriberConfig\n"
            "name: notifier\n"
            "handlers:\n"
            '  - message_types: ["demo_type"]\n'
            '    handler: "gpmq.subscriber.examples.on_msg_pingpong"\n',
            encoding="utf-8",
        )
        (gpmq_dir / "publisher" / "main.yaml").write_text(
            "cfg_class_name: PublisherConfig\npublisher_name: test_pub\n",
            encoding="utf-8",
        )
        return tmp_path

    def test_get_client_with_real_config_manager(self, tmp_path):
        """get_client() must work against a real GPConfigManager (0.5.x)."""
        cfg_folder = self._make_cfg_folder(tmp_path)
        saved_mgr = cfg_module._gpmq_cfg_mgr
        set_config_manager(
            GPConfigManager(project_name="gpmq_unit_test", cfg_folder=cfg_folder)
        )
        try:
            client = get_client(cfg_path="gpmq.publisher.main")
            assert client.publisher_name == "test_pub"
        finally:
            cfg_module._gpmq_cfg_mgr = saved_mgr

    def test_get_subscriber_with_real_config_manager(self, tmp_path):
        """get_subscriber() must work against a real GPConfigManager (0.5.x)."""
        cfg_folder = self._make_cfg_folder(tmp_path)
        saved_mgr = cfg_module._gpmq_cfg_mgr
        set_config_manager(
            GPConfigManager(project_name="gpmq_unit_test", cfg_folder=cfg_folder)
        )
        try:
            sub = get_subscriber("gpmq.subscribers.notifier")
            assert sub.name == "notifier"
        finally:
            cfg_module._gpmq_cfg_mgr = saved_mgr
