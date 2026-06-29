"""Test fixtures for GPMQ tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import redis
import pytest

if TYPE_CHECKING:
    from gpmq.config import GPMQConfig, PublisherConfig
    from gpmq.storage.audit_store import AuditStore
    from gpmq.storage.redis_adapter import RedisAdapter


@pytest.fixture
def redis_config() -> GPMQConfig:
    """Create test Redis config."""
    from gpmq.config import GPMQConfig
    return GPMQConfig(
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_password="123456",
        redis_db=15,  # Separate test DB
    )


@pytest.fixture
def redis_adapter(redis_config) -> RedisAdapter:
    """Create RedisAdapter with auto-cleanup."""
    from gpmq.storage.redis_adapter import RedisAdapter
    adapter = RedisAdapter(redis_config)
    adapter.connect()
    yield adapter
    # Cleanup test streams before disconnect
    try:
        # Clean up test streams
        for stream in ["test:stream", "test:stream:range", "gpmq:messages"]:
            try:
                adapter._client.delete(stream)
            except (redis.ConnectionError, redis.RedisError, AttributeError):
                pass
    except (redis.ConnectionError, redis.RedisError):
        pass  # Ignore cleanup errors
    finally:
        adapter.disconnect()


@pytest.fixture
def temp_db_path(tmp_path) -> str:
    """Create temporary SQLite database path."""
    return str(tmp_path / "test_audit.db")


@pytest.fixture
def audit_store(temp_db_path) -> AuditStore:
    """Create AuditStore with auto-cleanup."""
    from gpmq.storage.audit_store import AuditStore
    store = AuditStore(temp_db_path)
    store.connect()
    yield store
    store.close()


@pytest.fixture
def publisher_config() -> PublisherConfig:
    """Create test publisher config."""
    from gpmq.config import PublisherConfig
    return PublisherConfig(
        publisher_name="test_publisher",
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_password="123456",
        redis_db=15,
        stream_name="test:gpmq:messages",
        registry_key_prefix="gpmq:",
    )
