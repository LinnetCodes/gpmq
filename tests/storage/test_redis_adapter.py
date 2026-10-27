"""Tests for RedisAdapter."""

import time

import pytest


def test_redis_adapter_connect(redis_config):
    """RedisAdapter should connect to Redis."""
    from gpmq.storage.redis_adapter import RedisAdapter

    adapter = RedisAdapter(redis_config)
    adapter.connect()
    assert adapter.ping() is True
    adapter.disconnect()


def test_retry_on_connection_error(redis_config):
    """Retry logic should attempt multiple times on connection error."""
    from gpmq.storage.redis_adapter import RedisAdapter
    from gpmq.config import GPMQConfig
    from gpmq.exceptions import RedisConnectionError

    config = GPMQConfig(
        redis_host="127.0.0.1",
        redis_port=9999,
        redis_db=0,
        redis_max_retries=2,
        redis_retry_delay=0.1,
    )
    adapter = RedisAdapter(config)

    with pytest.raises(RedisConnectionError) as exc_info:
        adapter.connect()

    assert "after 3 attempts" in str(exc_info.value)


def test_xadd(redis_adapter):
    """xadd should add entry to stream and return message_id."""
    message_id = redis_adapter.xadd(
        "test:stream",
        {"type": "user_created", "data": "test"},
    )
    assert message_id is not None
    assert "-" in message_id  # Redis format: timestamp-sequence


def test_xrange(redis_adapter):
    """xrange should read range of entries from stream."""
    # Add test data
    id1 = redis_adapter.xadd("test:stream:range", {"id": "1"})
    id2 = redis_adapter.xadd("test:stream:range", {"id": "2"})

    # Read range
    results = redis_adapter.xrange("test:stream:range", "-", "+", count=10)
    assert len(results) == 2
    assert results[0][0] == id1
    assert results[1][0] == id2
    assert results[0][1]["id"] == "1"


def test_clear_stream(redis_adapter):
    """clear_stream should delete and recreate stream."""
    # Add data
    redis_adapter.xadd("test:clear", {"data": "test"})

    # Clear
    redis_adapter.clear_stream("test:clear")

    # Verify empty
    results = redis_adapter.xrange("test:clear")
    assert len(results) == 0


def test_xgroup_operations(redis_adapter):
    """Consumer group operations should work correctly."""
    # Cleanup any existing group/stream
    redis_adapter.xgroup_destroy("test:group:stream", "test_group")
    redis_adapter.clear_stream("test:group:stream")

    # Create group (with MKSTREAM option, creates empty stream if needed)
    redis_adapter.xgroup_create("test:group:stream", "test_group")

    # Add message
    msg_id = redis_adapter.xadd("test:group:stream", {"type": "test"})

    # Read with consumer group (">" reads new messages)
    results = redis_adapter.xreadgroup(
        "test_group",
        "consumer1",
        {"test:group:stream": ">"},
        count=1,
    )
    assert len(results) == 1
    assert len(results[0][1]) == 1
    assert results[0][1][0][0] == msg_id

    # Ack message
    count = redis_adapter.xack("test:group:stream", "test_group", msg_id)
    assert count == 1


def test_register_subscriber(redis_adapter):
    """register_subscriber should create registry entries."""
    info = {"name": "test_subscriber", "handlers": ["user_created"]}
    redis_adapter.register_subscriber("test_sub", info)

    # Verify in subscribers set
    assert redis_adapter.get_subscriber_info("test_sub") == info


def test_get_subscriber_info(redis_adapter):
    """get_subscriber_info should return subscriber info."""
    info = {"name": "test_sub", "handlers": ["user_created"]}
    redis_adapter.register_subscriber("test_sub", info)

    result = redis_adapter.get_subscriber_info("test_sub")
    assert result == info

    # Non-existent subscriber
    assert redis_adapter.get_subscriber_info("nonexistent") is None


def test_heartbeat_and_active_subscribers(redis_adapter):
    """update_heartbeat and get_active_subscribers should work."""
    redis_adapter.register_subscriber("test_sub", {"name": "test_sub"})

    # Update heartbeat
    redis_adapter.update_heartbeat("test_sub")

    # Get active subscribers
    active = redis_adapter.get_active_subscribers(timeout=60)
    assert "test_sub" in active

    # Old heartbeat should not be active
    time.sleep(0.1)  # Small delay
    active = redis_adapter.get_active_subscribers(timeout=0)
    assert "test_sub" not in active


def test_get_active_subscribers_returns_active_subset(redis_adapter):
    """4.2: pipeline returns exactly the subscribers with fresh heartbeats."""
    import json as _json

    # Register 3 subscribers; 2 get fresh heartbeats, 1 is stale.
    redis_adapter.register_subscriber("fresh-a", {"message_types": ["t"]})
    redis_adapter.register_subscriber("fresh-b", {"message_types": ["t"]})
    redis_adapter.register_subscriber("stale-c", {"message_types": ["t"]})

    redis_adapter.update_heartbeat("fresh-a")
    redis_adapter.update_heartbeat("fresh-b")
    # stale-c: overwrite its heartbeat with an old timestamp directly.
    old_ts = time.time() - 10000
    redis_adapter._client.set(
        f"{redis_adapter._config.registry_key_prefix}subscriber:stale-c:heartbeat",
        _json.dumps({"timestamp": old_ts}),
    )

    active = redis_adapter.get_active_subscribers(timeout=60.0)
    assert "fresh-a" in active
    assert "fresh-b" in active
    assert "stale-c" not in active

    # Cleanup: the fixture teardown doesn't clean the subscriber registry.
    redis_adapter.unregister_subscriber("fresh-a")
    redis_adapter.unregister_subscriber("fresh-b")
    redis_adapter.unregister_subscriber("stale-c")


def test_unregister_subscriber(redis_adapter):
    """unregister_subscriber should remove all subscriber data."""
    redis_adapter.register_subscriber("test_sub", {"name": "test_sub"})
    redis_adapter.unregister_subscriber("test_sub")

    assert redis_adapter.get_subscriber_info("test_sub") is None
    active = redis_adapter.get_active_subscribers(timeout=60)
    assert "test_sub" not in active


def test_publish_subscribe(redis_adapter):
    """Pub/Sub operations should work correctly."""
    channel = "test:pubsub"
    message = {"status": "success", "data": "test"}

    # Subscribe
    pubsub = redis_adapter.subscribe([channel])

    # Publish
    redis_adapter.publish(channel, message)

    # Receive - need to call get_message in a loop to find the message
    # Redis Pub/Sub is asynchronous and may need a moment
    received = None
    for _ in range(10):  # Try multiple times
        received = redis_adapter.get_message(pubsub, timeout=0.5)
        if received is not None:
            break
        # Small sleep between attempts
        time.sleep(0.1)

    assert received is not None
    assert received["channel"] == channel
    assert received["data"] == message

    # Cleanup
    redis_adapter.unsubscribe(pubsub)


def test_xpending(redis_adapter):
    """xpending should return pending message info."""
    # Clean up first
    redis_adapter.xgroup_destroy("test:pending:stream", "test_group")
    redis_adapter.clear_stream("test:pending:stream")

    redis_adapter.xgroup_create("test:pending:stream", "test_group", id="0")
    redis_adapter.xadd("test:pending:stream", {"type": "test"})

    # Read but don't ack
    redis_adapter.xreadgroup(
        "test_group",
        "consumer1",
        {"test:pending:stream": ">"},
        count=1,
    )

    pending = redis_adapter.xpending("test:pending:stream", "test_group")
    assert pending["count"] == 1
    assert "min_id" in pending
    assert "max_id" in pending
    assert "consumers" in pending


def test_xclaim(redis_adapter):
    """xclaim should claim idle messages."""
    # Clean up first
    redis_adapter.xgroup_destroy("test:claim:stream", "test_group")
    redis_adapter.clear_stream("test:claim:stream")

    redis_adapter.xgroup_create("test:claim:stream", "test_group", id="0")
    msg_id = redis_adapter.xadd("test:claim:stream", {"type": "test"})

    # Read with consumer1
    redis_adapter.xreadgroup(
        "test_group",
        "consumer1",
        {"test:claim:stream": ">"},
        count=1,
    )

    # Claim for consumer2 (idle time 0ms means any idle message)
    claimed = redis_adapter.xclaim(
        "test:claim:stream",
        "test_group",
        "consumer2",
        min_idle_time=0,
        message_ids=[msg_id],
    )
    assert len(claimed) == 1
    assert claimed[0][0] == msg_id


def test_disconnect_clears_connection(redis_adapter):
    """disconnect should clear the client."""
    assert redis_adapter.ping() is True
    redis_adapter.disconnect()
    assert redis_adapter.ping() is False


def test_retry_exponential_backoff(redis_config):
    """Retry should use exponential backoff."""
    from gpmq.storage.redis_adapter import RedisAdapter
    from gpmq.config import GPMQConfig
    from gpmq.exceptions import RedisConnectionError
    from unittest.mock import patch

    config = GPMQConfig(
        redis_host="127.0.0.1",
        redis_port=9999,
        redis_max_retries=3,
        redis_retry_delay=0.1,
    )
    adapter = RedisAdapter(config)

    with patch("time.sleep") as mock_sleep:
        with pytest.raises(RedisConnectionError):
            adapter.connect()

        # Check exponential backoff: 0.1, 0.2, 0.4
        # With max_retries=3, we have 4 total attempts but only 3 sleep calls
        assert mock_sleep.call_count == 3
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert delays == [0.1, 0.2, 0.4]


def test_register_worker(redis_adapter):
    """register_worker should add worker info to the consumer group hash."""
    info = {
        "hostname": "server1",
        "pid": 12345,
        "status": "running",
        "started_at": "1713500000.0",
        "last_heartbeat": "1713500100.0",
    }
    redis_adapter.register_worker("test-sub", "test-sub-server1-worker-0", info)

    workers = redis_adapter.get_consumer_group_workers("test-sub")
    assert len(workers) == 1
    assert workers[0]["hostname"] == "server1"
    assert workers[0]["pid"] == 12345


def test_register_multiple_workers(redis_adapter):
    """Multiple workers should be visible in the same consumer group."""
    for i in range(3):
        info = {
            "hostname": f"host{i}",
            "pid": 1000 + i,
            "status": "running",
            "started_at": "1713500000.0",
            "last_heartbeat": "1713500100.0",
        }
        redis_adapter.register_worker("test-multi-sub", f"sub-host{i}-worker-0", info)

    workers = redis_adapter.get_consumer_group_workers("test-multi-sub")
    assert len(workers) == 3


def test_update_worker_heartbeat(redis_adapter):
    """update_worker_heartbeat should update the last_heartbeat field."""
    info = {
        "hostname": "server1",
        "pid": 12345,
        "status": "running",
        "started_at": "1713500000.0",
        "last_heartbeat": "1713500100.0",
    }
    redis_adapter.register_worker("test-hb-sub", "sub-server1-worker-0", info)
    redis_adapter.update_worker_heartbeat("test-hb-sub", "sub-server1-worker-0")

    workers = redis_adapter.get_consumer_group_workers("test-hb-sub")
    assert len(workers) == 1
    assert float(workers[0]["last_heartbeat"]) > 1713500100.0


def test_unregister_worker(redis_adapter):
    """unregister_worker should remove a worker from the consumer group."""
    info = {
        "hostname": "server1",
        "pid": 12345,
        "status": "running",
        "started_at": "1713500000.0",
        "last_heartbeat": "1713500100.0",
    }
    redis_adapter.register_worker("test-unreg-sub", "sub-server1-worker-0", info)
    redis_adapter.unregister_worker("test-unreg-sub", "sub-server1-worker-0")

    workers = redis_adapter.get_consumer_group_workers("test-unreg-sub")
    assert len(workers) == 0


def test_get_consumer_group_workers_empty(redis_adapter):
    """Should return empty list for subscriber with no workers."""
    workers = redis_adapter.get_consumer_group_workers("nonexistent-sub")
    assert workers == []


def test_connect_passes_socket_timeout_explicitly():
    """connect() must pass redis_socket_timeout to redis.Redis.

    redis-py >= 6 defaults to socket_timeout=5s with built-in retries;
    leaving it implicit races the worker's 5s XREADGROUP block. Explicit
    is the contract.
    """
    from unittest.mock import MagicMock, patch

    from gpmq.config import GPMQConfig
    from gpmq.storage.redis_adapter import RedisAdapter

    adapter = RedisAdapter(GPMQConfig(redis_socket_timeout=12.5))
    with patch("gpmq.storage.redis_adapter.redis.Redis") as mock_redis:
        mock_redis.return_value = MagicMock()
        adapter.connect()

    _, kwargs = mock_redis.call_args
    assert kwargs["socket_timeout"] == 12.5


def test_connect_socket_timeout_default_is_10(redis_config):
    """Live client must carry the 10.0 default from config (integration)."""
    from gpmq.storage.redis_adapter import RedisAdapter

    adapter = RedisAdapter(redis_config)
    adapter.connect()
    try:
        conn_kwargs = adapter._require_client().connection_pool.connection_kwargs
        assert conn_kwargs["socket_timeout"] == 10.0
    finally:
        adapter.disconnect()


def test_idle_xreadgroup_returns_at_block_cadence(redis_adapter):
    """Idle XREADGROUP(block_ms=5000) must return at ~5s, not 10s+.

    Regression test for the redis-py >= 6 default-socket_timeout race: with
    socket_timeout <= block_ms the client timeout fires first and redis-py
    transparently re-issues the blocking command, so an idle read takes at
    least 2x block. With redis_socket_timeout=10.0 the server's nil reply
    wins the race and the read returns at the block cadence.
    """
    stream = "test:stream:idle"
    group = "test_idle_group"
    redis_adapter.xgroup_destroy(stream, group)
    redis_adapter.clear_stream(stream)
    try:
        redis_adapter.xgroup_create(stream, group)
        start = time.monotonic()
        messages = redis_adapter.xreadgroup(
            group, "consumer1", {stream: ">"}, count=1, block_ms=5000
        )
        elapsed = time.monotonic() - start
        assert messages == []
        assert 4.5 <= elapsed < 9.0
    finally:
        redis_adapter.clear_stream(stream)
