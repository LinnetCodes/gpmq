"""Integration tests for stream retention (requires live Redis)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import pytest

from gpmq.config import GPMQConfig
from gpmq.models.message import Message
from gpmq.publisher.publisher import Publisher
from gpmq.storage.redis_adapter import RedisAdapter

STREAM = "test:stream:trim"


def _cfg(**updates: Any) -> GPMQConfig:
    return GPMQConfig(
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_password="123456",
        redis_db=15,
        stream_name=STREAM,
        **updates,
    )


def _reset(adapter: RedisAdapter) -> None:
    """Empty the registry and drop the test stream (with its groups)."""
    for name in adapter.get_registered_subscribers():
        adapter.unregister_subscriber(name)
    adapter.clear_stream(STREAM)


@pytest.fixture
def adapter() -> RedisAdapter:
    adapter = RedisAdapter(_cfg())
    adapter.connect()
    # Reset on setup too: db 15 is shared with other suites, and a leftover
    # registration (e.g. a legacy info without stream_name, which counts as
    # being on this stream) blocks every trim and disables the idle cap.
    _reset(adapter)
    yield adapter
    _reset(adapter)
    adapter.disconnect()


def _register(adapter: RedisAdapter, name: str) -> None:
    adapter.register_subscriber(
        name, {"message_types": ["t.ping"], "workers": 1, "stream_name": STREAM}
    )


def _go_offline(adapter: RedisAdapter, name: str) -> None:
    """Make a registered subscriber read as offline, as a crashed one does.

    register_subscriber() stamps the heartbeat with "now" (active for
    heartbeat_interval * 3); a crash leaves the key (no TTL) with its last,
    aging timestamp. Backdate it well past the liveness window.
    """
    adapter._client.set(
        f"{adapter._config.registry_key_prefix}subscriber:{name}:heartbeat",
        json.dumps({"timestamp": time.time() - 10_000}),
    )


def _xadd_n(adapter: RedisAdapter, n: int) -> list[str]:
    return [adapter.xadd(STREAM, {"seq": str(i)}) for i in range(n)]


def _read_all_and_ack(adapter: RedisAdapter, group: str) -> list[str]:
    adapter.xgroup_create(STREAM, group)
    delivered: list[str] = []
    while True:
        res = adapter.xreadgroup(group, f"{group}-c", {STREAM: ">"}, count=10)
        if not res:
            break
        for _s, entries in res:
            for mid, _f in entries:
                adapter.xack(STREAM, group, mid)
                delivered.append(mid)
    return delivered


def _ids(adapter: RedisAdapter) -> list[str]:
    return [mid for mid, _f in adapter.xrange(STREAM, "-", "+", count=1000)]


def _message() -> Message:
    return Message(
        id="pending",
        type="t.ping",
        timestamp=datetime.now(timezone.utc),
        publisher="trim_test",
        payload={"x": 1},
    )


def _read_all_and_ack_except(adapter: RedisAdapter, group: str, keep: int) -> list[str]:
    """Deliver everything to the group; ack all but the last `keep` entries."""
    adapter.xgroup_create(STREAM, group)
    delivered: list[str] = []
    while True:
        res = adapter.xreadgroup(group, f"{group}-c", {STREAM: ">"}, count=10)
        if not res:
            break
        for _s, entries in res:
            for mid, _f in entries:
                delivered.append(mid)
    for mid in delivered[: len(delivered) - keep]:
        adapter.xack(STREAM, group, mid)
    return delivered


def test_trim_removes_acked_entries_down_to_safe_point(adapter):
    """XTRIM MINID is strictly-lower-than: everything below the safe point
    (the group's cursor, i.e. the newest acked entry) is removed; the entry
    AT the safe point survives until a later trim advances past it —
    steady-state residue is O(1)."""
    ids = _xadd_n(adapter, 3)
    _read_all_and_ack(adapter, "trim_a")
    _register(adapter, "trim_a")
    assert adapter.trim_stream_to_registered_progress() == 2
    assert _ids(adapter) == [ids[2]]


def test_pending_entry_blocks_trim_below_it(adapter):
    """Nothing may be removed at or above the smallest pending (unacked) ID."""
    _xadd_n(adapter, 3)
    delivered = _read_all_and_ack_except(adapter, "trim_a", keep=3)  # ack none
    _register(adapter, "trim_a")
    assert adapter.trim_stream_to_registered_progress() == 0
    assert _ids(adapter) == delivered  # everything survives

    for mid in delivered[:2]:  # ack the two oldest
        adapter.xack(STREAM, "trim_a", mid)
    assert adapter.trim_stream_to_registered_progress() == 2
    assert _ids(adapter) == [delivered[2]]


def test_unregistered_leftover_group_does_not_block(adapter):
    ids = _xadd_n(adapter, 3)
    adapter.xgroup_create(STREAM, "leftover")  # never read, never registered
    _read_all_and_ack(adapter, "trim_a")
    _register(adapter, "trim_a")
    assert adapter.trim_stream_to_registered_progress() == 2  # safe point = last entry
    assert _ids(adapter) == [ids[2]]


def test_registered_without_group_blocks(adapter):
    _xadd_n(adapter, 3)
    _read_all_and_ack(adapter, "trim_a")
    _register(adapter, "trim_a")
    _register(adapter, "trim_ghost")  # registered, no group on the stream
    assert adapter.trim_stream_to_registered_progress() == 0
    assert len(_ids(adapter)) == 3


def test_publisher_gate_skips_without_registered_handler(adapter):
    publisher = Publisher(adapter, None, _cfg(publish_require_subscriber=True))
    message_id, handler = publisher.publish_message(_message())
    assert message_id == ""
    assert handler.wait(timeout=1) == {}
    assert _ids(adapter) == []


def test_publisher_gate_publishes_for_registered_offline_handler(adapter):
    _register(adapter, "trim_off")
    _go_offline(adapter, "trim_off")  # registered, heartbeat stale (crashed)
    publisher = Publisher(adapter, None, _cfg(publish_require_subscriber=True))
    # Offline for real: a liveness-based gate would skip this publish.
    assert "trim_off" not in publisher.get_active_subscribers()
    message_id, handler = publisher.publish_message(_message())
    assert message_id != ""
    assert handler.wait(timeout=1) == {}
    assert len(_ids(adapter)) == 1


def test_send_gate_publishes_for_registered_offline_handler(adapter):
    _register(adapter, "trim_off2")
    _go_offline(adapter, "trim_off2")  # registered, heartbeat stale (crashed)
    publisher = Publisher(adapter, None, _cfg(publish_require_subscriber=True))
    # Offline for real: a liveness-based gate would skip this send.
    assert "trim_off2" not in publisher.get_active_subscribers()
    result = publisher.send_message(_message())
    assert result is not None
    assert len(_ids(adapter)) == 1


def test_gate_matches_on_message_type_not_just_registration(adapter):
    """A registered subscriber that declares only another message type does
    not open the gate: matching is on declared type, not mere registration."""
    # Heartbeat left fresh on purpose: the subscriber is active, so the
    # publish half also pins the type filter on the active-subscriber path.
    adapter.register_subscriber(
        "trim_other", {"message_types": ["t.other"], "stream_name": STREAM}
    )
    publisher = Publisher(adapter, None, _cfg(publish_require_subscriber=True))

    message_id, handler = publisher.publish_message(_message())
    assert message_id == ""
    assert handler.wait(timeout=1) == {}
    assert _ids(adapter) == []

    assert publisher.send_message(_message()) is None
    assert _ids(adapter) == []


def test_idle_maxlen_caps_stream_when_registry_empty(adapter):
    publisher = Publisher(adapter, None, _cfg(stream_trim_idle_maxlen=2))
    for _ in range(6):
        assert publisher.send_message(_message()) is not None
    assert len(_ids(adapter)) == 2  # exact cap (approximate=False)
