"""Integration tests for the orphan PEL abandon sweep (requires live Redis).

The orphan is aged server-side with XCLAIM IDLE rather than a real-time
sleep, so Redis server clock steps cannot swallow the idle window.
"""

from __future__ import annotations

from typing import Any

import pytest

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.models.result import ProcessStatus
from gpmq.storage.redis_adapter import RedisAdapter
from gpmq.subscriber.subscriber import Subscriber

STREAM = "test:stream:abandon"
GROUP = "abandon_it"


def _cfg(**updates: Any) -> SubscriberConfig:
    return SubscriberConfig(
        name=GROUP,
        handlers=[HandlerConfig(message_types=["t"], handler="m.f")],
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
    # being on this stream) blocks every trim, failing the first trim assert.
    _reset(adapter)
    yield adapter
    _reset(adapter)
    adapter.disconnect()


def _seed_orphan(adapter: RedisAdapter) -> list[str]:
    """Deliver 3 entries to GROUP; ack #0 and #2, leave #1 pending (orphan)."""
    ids = [adapter.xadd(STREAM, {"seq": str(i)}) for i in range(3)]
    adapter.xgroup_create(STREAM, GROUP)
    while adapter.xreadgroup(GROUP, "orphan-consumer", {STREAM: ">"}, count=10):
        pass
    adapter.xack(STREAM, GROUP, ids[0])
    adapter.xack(STREAM, GROUP, ids[2])
    return ids


def test_orphan_abandoned_unlocks_trim(adapter, tmp_path):
    config = _cfg(
        pel_abandon_idle_seconds=30.0,
        audit_db_path=str(tmp_path / "audit.db"),
    )
    ids = _seed_orphan(adapter)
    adapter.register_subscriber(
        GROUP, {"message_types": ["t"], "workers": 1, "stream_name": STREAM}
    )

    # The pending middle entry pins the safe point at itself: only ids[0]
    # (strictly below) is reclaimable.
    assert adapter.trim_stream_to_registered_progress() == 1
    assert adapter.xpending(STREAM, GROUP)["pending"] == 1

    subscriber = Subscriber(config)
    subscriber._redis = adapter  # real adapter; start() not called

    # Right after delivery the entry is moments old (idle ~0) against the
    # 30s threshold: no abandon.
    subscriber._maybe_abandon_pending()
    assert adapter.xpending(STREAM, GROUP)["pending"] == 1

    # Age the orphan deterministically: a server clock that steps backward
    # (WSL2 time sync does) makes sleep-based idle aging nondeterministic,
    # so set the idle time server-side instead. XCLAIM IDLE with the SAME
    # consumer leaves ownership untouched; JUSTID returns bare IDs and does
    # not increment the delivery counter. The live idle filter is still
    # exercised by the fresh-entry assertion above and by the adapter-level
    # idle filter tests (test_redis_adapter.py).
    # Direct _client access is test-only seeding of server-side state (like
    # _go_offline in test_stream_trim_integration.py), not production usage.
    assert adapter._client.xclaim(
        STREAM,
        GROUP,
        "orphan-consumer",
        min_idle_time=0,
        message_ids=[ids[1]],
        idle=60000,
        justid=True,
    ) == [ids[1]]
    subscriber._maybe_abandon_pending()
    assert adapter.xpending(STREAM, GROUP)["pending"] == 0

    # Idempotent re-sweep: no second audit row, PEL stays empty.
    subscriber._maybe_abandon_pending()
    assert adapter.xpending(STREAM, GROUP)["pending"] == 0

    # Audit carries the ABANDONED placeholder for the orphan.
    assert subscriber._audit is not None
    rows = subscriber._audit.query_by_status(ProcessStatus.ABANDONED)
    assert [r["message_id"] for r in rows] == [ids[1]]
    assert rows[0]["subscriber_name"] == GROUP

    # Safe point unpinned: the orphan's entry (already xack'd) is reclaimed.
    assert adapter.trim_stream_to_registered_progress() == 1
    assert [mid for mid, _f in adapter.xrange(STREAM)] == [ids[2]]

    # Manual teardown: stop() would unregister/disconnect the shared adapter.
    subscriber._audit.close()
    subscriber._redis = None
