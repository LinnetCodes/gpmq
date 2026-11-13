"""Integration tests for the orphan PEL abandon sweep (requires live Redis).

The orphan is aged server-side with XCLAIM IDLE rather than a real-time
sleep, so Redis server clock steps cannot swallow the idle window.
"""

from __future__ import annotations

from typing import Any

import pytest

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.models.result import ProcessStatus
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
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


def test_abandoned_receipt_unblocks_rpc_wait(adapter, tmp_path):
    """Full RPC loop: the sweep's ABANDONED receipt unblocks wait().

    A caller blocked on the reply channel must learn the true terminal
    state from the sweep — not synthesize its own TIMEOUT after the full
    deadline. Exercises the exact production order: subscribe before the
    xadd, receipt published by the sweep, then wait() consumes the
    already-queued pub/sub message.
    """
    config = _cfg(
        pel_abandon_idle_seconds=30.0,
        audit_db_path=str(tmp_path / "sub.db"),
    )
    token = "tok-abandon-rpc"
    publisher_audit = AuditStore(str(tmp_path / "pub.db"))
    publisher_audit.connect()

    # Caller-side subscription BEFORE the message exists (mirrors
    # Publisher.publish_message: subscribe-then-xadd, so the sweep's
    # publish can never race ahead of the subscription).
    handler = ResultHandler(
        reply_token=token,
        subscriber_names=[GROUP],
        redis_adapter=adapter,
        key_prefix=config.registry_key_prefix,
        audit_store=publisher_audit,
    )
    try:
        message_id = adapter.xadd(STREAM, {"type": "t.x", "reply_token": token})
        handler.bind(message_id)
        adapter.xgroup_create(STREAM, GROUP)
        while adapter.xreadgroup(GROUP, "orphan-consumer", {STREAM: ">"}, count=10):
            pass

        subscriber = Subscriber(config)
        subscriber._redis = adapter  # real adapter; start() not called

        # Fresh entry is below the 30s idle threshold: no abandon, the
        # caller would still be waiting (live idle filter).
        subscriber._maybe_abandon_pending()
        assert not handler.is_complete()

        # Ripen server-side, same XCLAIM IDLE trick as the test above.
        assert adapter._client.xclaim(
            STREAM,
            GROUP,
            "orphan-consumer",
            min_idle_time=0,
            message_ids=[message_id],
            idle=60000,
            justid=True,
        ) == [message_id]

        subscriber._maybe_abandon_pending()  # publishes the receipt
        assert adapter.xpending(STREAM, GROUP)["pending"] == 0

        # wait() must return the ABANDONED receipt, not a TIMEOUT.
        results = handler.wait(timeout=5.0)
        assert set(results) == {GROUP}
        assert results[GROUP].status is ProcessStatus.ABANDONED
        assert results[GROUP].message_id == message_id

        # Both sides' audits agree on the terminal state.
        pub_rows = publisher_audit.query_by_status(ProcessStatus.ABANDONED)
        assert [r["message_id"] for r in pub_rows] == [message_id]
        assert subscriber._audit is not None
        sub_rows = subscriber._audit.query_by_status(ProcessStatus.ABANDONED)
        assert [r["message_id"] for r in sub_rows] == [message_id]

        subscriber._audit.close()
        subscriber._redis = None
    finally:
        publisher_audit.close()
