"""Integration tests for sticky routing against a real Redis (db 15).

Three scenarios over one stream/group pair (construction patterns mirror
``test_abandon_integration.py``):

- Handoff end-to-end: worker-0 reads a ``target_worker=1`` envelope and
  XCLAIMs it into slot 1's consumer PEL (liveness gate satisfied),
  without processing it locally.
- Restart inheritance: a fresh Worker with the SAME worker_id (slot 1)
  picks the claimed entry up via its ① own-PEL cursor poll (restart
  cursor ``"0"``) and processes it exactly once.
- Exemption, no false abandon: an idle-matured routed entry whose PEL
  OWNER is heartbeat-fresh is exempted from the orphan sweep (stays
  queued, no ABANDONED receipt); once the owner's heartbeat goes stale
  the same entry IS abandoned (xack + ABANDONED receipt unblocking a
  waiting RPC caller).

Idle maturity is forced server-side with XCLAIM IDLE (same consumer,
JUSTID) instead of a real-time sleep: a server clock that steps backward
(WSL2 time sync does) makes sleep-based aging nondeterministic. The
small ``pel_abandon_idle_seconds`` threshold keeps the sweeps fast.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.exceptions import RedisConnectionError
from gpmq.models.message import Message
from gpmq.models.result import ProcessStatus
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter
from gpmq.subscriber.handler import HandlerRegistry
from gpmq.subscriber.subscriber import Subscriber
from gpmq.subscriber.worker import Worker

STREAM = "test:stream:routing"
GROUP = "routing_it"
HOSTNAME = "hostA"
MSG_TYPE = "routing_it_type"
WORKER_0 = f"{GROUP}-{HOSTNAME}-worker-0"
WORKER_1 = f"{GROUP}-{HOSTNAME}-worker-1"
CONSUMER_0 = f"{GROUP}:{WORKER_0}"
CONSUMER_1 = f"{GROUP}:{WORKER_1}"


def _cfg(**updates: Any) -> SubscriberConfig:
    """Subscriber config bound to the db-15 test Redis and STREAM."""
    return SubscriberConfig(
        name=GROUP,
        handlers=[HandlerConfig(message_types=[MSG_TYPE], handler="m.f")],
        redis_host="127.0.0.1",
        redis_port=6379,
        redis_password="123456",
        redis_db=15,
        stream_name=STREAM,
        **updates,
    )


def _reset(adapter: RedisAdapter) -> None:
    """Drop this module's stream (with its group) and workers hash."""
    # Direct _client access is test-only cleanup of seeded server-side
    # state, not production usage (same ruling as the abandon tests).
    adapter._client.delete(f"{adapter._config.registry_key_prefix}workers:{GROUP}")
    adapter.clear_stream(STREAM)


@pytest.fixture
def adapter() -> Iterator[RedisAdapter]:
    """Real adapter on db 15; skip gracefully when Redis is unreachable."""
    adapter = RedisAdapter(_cfg())
    try:
        adapter.connect()
    except RedisConnectionError:
        pytest.skip("live Redis at 127.0.0.1:6379 (password 123456, db 15) required")
    _reset(adapter)  # db 15 is shared; clear leftovers of crashed runs
    yield adapter
    _reset(adapter)
    adapter.disconnect()


@pytest.fixture
def routing_handler() -> Iterator[list[Message]]:
    """Register a recording handler for MSG_TYPE; yield the call log."""
    calls: list[Message] = []

    def _handler(msg: Message) -> None:
        calls.append(msg)

    HandlerRegistry._handlers[MSG_TYPE] = _handler
    yield calls
    HandlerRegistry.clear()  # global registry: avoid cross-test leakage


def _envelope(target_worker: str, reply_token: str | None = None) -> dict[str, str]:
    """Raw stream fields of one routed envelope (adapted from test_worker)."""
    fields = {
        "type": MSG_TYPE,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "publisher": "routing-it-pub",
        "payload": json.dumps({"key": "val"}),
    }
    if reply_token is not None:
        fields["reply_token"] = reply_token
    fields["target_worker"] = target_worker
    return fields


def _register_worker_hb(
    adapter: RedisAdapter, worker_id: str, age: float = 0.0
) -> None:
    """Seed the workers hash, mirroring Worker._register_self's JSON shape."""
    now = time.time()
    adapter.register_worker(
        GROUP,
        worker_id,
        {
            "hostname": HOSTNAME,
            "pid": 0,
            "status": "running",
            "started_at": str(now - age - 1.0),
            "last_heartbeat": str(now - age),
        },
    )


def _make_worker(adapter: RedisAdapter, slot: int) -> Worker:
    """Construct a Worker for *slot* without starting its process."""
    worker = Worker(
        subscriber_name=GROUP,
        stream_name=STREAM,
        message_types=[MSG_TYPE],
        worker_id=f"{GROUP}-{HOSTNAME}-worker-{slot}",
        hostname=HOSTNAME,
        config_dict=_cfg().model_dump(),
    )
    worker._redis = adapter  # injected like the unit tests; run() not called
    worker._audit = None
    return worker


def test_handoff_moves_entry_to_target_consumer_pel(adapter, routing_handler):
    """Scenario A: slot 0 hands a target_worker=1 entry to slot 1.

    The registry carries a fresh slot-1 heartbeat, so the liveness gate
    passes and the entry is XCLAIMed into slot 1's consumer PEL; slot 0
    neither processes nor acks it.
    """
    _register_worker_hb(adapter, WORKER_1)  # slot 1 looks alive
    msg_id = adapter.xadd(STREAM, _envelope(target_worker="1"))
    adapter.xgroup_create(STREAM, GROUP)

    worker = _make_worker(adapter, slot=0)
    with patch.object(worker, "_process_message") as process_spy:
        worker._read_and_process()  # ① empty, ② `>` delivers to slot 0

    # The batched XCLAIM moved the entry into slot 1's consumer PEL.
    claimed = adapter.xpending_range(STREAM, GROUP, consumer=CONSUMER_1)
    assert [e["message_id"] for e in claimed] == [msg_id]
    # Slot 0 keeps nothing pending and processed nothing locally.
    assert adapter.xpending_range(STREAM, GROUP, consumer=CONSUMER_0) == []
    process_spy.assert_not_called()
    assert routing_handler == []  # handler never dispatched
    # Exactly the one entry group-wide, still unacked.
    assert adapter.xpending(STREAM, GROUP)["pending"] == 1


def test_restarted_worker_inherits_claimed_entry(adapter, routing_handler, tmp_path):
    """Scenario B: a fresh same-worker_id instance inherits the claimed entry.

    Seeded post-handoff state: the entry sits in slot 1's consumer PEL.
    The restarted slot-1 Worker must pick it up via its ① own-PEL poll
    (cursor ``"0"``) and process it exactly once (from_pel path).
    """
    msg_id = adapter.xadd(STREAM, _envelope(target_worker="1"))
    adapter.xgroup_create(STREAM, GROUP)
    # Seed the post-handoff state: delivered to slot 0, claimed to slot 1.
    assert adapter.xreadgroup(GROUP, CONSUMER_0, {STREAM: ">"}, count=10)
    assert adapter.xclaim(STREAM, GROUP, CONSUMER_1, 0, [msg_id])
    owned = adapter.xpending_range(STREAM, GROUP, consumer=CONSUMER_1)
    assert [e["message_id"] for e in owned] == [msg_id]
    _register_worker_hb(adapter, WORKER_1)

    audit = AuditStore(str(tmp_path / "worker1-audit.db"))
    audit.connect()
    try:
        worker = _make_worker(adapter, slot=1)  # FRESH instance, same id
        worker._audit = audit
        with patch.object(adapter, "xreadgroup", wraps=adapter.xreadgroup) as read_spy:
            worker._read_and_process()

        # The ① own-PEL poll (restart cursor "0", this consumer) returned
        # the entry; drain-first means no ② `>` read was issued at all.
        assert read_spy.call_count == 1
        first = read_spy.call_args_list[0].kwargs
        assert first["streams"] == {STREAM: "0"}
        assert first["consumer"] == CONSUMER_1
        assert first["block_ms"] is None

        # Processed exactly once (the routed-to-me entry), then acked.
        assert [m.id for m in routing_handler] == [msg_id]
        assert routing_handler[0].target_worker == 1
        assert adapter.xpending(STREAM, GROUP)["pending"] == 0
        rows = audit.query_by_status(ProcessStatus.SUCCESS)
        assert [r["message_id"] for r in rows] == [msg_id]
        assert worker._pel_cursor == msg_id  # cursor advanced past it
    finally:
        audit.close()


def test_routed_entry_exempt_while_owner_alive_then_abandoned(adapter, tmp_path):
    """Scenario C: live-owner exemption holds; a stale owner is abandoned.

    The entry is idle-matured server-side (XCLAIM IDLE) while its PEL
    owner's heartbeat is fresh: the sweep must exempt it (stays queued,
    no ABANDONED receipt). Flipping the heartbeat stale makes the same
    sweep abandon it (xack + receipt to the waiting RPC caller).
    """
    config = _cfg(
        pel_abandon_idle_seconds=0.1,  # small threshold: fast, no sleeps
        audit_db_path=str(tmp_path / "sweep-audit.db"),
    )
    token = "tok-routing-exempt"
    # Caller-side subscription BEFORE the xadd (production order: the
    # sweep's publish can never race ahead of the subscription).
    handler = ResultHandler(
        reply_token=token,
        subscriber_names=[GROUP],
        redis_adapter=adapter,
        key_prefix=config.registry_key_prefix,
    )
    try:
        msg_id = adapter.xadd(STREAM, _envelope(target_worker="1", reply_token=token))
        handler.bind(msg_id)
        adapter.xgroup_create(STREAM, GROUP)
        # Deliver straight into slot 1's PEL (the idle owner is slot 1).
        assert adapter.xreadgroup(GROUP, CONSUMER_1, {STREAM: ">"}, count=10)
        _register_worker_hb(adapter, WORKER_1)  # PEL owner heartbeat-fresh

        # Mature the idle time server-side (WSL2 clock lesson: no sleep).
        # Direct _client use is test-only state seeding, as in the
        # abandon tests: same consumer + JUSTID leaves ownership and the
        # delivery counter untouched.
        assert adapter._client.xclaim(
            STREAM,
            GROUP,
            CONSUMER_1,
            min_idle_time=0,
            message_ids=[msg_id],
            idle=2000,
            justid=True,
        ) == [msg_id]

        subscriber = Subscriber(config)
        subscriber._redis = adapter  # real adapter; start() not called
        try:
            # Owner alive: the entry is idle-matured yet EXEMPT.
            subscriber._maybe_abandon_pending()
            assert adapter.xpending(STREAM, GROUP)["pending"] == 1
            still_queued = adapter.xpending_range(STREAM, GROUP, consumer=CONSUMER_1)
            assert [e["message_id"] for e in still_queued] == [msg_id]
            assert subscriber._audit is None  # exempt chain never ran
            assert not handler.is_complete()  # no false ABANDONED receipt

            # Owner's heartbeat goes stale: the same sweep now abandons.
            _register_worker_hb(adapter, WORKER_1, age=9999.0)
            subscriber._maybe_abandon_pending()
            assert adapter.xpending(STREAM, GROUP)["pending"] == 0

            results = handler.wait(timeout=5.0)
            assert set(results) == {GROUP}
            assert results[GROUP].status is ProcessStatus.ABANDONED
            assert results[GROUP].message_id == msg_id
            assert subscriber._audit is not None
            rows = subscriber._audit.query_by_status(ProcessStatus.ABANDONED)
            assert [r["message_id"] for r in rows] == [msg_id]
        finally:
            if subscriber._audit is not None:
                subscriber._audit.close()
            subscriber._redis = None  # manual teardown: shared adapter
    finally:
        # wait() already unsubscribed on success; only a mid-test failure
        # leaves the subscription open.
        if handler._pubsub is not None:
            adapter.unsubscribe(handler._pubsub)
