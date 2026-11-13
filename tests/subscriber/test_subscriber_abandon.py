"""Unit tests for the subscriber-side orphan PEL abandon sweep."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.models.result import ProcessStatus
from gpmq.subscriber.subscriber import Subscriber


def _make_subscriber(**overrides) -> Subscriber:
    config = SubscriberConfig(
        name="abandon_test",
        handlers=[HandlerConfig(message_types=["t"], handler="m.f")],
        heartbeat_interval=5,
        **overrides,
    )
    return Subscriber(config)


def _with_redis(subscriber: Subscriber) -> MagicMock:
    adapter = MagicMock()
    subscriber._redis = adapter  # type: ignore[assignment]
    return adapter


def _entry(message_id: str, consumer: str = "abandon_test:h1-worker-0") -> dict:
    return {
        "message_id": message_id,
        "consumer": consumer,
        "time_since_delivered": 65_000,
        "times_delivered": 1,
    }


class TestMaybeAbandonPending:
    def test_disabled_when_idle_zero(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=0.0)
        adapter = _with_redis(subscriber)
        subscriber._maybe_abandon_pending()
        adapter.xpending_range.assert_not_called()

    def test_no_redis_is_noop(self):
        subscriber = _make_subscriber()  # _redis is None until start()
        subscriber._maybe_abandon_pending()  # must not raise

    def test_abandons_idle_entries(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0"), _entry("6-0")]
        adapter.xrange.return_value = [("5-0", {"type": "t.x", "publisher": "p"})]

        subscriber._maybe_abandon_pending()

        # seconds -> milliseconds conversion happens at the call site
        adapter.xpending_range.assert_called_once_with(
            "gpmq:messages", "abandon_test", count=100, idle_ms=60_000
        )
        assert adapter.xack.call_count == 2
        adapter.xack.assert_any_call("gpmq:messages", "abandon_test", "5-0")
        adapter.xack.assert_any_call("gpmq:messages", "abandon_test", "6-0")

    def test_audit_written_before_xack(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0, enable_audit=True)
        adapter = _with_redis(subscriber)
        audit = MagicMock()
        subscriber._audit = audit  # pre-injected: skip opening SQLite
        adapter.xpending_range.return_value = [_entry("5-0")]

        calls: list[str] = []
        audit.save_result.side_effect = lambda *a, **k: calls.append("save")
        adapter.xack.side_effect = lambda *a, **k: calls.append("xack")

        subscriber._maybe_abandon_pending()

        assert calls == ["save", "xack"]
        result = audit.save_result.call_args[0][1]
        assert result.status.value == "abandoned"
        assert result.message_id == "5-0"
        assert result.subscriber_name == "abandon_test"
        assert "abandoned: idle 65.0s in PEL" in result.error_message
        assert "abandon_test:h1-worker-0" in result.error_message

    def test_audit_failure_still_xacks(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0, enable_audit=True)
        adapter = _with_redis(subscriber)
        audit = MagicMock()
        audit.save_result.side_effect = RuntimeError("sqlite locked")
        subscriber._audit = audit
        adapter.xpending_range.return_value = [_entry("5-0")]

        subscriber._maybe_abandon_pending()  # must not raise

        adapter.xack.assert_called_once_with("gpmq:messages", "abandon_test", "5-0")

    def test_audit_disabled_never_opens_store(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0, enable_audit=False)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]

        subscriber._maybe_abandon_pending()

        assert subscriber._audit is None
        adapter.xack.assert_called_once()

    def test_xrange_failure_still_abandons(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.side_effect = RuntimeError("boom")

        subscriber._maybe_abandon_pending()  # must not raise

        adapter.xack.assert_called_once_with("gpmq:messages", "abandon_test", "5-0")

    def test_scan_error_swallowed(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.side_effect = RuntimeError("boom")

        subscriber._maybe_abandon_pending()  # must not raise
        adapter.xack.assert_not_called()

    def test_lazy_audit_opened_once_and_closed_on_stop(self, tmp_path):
        subscriber = _make_subscriber(
            pel_abandon_idle_seconds=60.0,
            enable_audit=True,
            audit_db_path=str(tmp_path / "audit.db"),
        )
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = True
        adapter.get_registered_subscribers.return_value = ["abandon_test"]
        adapter.xpending_range.return_value = [_entry("5-0")]

        subscriber._maybe_abandon_pending()
        assert subscriber._audit is not None
        first = subscriber._audit

        adapter.xpending_range.return_value = []  # second sweep reuses store
        subscriber._maybe_abandon_pending()
        assert subscriber._audit is first

        subscriber.stop()
        assert subscriber._audit is None
        assert first._conn is None

    def test_failed_audit_connect_is_not_cached(self, tmp_path):
        """A failed lazy connect must not be cached: a dead store would turn
        every later audit write into a silent no-op. The next entry retries."""
        subscriber = _make_subscriber(
            pel_abandon_idle_seconds=60.0,
            enable_audit=True,
            audit_db_path=str(tmp_path / "audit.db"),
        )
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]

        with patch(
            "gpmq.storage.audit_store.AuditStore.connect",
            side_effect=RuntimeError("unable to open database file"),
        ) as connect:
            subscriber._maybe_abandon_pending()  # must not raise

        connect.assert_called_once()  # the failure path really ran
        adapter.xack.assert_called_once_with("gpmq:messages", "abandon_test", "5-0")
        assert subscriber._audit is None  # broken store not cached

        adapter.xpending_range.return_value = [_entry("6-0")]
        subscriber._maybe_abandon_pending()  # real connect succeeds now

        assert subscriber._audit is not None
        rows = subscriber._audit.query_by_status(ProcessStatus.ABANDONED)
        assert [row["message_id"] for row in rows] == ["6-0"]
        subscriber._audit.close()

    def test_no_idle_entries_leaves_audit_unopened(self, tmp_path):
        """Lazy open fires only on an actual abandon: a healthy sweep with
        enable_audit=True must not open (or create) the SQLite store."""
        subscriber = _make_subscriber(
            pel_abandon_idle_seconds=60.0,
            enable_audit=True,
            audit_db_path=str(tmp_path / "audit.db"),
        )
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = []

        subscriber._maybe_abandon_pending()

        assert subscriber._audit is None
        assert not (tmp_path / "audit.db").exists()


class TestAbandonedReceiptPublish:
    """ABANDONED receipt publish: notify a waiting RPC caller, best-effort.

    The sweep runs after the owning worker died; a caller blocked in
    ResultHandler.wait() can only learn the true terminal state via a
    publish on {prefix}result:{reply_token}:{name} — same channel and
    serialization as worker-side _publish_result. Best-effort: a publish
    failure is logged but never blocks the XACK.
    """

    def test_publishes_receipt_on_reply_channel(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.return_value = [
            ("5-0", {"type": "t.x", "publisher": "p", "reply_token": "tok-1"})
        ]

        subscriber._maybe_abandon_pending()

        adapter.publish.assert_called_once()
        channel, payload = adapter.publish.call_args[0]
        assert channel == "gpmq:result:tok-1:abandon_test"
        assert payload["status"] == "abandoned"
        assert payload["message_id"] == "5-0"
        assert payload["subscriber_name"] == "abandon_test"

    def test_publish_order_audit_then_publish_then_xack(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0, enable_audit=True)
        adapter = _with_redis(subscriber)
        audit = MagicMock()
        subscriber._audit = audit  # pre-injected: skip opening SQLite
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.return_value = [("5-0", {"reply_token": "tok-1"})]

        calls: list[str] = []
        audit.save_result.side_effect = lambda *a, **k: calls.append("save")
        adapter.publish.side_effect = lambda *a, **k: calls.append("publish")
        adapter.xack.side_effect = lambda *a, **k: calls.append("xack")

        subscriber._maybe_abandon_pending()

        assert calls == ["save", "publish", "xack"]

    def test_fire_and_forget_entry_not_published(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.return_value = [("5-0", {"type": "t.x"})]  # no token

        subscriber._maybe_abandon_pending()

        adapter.publish.assert_not_called()
        adapter.xack.assert_called_once()

    def test_fields_unavailable_skips_publish(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.return_value = []  # entry already trimmed from stream

        subscriber._maybe_abandon_pending()

        adapter.publish.assert_not_called()
        adapter.xack.assert_called_once()

    def test_publish_failure_still_xacks(self):
        subscriber = _make_subscriber(pel_abandon_idle_seconds=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = [_entry("5-0")]
        adapter.xrange.return_value = [("5-0", {"reply_token": "tok-1"})]
        adapter.publish.side_effect = RuntimeError("redis down")

        subscriber._maybe_abandon_pending()  # must not raise

        adapter.xack.assert_called_once_with("gpmq:messages", "abandon_test", "5-0")


class TestHeartbeatLoopAbandon:
    @staticmethod
    def _run_one_round(subscriber: Subscriber) -> None:
        # wait() -> False runs one heartbeat round; -> True ends the loop.
        stop_event = MagicMock()
        stop_event.wait.side_effect = [False, True]
        subscriber._heartbeat_stop = stop_event  # type: ignore[assignment]
        subscriber._heartbeat_loop()

    def test_round_runs_abandon_then_trim(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = []
        self._run_one_round(subscriber)
        names = [c[0] for c in adapter.mock_calls]
        assert names.index("update_heartbeat") < names.index("xpending_range")
        assert names.index("xpending_range") < names.index(
            "trim_stream_to_registered_progress"
        )

    def test_abandon_failure_does_not_skip_trim(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        adapter.xpending_range.side_effect = RuntimeError("boom")
        self._run_one_round(subscriber)  # must not raise
        adapter.trim_stream_to_registered_progress.assert_called_once()


class TestStopSweep:
    def test_stop_sweep_precedes_final_trim(self):
        subscriber = _make_subscriber()
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = True
        adapter.get_registered_subscribers.return_value = ["abandon_test"]
        adapter.xpending_range.return_value = []
        subscriber.stop()
        names = [c[0] for c in adapter.mock_calls]
        assert names.index("xpending_range") < names.index(
            "trim_stream_to_registered_progress"
        )

    def test_stop_sweep_runs_with_trimming_disabled(self):
        """The sweep is its own mechanism — trim flags must not gate it."""
        subscriber = _make_subscriber(
            stream_trim_interval=0.0, stream_trim_on_last_stop=False
        )
        adapter = _with_redis(subscriber)
        adapter.xpending_range.return_value = []
        subscriber.stop()
        adapter.xpending_range.assert_called_once()
        adapter.trim_stream_to_registered_progress.assert_not_called()
