"""Unit tests for subscriber-side stream retention trimming."""

from __future__ import annotations

from unittest.mock import MagicMock

from gpmq.config import HandlerConfig, SubscriberConfig
from gpmq.subscriber.subscriber import Subscriber


def _make_subscriber(**overrides) -> Subscriber:
    config = SubscriberConfig(
        name="trim_test",
        handlers=[HandlerConfig(message_types=["t"], handler="m.f")],
        heartbeat_interval=5,
        **overrides,
    )
    return Subscriber(config)


def _with_redis(subscriber: Subscriber) -> MagicMock:
    adapter = MagicMock()
    subscriber._redis = adapter  # type: ignore[assignment]
    return adapter


class TestMaybeTrim:
    def test_first_call_trims_immediately(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        subscriber._maybe_trim()
        adapter.trim_stream_to_registered_progress.assert_called_once()

    def test_throttles_within_interval(self):
        subscriber = _make_subscriber(stream_trim_interval=3600.0)
        adapter = _with_redis(subscriber)
        subscriber._maybe_trim()
        subscriber._maybe_trim()
        adapter.trim_stream_to_registered_progress.assert_called_once()

    def test_disabled_when_interval_zero(self):
        subscriber = _make_subscriber(stream_trim_interval=0.0)
        adapter = _with_redis(subscriber)
        subscriber._maybe_trim()
        adapter.trim_stream_to_registered_progress.assert_not_called()

    def test_trim_error_does_not_raise(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        adapter.trim_stream_to_registered_progress.side_effect = RuntimeError("boom")
        subscriber._maybe_trim()  # must not raise


class TestHeartbeatLoopTrim:
    @staticmethod
    def _run_one_round(subscriber: Subscriber) -> None:
        # wait() -> False runs one heartbeat round; -> True ends the loop.
        stop_event = MagicMock()
        stop_event.wait.side_effect = [False, True]
        subscriber._heartbeat_stop = stop_event  # type: ignore[assignment]
        subscriber._heartbeat_loop()

    def test_heartbeat_round_runs_trim(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        self._run_one_round(subscriber)
        adapter.update_heartbeat.assert_called_once_with("trim_test")
        adapter.trim_stream_to_registered_progress.assert_called_once()

    def test_heartbeat_failure_does_not_skip_trim(self):
        subscriber = _make_subscriber(stream_trim_interval=60.0)
        adapter = _with_redis(subscriber)
        adapter.update_heartbeat.side_effect = RuntimeError("down")
        self._run_one_round(subscriber)
        adapter.trim_stream_to_registered_progress.assert_called_once()


class TestStopFinalTrim:
    def test_last_registered_trims_on_stop(self):
        subscriber = _make_subscriber()
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = True
        adapter.get_registered_subscribers.return_value = ["trim_test"]
        subscriber.stop()
        adapter.trim_stream_to_registered_progress.assert_called_once()
        adapter.unregister_subscriber.assert_called_once_with("trim_test")
        # Order invariant: the final trim must run BEFORE unregistering —
        # after unregister the registry no longer contains this subscriber,
        # so a trim-then-unregister regression would silently no-op.
        names = [c[0] for c in adapter.mock_calls]
        assert names.index("trim_stream_to_registered_progress") < names.index(
            "unregister_subscriber"
        )

    def test_not_last_registered_skips_final_trim(self):
        subscriber = _make_subscriber()
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = True
        adapter.get_registered_subscribers.return_value = ["trim_test", "other"]
        subscriber.stop()
        adapter.trim_stream_to_registered_progress.assert_not_called()

    def test_on_last_stop_false_skips_final_trim(self):
        subscriber = _make_subscriber(stream_trim_on_last_stop=False)
        adapter = _with_redis(subscriber)
        adapter.get_registered_subscribers.return_value = ["trim_test"]
        subscriber.stop()
        adapter.trim_stream_to_registered_progress.assert_not_called()
        adapter.ping.assert_not_called()  # config flags are checked first

    def test_trim_disabled_skips_final_trim(self):
        subscriber = _make_subscriber(stream_trim_interval=0.0)
        adapter = _with_redis(subscriber)
        adapter.get_registered_subscribers.return_value = ["trim_test"]
        subscriber.stop()
        adapter.trim_stream_to_registered_progress.assert_not_called()
        adapter.ping.assert_not_called()  # config flags are checked first

    def test_unreachable_redis_skips_final_trim(self):
        subscriber = _make_subscriber()
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = False
        adapter.get_registered_subscribers.return_value = ["trim_test"]
        subscriber.stop()
        # No retry-wrapped registry read and no trim; unregister still runs.
        adapter.get_registered_subscribers.assert_not_called()
        adapter.trim_stream_to_registered_progress.assert_not_called()
        adapter.unregister_subscriber.assert_called_once_with("trim_test")

    def test_final_trim_error_still_unregisters(self):
        subscriber = _make_subscriber()
        adapter = _with_redis(subscriber)
        adapter.ping.return_value = True
        adapter.get_registered_subscribers.return_value = ["trim_test"]
        adapter.trim_stream_to_registered_progress.side_effect = RuntimeError("boom")
        subscriber.stop()  # must not raise
        adapter.trim_stream_to_registered_progress.assert_called_once()
        adapter.unregister_subscriber.assert_called_once_with("trim_test")
        adapter.disconnect.assert_called_once()
