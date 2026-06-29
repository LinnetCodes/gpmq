"""Tests for ResultHandler."""

from unittest.mock import MagicMock, patch


from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.publisher.result_handler import ResultHandler


def _make_result(message_id: str, subscriber_name: str, status: ProcessStatus = ProcessStatus.SUCCESS) -> dict:
    """Helper to create a result dict as it would arrive from Pub/Sub."""
    return ProcessResult(
        message_id=message_id,
        status=status,
        subscriber_name=subscriber_name,
        processing_time=0.1,
    ).model_dump()


class TestResultHandlerInit:
    """Tests for ResultHandler constructor."""

    def test_subscribes_to_result_channels(self):
        """Should subscribe to gpmq:result:{msg_id}:{sub} for each subscriber."""
        redis = MagicMock()
        ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")

        redis.subscribe.assert_called_once()
        channels = redis.subscribe.call_args[0][0]
        assert "gpmq:result:msg-1:sub-a" in channels
        assert "gpmq:result:msg-1:sub-b" in channels

    def test_no_subscribers_does_not_subscribe(self):
        """Should not subscribe when subscriber list is empty."""
        redis = MagicMock()
        ResultHandler("msg-1", [], redis, key_prefix="gpmq:")
        redis.subscribe.assert_not_called()

    def test_custom_key_prefix(self):
        """Should use custom key prefix for channel names."""
        redis = MagicMock()
        ResultHandler("msg-1", ["sub-a"], redis, key_prefix="custom:")
        channels = redis.subscribe.call_args[0][0]
        assert "custom:result:msg-1:sub-a" in channels

    def test_message_id_property(self):
        """Should expose message_id as property."""
        redis = MagicMock()
        handler = ResultHandler("msg-1", [], redis, key_prefix="gpmq:")
        assert handler.message_id == "msg-1"


class TestResultHandlerWait:
    """Tests for wait() method."""

    def test_all_results_received(self):
        """Should return all results when all subscribers respond."""
        redis = MagicMock()
        redis.get_message.side_effect = [
            {"data": _make_result("msg-1", "sub-a")},
            {"data": _make_result("msg-1", "sub-b")},
            None,
        ]

        handler = ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")
        results = handler.wait(timeout=5.0)

        assert len(results) == 2
        assert results["sub-a"].status == ProcessStatus.SUCCESS
        assert results["sub-b"].status == ProcessStatus.SUCCESS

    def test_partial_timeout(self):
        """Should return TIMEOUT for subscribers that don't respond."""
        redis = MagicMock()
        redis.get_message.side_effect = [
            {"data": _make_result("msg-1", "sub-a")},
            None,
            None,
        ]

        # __init__: 0.0 (start_time)
        # wait(): 0.01 (deadline calc = 0.01 + 5.0 = 5.01)
        # loop iter 1: 0.02 (remaining = 5.01 - 0.02 > 0, calls get_message -> result for sub-a)
        # loop iter 2: 10.0 (remaining = 5.01 - 10.0 < 0, break)
        # elapsed: 10.0
        monotonic_values = iter([0.0, 0.01, 0.02, 10.0, 10.0])
        with patch("gpmq.publisher.result_handler.time.monotonic", side_effect=monotonic_values):
            handler = ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")
            results = handler.wait(timeout=5.0)

        assert len(results) == 2
        assert results["sub-a"].status == ProcessStatus.SUCCESS
        assert results["sub-b"].status == ProcessStatus.TIMEOUT
        assert results["sub-b"].timeout_seconds == 5.0

    def test_all_timeout(self):
        """Should return TIMEOUT for all subscribers when none respond."""
        redis = MagicMock()
        redis.get_message.return_value = None

        # __init__: 1 call (start_time), wait(): 1 (deadline),
        # loop: 1 (remaining check), elapsed: 1 (after loop)
        monotonic_values = iter([0.0, 0.01, 10.0, 10.0])
        with patch("gpmq.publisher.result_handler.time.monotonic", side_effect=monotonic_values):
            handler = ResultHandler("msg-1", ["sub-a"], redis, key_prefix="gpmq:")
            results = handler.wait(timeout=5.0)

        assert len(results) == 1
        assert results["sub-a"].status == ProcessStatus.TIMEOUT

    def test_no_subscribers_returns_empty(self):
        """Should return empty dict when no subscribers expected."""
        redis = MagicMock()
        handler = ResultHandler("msg-1", [], redis, key_prefix="gpmq:")
        results = handler.wait(timeout=5.0)
        assert results == {}

    def test_unsubscribes_after_wait(self):
        """Should unsubscribe from Pub/Sub after wait completes."""
        redis = MagicMock()
        redis.get_message.return_value = None

        # __init__: 1 call (start_time), wait(): 1 (deadline),
        # loop: 1 (remaining check), elapsed: 1 (after loop)
        monotonic_values = iter([0.0, 0.01, 10.0, 10.0])
        with patch("gpmq.publisher.result_handler.time.monotonic", side_effect=monotonic_values):
            handler = ResultHandler("msg-1", ["sub-a"], redis, key_prefix="gpmq:")
            handler.wait(timeout=5.0)

        redis.unsubscribe.assert_called_once()

    def test_timeout_none_waits_until_all_received(self):
        """With timeout=None, should keep polling until all results arrive."""
        redis = MagicMock()
        redis.get_message.side_effect = [
            {"data": _make_result("msg-1", "sub-a")},
            None,  # Keep polling
            {"data": _make_result("msg-1", "sub-b")},
        ]

        handler = ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")
        # Use a very short timeout to avoid hanging in test
        results = handler.wait(timeout=2.0)

        assert len(results) == 2
        assert results["sub-a"].status == ProcessStatus.SUCCESS
        assert results["sub-b"].status == ProcessStatus.SUCCESS


class TestResultHandlerWaitAny:
    """Tests for wait_any() method."""

    def test_returns_on_first_result(self):
        """Should return as soon as one result arrives."""
        redis = MagicMock()
        redis.get_message.return_value = {"data": _make_result("msg-1", "sub-a")}

        handler = ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")
        results = handler.wait_any(timeout=5.0)

        assert len(results) == 1
        assert "sub-a" in results

    def test_timeout_returns_empty(self):
        """Should return empty dict if no result arrives before timeout."""
        redis = MagicMock()
        redis.get_message.return_value = None

        # __init__: 1 call (start_time), wait_any(): 1 (deadline),
        # loop: 1 (remaining check)
        monotonic_values = iter([0.0, 0.01, 10.0])
        with patch("gpmq.publisher.result_handler.time.monotonic", side_effect=monotonic_values):
            handler = ResultHandler("msg-1", ["sub-a"], redis, key_prefix="gpmq:")
            results = handler.wait_any(timeout=5.0)

        assert results == {}

    def test_no_subscribers_returns_empty(self):
        """Should return empty dict when no subscribers expected."""
        redis = MagicMock()
        handler = ResultHandler("msg-1", [], redis, key_prefix="gpmq:")
        results = handler.wait_any(timeout=5.0)
        assert results == {}


class TestResultHandlerStatus:
    """Tests for is_complete(), get_completed_count(), get_results()."""

    def test_initial_state(self):
        """Should show incomplete with zero results initially."""
        redis = MagicMock()
        redis.get_message.return_value = None

        handler = ResultHandler("msg-1", ["sub-a", "sub-b"], redis, key_prefix="gpmq:")
        assert not handler.is_complete()
        assert handler.get_completed_count() == 0
        assert handler.get_results() == {}
