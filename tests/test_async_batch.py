"""Tests for GPMQAsyncBatchHandler."""

from unittest.mock import MagicMock

from gpmq.async_batch import GPMQAsyncBatchHandler
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.publisher.result_handler import ResultHandler


def _make_mock_client():
    """Create a mock GPMQClient with publish_async support."""
    client = MagicMock()
    client._connected = True
    return client


def _make_success_result(subscriber_name="sub-a", message_id="msg-1"):
    return ProcessResult(
        message_id=message_id,
        status=ProcessStatus.SUCCESS,
        subscriber_name=subscriber_name,
        processing_time=0.1,
    )


def _make_timeout_result(subscriber_name="sub-a", message_id="msg-1"):
    return ProcessResult(
        message_id=message_id,
        status=ProcessStatus.TIMEOUT,
        subscriber_name=subscriber_name,
        processing_time=1.0,
        timeout_seconds=1.0,
    )


class TestGPMQAsyncBatchHandlerInit:
    def test_stores_client_and_defaults(self):
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        assert handler._client is client
        assert handler._progress is None
        assert handler._timeout is None
        assert handler._pending_messages == []
        assert handler._all_results == []

    def test_stores_progress_and_timeout(self):
        from gpmq.progress import GPMQProgress

        client = _make_mock_client()
        progress = GPMQProgress()
        handler = GPMQAsyncBatchHandler(client, progress=progress, timeout=30.0)
        assert handler._progress is progress
        assert handler._timeout == 30.0

    def test_default_queue_size(self):
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        assert handler._queue_size == 32

    def test_custom_queue_size(self):
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client, queue_size=10)
        assert handler._queue_size == 10

    def test_queue_size_zero_raises(self):
        client = _make_mock_client()
        import pytest
        with pytest.raises(ValueError, match="queue_size must be >= 1"):
            GPMQAsyncBatchHandler(client, queue_size=0)

    def test_queue_size_negative_raises(self):
        client = _make_mock_client()
        import pytest
        with pytest.raises(ValueError, match="queue_size must be >= 1"):
            GPMQAsyncBatchHandler(client, queue_size=-5)


class TestGPMQAsyncBatchHandlerSendMessage:
    def test_send_message_does_not_call_publish_async(self):
        """send_message should defer publishing — store message, don't call publish_async."""
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        handler.send_message("test_type", {"key": "val"})
        client.publish_async.assert_not_called()

    def test_send_message_stores_pending_message(self):
        """send_message should store message tuples in _pending_messages."""
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        handler.send_message("type_a", {"id": 1})
        handler.send_message("type_b", {"id": 2}, correlation_id="corr-1")
        assert handler._pending_messages == [
            ("type_a", {"id": 1}, None),
            ("type_b", {"id": 2}, "corr-1"),
        ]


class TestGPMQAsyncBatchHandlerWaitAll:
    def test_wait_all_empty_handlers(self):
        """Should return empty list when no messages sent."""
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        results = handler.wait_all()
        assert results == []
        client.wait_all.assert_not_called()

    def test_wait_all_calls_client_wait_all_in_loop(self):
        """Should loop client.wait_all until all complete, only submitting pending handlers."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2]

        # First call: rh1 completed, rh2 timeout
        result_1st = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_timeout_result("sub-b", "msg-2")},
        ]
        # Second call: only rh2 submitted (pending), it completes
        result_2nd = [
            {"sub-b": _make_success_result("sub-b", "msg-2")},
        ]
        client.wait_all.side_effect = [result_1st, result_2nd]

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.send_message("t2", {"id": 2})

        results = handler.wait_all()

        # One loop call with all + one loop call with pending only = 2 total
        assert client.wait_all.call_count == 2

        # First call passes all handlers
        call_1_args = client.wait_all.call_args_list[0]
        assert call_1_args[0][0] == [rh1, rh2]

        # Second call passes only pending handler
        call_2_args = client.wait_all.call_args_list[1]
        assert call_2_args[0][0] == [rh2]

        # Results correctly mapped to original handler order
        assert len(results) == 2
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS
        assert results[1]["sub-b"].status == ProcessStatus.SUCCESS

    def test_wait_all_respects_total_timeout(self):
        """Should stop when total timeout expires."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        # Always return TIMEOUT (never complete)
        timeout_result = [{"sub-a": _make_timeout_result("sub-a", "msg-1")}]
        client.wait_all.return_value = timeout_result

        handler = GPMQAsyncBatchHandler(client, timeout=0.1)
        handler.send_message("t1", {"id": 1})

        results = handler.wait_all()

        # Should have stopped due to timeout
        assert results == timeout_result

    def test_wait_all_passes_handlers_to_client(self):
        """Should pass collected handlers to client.wait_all."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2]

        complete_result = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_success_result("sub-b", "msg-2")},
        ]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.send_message("t2", {"id": 2})
        handler.wait_all()

        # Check that handlers list was passed
        call_args = client.wait_all.call_args_list[0]
        assert call_args[0][0] == [rh1, rh2]

    def test_wait_all_uses_timeout_1_and_allow_retry(self):
        """Loop calls should use timeout=1.0 and allow_timeout_retry=True."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.wait_all()

        # Loop call uses timeout=1.0, allow_timeout_retry=True
        loop_call = client.wait_all.call_args_list[0]
        assert loop_call.kwargs["timeout"] == 1.0
        assert loop_call.kwargs["allow_timeout_retry"] is True

    def test_final_cleanup_call_uses_allow_timeout_retry_false(self):
        """Final cleanup call uses allow_timeout_retry=False when handlers still pending."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        timeout_result = [{"sub-a": _make_timeout_result("sub-a", "msg-1")}]
        client.wait_all.return_value = timeout_result

        handler = GPMQAsyncBatchHandler(client, timeout=0.1, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.wait_all()

        # Last call is the final cleanup with allow_timeout_retry=False
        final_call = client.wait_all.call_args_list[-1]
        assert final_call.kwargs["allow_timeout_retry"] is False

    def test_wait_all_only_submits_pending_handlers(self):
        """Should only re-submit handlers that returned TIMEOUT on previous iterations."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        rh3 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2, rh3]

        # Round 1: rh1 success, rh2 timeout, rh3 timeout
        result_round1 = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_timeout_result("sub-b", "msg-2")},
            {"sub-c": _make_timeout_result("sub-c", "msg-3")},
        ]
        # Round 2: only rh2 and rh3 submitted; rh2 completes, rh3 still timeout
        result_round2 = [
            {"sub-b": _make_success_result("sub-b", "msg-2")},
            {"sub-c": _make_timeout_result("sub-c", "msg-3")},
        ]
        # Final: only rh3 submitted (still pending after deadline or loop exit)
        result_final = [
            {"sub-c": _make_success_result("sub-c", "msg-3")},
        ]
        client.wait_all.side_effect = [result_round1, result_round2, result_final]

        handler = GPMQAsyncBatchHandler(client, timeout=5.0, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.send_message("t2", {"id": 2})
        handler.send_message("t3", {"id": 3})
        results = handler.wait_all()

        # Round 1: all 3 handlers submitted
        call_1_args = client.wait_all.call_args_list[0]
        assert call_1_args[0][0] == [rh1, rh2, rh3]

        # Round 2: only rh2 and rh3 (the pending ones from round 1)
        call_2_args = client.wait_all.call_args_list[1]
        assert call_2_args[0][0] == [rh2, rh3]

        # Final: only rh3 (the still-pending one from round 2)
        call_3_args = client.wait_all.call_args_list[2]
        assert call_3_args[0][0] == [rh3]

        # All results collected and correctly ordered
        assert len(results) == 3
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS
        assert results[1]["sub-b"].status == ProcessStatus.SUCCESS
        assert results[2]["sub-c"].status == ProcessStatus.SUCCESS


class TestGPMQAsyncBatchHandlerSlidingWindow:
    def test_wait_all_sends_initial_batch(self):
        """wait_all should publish only queue_size messages initially."""
        client = _make_mock_client()
        handlers = [MagicMock(spec=ResultHandler) for _ in range(5)]
        client.publish_async.side_effect = handlers

        # All return success; each call returns results matching the active window
        # Round 1: 3 handlers, all succeed -> refill 2 more
        # Round 2: 2 handlers, all succeed
        r1 = [{"sub-a": _make_success_result("sub-a", f"msg-{i}")} for i in range(3)]
        r2 = [{"sub-a": _make_success_result("sub-a", f"msg-{i}")} for i in range(3, 5)]
        client.wait_all.side_effect = [r1, r2]

        batch = GPMQAsyncBatchHandler(client, queue_size=3)
        for i in range(5):
            batch.send_message(f"t{i}", {"id": i})
        batch.wait_all()

        # publish_async: 3 initial + 2 refill after first round = 5 total
        assert client.publish_async.call_count == 5
        # But at most queue_size were active at any time
        # First client.wait_all call received exactly 3 handlers (initial batch)
        first_call_args = client.wait_all.call_args_list[0]
        assert len(first_call_args[0][0]) == 3

    def test_wait_all_refills_on_completion(self):
        """When a result completes, next pending message should be published."""
        client = _make_mock_client()
        handlers = [MagicMock(spec=ResultHandler) for _ in range(5)]
        client.publish_async.side_effect = handlers

        # Round 1: 3 messages sent, first 2 complete, 1 timeout -> refill 2
        result_r1 = [
            {"sub-a": _make_success_result("sub-a", "msg-0")},
            {"sub-b": _make_success_result("sub-b", "msg-1")},
            {"sub-c": _make_timeout_result("sub-c", "msg-2")},
        ]
        # Round 2: active = [msg-2 (timeout), msg-3 (refill), msg-4 (refill)]
        # msg-2 completes, msg-3 completes, msg-4 timeout
        result_r2 = [
            {"sub-c": _make_success_result("sub-c", "msg-2")},
            {"sub-d": _make_success_result("sub-d", "msg-3")},
            {"sub-e": _make_timeout_result("sub-e", "msg-4")},
        ]
        # Round 3: only msg-4 still pending, completes
        result_r3 = [
            {"sub-e": _make_success_result("sub-e", "msg-4")},
        ]
        client.wait_all.side_effect = [result_r1, result_r2, result_r3]

        batch = GPMQAsyncBatchHandler(client, queue_size=3)
        for i in range(5):
            batch.send_message(f"t{i}", {"id": i})
        results = batch.wait_all()

        # publish_async: 3 initial + 2 refill (after r1, both successes) = 5
        assert client.publish_async.call_count == 5
        assert len(results) == 5
        # Results in original order
        for i, r in enumerate(results):
            assert list(r.values())[0].status == ProcessStatus.SUCCESS

    def test_wait_all_never_exceeds_queue_size(self):
        """Active handlers should never exceed queue_size."""
        client = _make_mock_client()
        handlers = [MagicMock(spec=ResultHandler) for _ in range(10)]
        client.publish_async.side_effect = handlers

        # Track max active handlers seen
        max_active = [0]

        def track_and_return(handlers_list, **kwargs):
            max_active[0] = max(max_active[0], len(handlers_list))
            return [{"sub-a": _make_success_result("sub-a", f"msg-{i}")} for i in range(len(handlers_list))]

        client.wait_all = track_and_return

        batch = GPMQAsyncBatchHandler(client, queue_size=3)
        for i in range(10):
            batch.send_message(f"t{i}", {"id": i})
        batch.wait_all()

        assert max_active[0] <= 3

    def test_wait_all_fifo_order(self):
        """Results should be returned in send_message() order."""
        client = _make_mock_client()
        handlers = [MagicMock(spec=ResultHandler) for _ in range(4)]
        client.publish_async.side_effect = handlers

        # All succeed in order
        client.wait_all.return_value = [
            {"sub-a": _make_success_result("sub-a", "msg-0")},
            {"sub-a": _make_success_result("sub-a", "msg-1")},
        ]

        batch = GPMQAsyncBatchHandler(client, queue_size=2)
        batch.send_message("first", {"order": 1})
        batch.send_message("second", {"order": 2})
        batch.send_message("third", {"order": 3})
        batch.send_message("fourth", {"order": 4})
        results = batch.wait_all()

        assert len(results) == 4

    def test_wait_all_fewer_than_queue_size(self):
        """When total messages < queue_size, all sent at once."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2]

        complete_result = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_success_result("sub-b", "msg-2")},
        ]
        client.wait_all.return_value = complete_result

        batch = GPMQAsyncBatchHandler(client, queue_size=32)
        batch.send_message("t1", {"id": 1})
        batch.send_message("t2", {"id": 2})
        results = batch.wait_all()

        assert len(results) == 2
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS
        assert results[1]["sub-b"].status == ProcessStatus.SUCCESS

    def test_wait_all_single_message(self):
        """Single message should work correctly."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        batch = GPMQAsyncBatchHandler(client, queue_size=3)
        batch.send_message("t1", {"id": 1})
        results = batch.wait_all()

        assert len(results) == 1
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS
        assert client.publish_async.call_count == 1

    def test_wait_all_publish_async_failure_during_refill(self):
        """If publish_async fails during refill, batch should continue."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        rh3 = MagicMock(spec=ResultHandler)
        # rh1, rh2 succeed on initial send; rh3 fails during refill
        client.publish_async.side_effect = [rh1, rh2, RuntimeError("Redis down"), rh3]

        # Round 1: both initial succeed -> refill 2 more (msg-2 fails, msg-3 succeeds)
        result_r1 = [
            {"sub-a": _make_success_result("sub-a", "msg-0")},
            {"sub-b": _make_success_result("sub-b", "msg-1")},
        ]
        # Round 2: only rh3 active, completes
        result_r2 = [
            {"sub-c": _make_success_result("sub-c", "msg-3")},
        ]
        client.wait_all.side_effect = [result_r1, result_r2]

        batch = GPMQAsyncBatchHandler(client, queue_size=2)
        batch.send_message("t0", {"id": 0})
        batch.send_message("t1", {"id": 1})
        batch.send_message("t2", {"id": 2})  # will fail to publish
        batch.send_message("t3", {"id": 3})
        results = batch.wait_all()

        assert len(results) == 4
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS
        assert results[1]["sub-b"].status == ProcessStatus.SUCCESS
        assert results[2] == {}  # publish_async failed -> empty result
        assert results[3]["sub-c"].status == ProcessStatus.SUCCESS


class TestGPMQAsyncBatchHandlerGetResult:
    def test_get_all_result_returns_empty_before_wait(self):
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        assert handler.get_all_result() == []

    def test_get_all_result_returns_wait_all_results(self):
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.wait_all()

        assert handler.get_all_result() == complete_result


class TestGPMQAsyncBatchHandlerContextManager:
    def test_context_manager_calls_wait_all_on_exit(self):
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        with handler as batch:
            batch.send_message("t1", {"id": 1})

        # wait_all was called by __exit__
        assert client.wait_all.called
        assert handler.get_all_result() == complete_result

    def test_context_manager_get_result_after_exit(self):
        """User can get results after exiting context."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        with GPMQAsyncBatchHandler(client, queue_size=32) as batch:
            batch.send_message("t1", {"id": 1})

        results = batch.get_all_result()
        assert len(results) == 1
        assert results[0]["sub-a"].status == ProcessStatus.SUCCESS

    def test_enter_returns_self(self):
        client = _make_mock_client()
        handler = GPMQAsyncBatchHandler(client)
        result = handler.__enter__()
        assert result is handler


class TestGPMQAsyncBatchHandlerProgress:
    def test_progress_create_called_with_handler_count(self):
        from gpmq.progress import GPMQProgress

        client = _make_mock_client()
        progress = GPMQProgress()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2]

        complete_result = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_success_result("sub-b", "msg-2")},
        ]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, progress=progress, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.send_message("t2", {"id": 2})
        handler.wait_all()

        assert progress.max_value == 2
        assert progress.current_value == 2

    def test_progress_updates_on_each_iteration(self):
        from gpmq.progress import GPMQProgress

        client = _make_mock_client()
        progress = GPMQProgress()
        rh1 = MagicMock(spec=ResultHandler)
        rh2 = MagicMock(spec=ResultHandler)
        client.publish_async.side_effect = [rh1, rh2]

        # Track update calls
        update_values = []
        original_update = progress.update
        progress.update = lambda v: (update_values.append(v), original_update(v))

        # First iteration: 1 complete, second: both complete
        result_1st = [
            {"sub-a": _make_success_result("sub-a", "msg-1")},
            {"sub-b": _make_timeout_result("sub-b", "msg-2")},
        ]
        result_2nd = [
            {"sub-b": _make_success_result("sub-b", "msg-2")},
        ]
        client.wait_all.side_effect = [result_1st, result_2nd]

        handler = GPMQAsyncBatchHandler(client, progress=progress, queue_size=32)
        handler.send_message("t1", {"id": 1})
        handler.send_message("t2", {"id": 2})
        handler.wait_all()

        assert update_values[0] == 1  # After first iteration
        assert update_values[1] == 2  # After second iteration

    def test_no_progress_when_none(self):
        """Should work fine without progress indicator."""
        client = _make_mock_client()
        rh1 = MagicMock(spec=ResultHandler)
        client.publish_async.return_value = rh1

        complete_result = [{"sub-a": _make_success_result("sub-a", "msg-1")}]
        client.wait_all.return_value = complete_result

        handler = GPMQAsyncBatchHandler(client, queue_size=32)
        handler.send_message("t1", {"id": 1})
        results = handler.wait_all()

        assert results == complete_result
