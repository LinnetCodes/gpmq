"""Integration tests for Publisher and ResultHandler with real Redis."""

import json
from datetime import datetime, timezone

import pytest

from gpmq.config import PublisherConfig
from gpmq.exceptions import InvalidTargetSubscriberError
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus
from gpmq.publisher.publisher import Publisher
from gpmq.publisher.result_handler import ResultHandler


class TestPubSubResultCollection:
    """Test actual Pub/Sub result collection flow."""

    def test_result_handler_receives_published_results(self, redis_adapter):
        """ResultHandler should receive results published to reply-token channels."""
        reply_token = "integ-token-1"
        message_id = "integ-msg-1"
        sub_name = "test-sub"

        # Create handler (subscribes to the token channels and is confirmed
        # by the server before returning)
        handler = ResultHandler(
            reply_token, [sub_name], redis_adapter, key_prefix="gpmq:"
        )
        handler.bind(message_id)

        # Simulate subscriber publishing a result — safe to do immediately:
        # the subscription is already confirmed effective.
        result = ProcessResult(
            message_id=message_id,
            status=ProcessStatus.SUCCESS,
            subscriber_name=sub_name,
            processing_time=0.05,
        )
        channel = f"gpmq:result:{reply_token}:{sub_name}"
        redis_adapter.publish(channel, result.model_dump())

        results = handler.wait(timeout=2.0)

        assert len(results) == 1
        assert results[sub_name].status == ProcessStatus.SUCCESS
        assert results[sub_name].processing_time == 0.05

    def test_publisher_flow_with_subscriber_simulation(
        self, redis_adapter, audit_store
    ):
        """Full publish flow: subscribe (token) -> xadd -> collect result."""
        config = PublisherConfig(
            publisher_name="integ_pub",
            redis_host="127.0.0.1",
            redis_port=6379,
            redis_password="123456",
            redis_db=15,
            stream_name="test:integ:stream",
            registry_key_prefix="gpmq:",
        )
        pub = Publisher(redis_adapter, audit_store, config)

        # Simulate a registered subscriber
        redis_adapter.register_subscriber(
            "integ-sub",
            {
                "message_types": ["integ_test"],
                "name": "integ-sub",
            },
        )

        # Publish message
        msg = Message(
            id="pending",
            type="integ_test",
            timestamp=datetime.now(timezone.utc),
            publisher="integ_pub",
            payload={"test": True},
        )
        message_id, handler = pub.publish_message(msg)

        # The stream entry carries the reply token the handler subscribed to
        entries = redis_adapter.xrange("test:integ:stream", "-", "+")
        _, last_fields = entries[-1]
        reply_token = last_fields["reply_token"]
        assert handler.message_id == message_id

        # Simulate subscriber processing and publishing result on the
        # token channel — the subscription was confirmed before the xadd,
        # so this cannot race even though it happens "instantly".
        result = ProcessResult(
            message_id=message_id,
            status=ProcessStatus.SUCCESS,
            subscriber_name="integ-sub",
            processing_time=0.01,
        )
        channel = f"gpmq:result:{reply_token}:integ-sub"
        redis_adapter.publish(channel, result.model_dump())

        results = handler.wait(timeout=2.0)

        assert len(results) == 1
        assert results["integ-sub"].status == ProcessStatus.SUCCESS

        # Cleanup
        try:
            redis_adapter.unregister_subscriber("integ-sub")
            redis_adapter.clear_stream("test:integ:stream")
        except Exception:
            pass


class TestTargetedPublishingIntegration:
    """Integration tests for target_subscribers end-to-end (real Redis)."""

    def test_publish_with_target_only_expects_target(self, redis_adapter, audit_store):
        """publish_message with target_subscribers should expect only the target."""
        config = PublisherConfig(
            publisher_name="integ_pub",
            redis_host="127.0.0.1",
            redis_port=6379,
            redis_password="123456",
            redis_db=15,
            stream_name="test:integ:target:stream",
            registry_key_prefix="gpmq:",
        )
        pub = Publisher(redis_adapter, audit_store, config)

        # Register two subscribers that both handle "target_test"
        for name in ("integ-target-a", "integ-target-b"):
            redis_adapter.register_subscriber(
                name,
                {
                    "message_types": ["target_test"],
                    "name": name,
                },
            )

        try:
            msg = Message(
                id="pending",
                type="target_test",
                timestamp=datetime.now(timezone.utc),
                publisher="integ_pub",
                payload={"n": 1},
                target_subscribers=["integ-target-a"],  # only A
            )
            message_id, handler = pub.publish_message(msg)

            # ResultHandler must expect ONLY integ-target-a (not B)
            assert handler.get_completed_count() == 0
            assert not handler.is_complete()  # waiting on A's result

            # Stream entry must carry the target_subscribers field and the
            # reply token the handler subscribed to
            entries = redis_adapter.xrange("test:integ:target:stream", "-", "+")
            assert entries, "stream should contain the published message"
            _, last_fields = entries[-1]
            assert json.loads(last_fields["target_subscribers"]) == ["integ-target-a"]
            reply_token = last_fields["reply_token"]

            # Simulate A returning a result on the token channel
            result = ProcessResult(
                message_id=message_id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="integ-target-a",
                processing_time=0.01,
            )
            redis_adapter.publish(
                f"gpmq:result:{reply_token}:integ-target-a", result.model_dump()
            )

            results = handler.wait(timeout=2.0)
            assert set(results.keys()) == {"integ-target-a"}
            assert results["integ-target-a"].status == ProcessStatus.SUCCESS
        finally:
            try:
                redis_adapter.unregister_subscriber("integ-target-a")
                redis_adapter.unregister_subscriber("integ-target-b")
                redis_adapter.clear_stream("test:integ:target:stream")
            except Exception:
                pass

    def test_validation_failure_prevents_xadd(self, redis_adapter, audit_store):
        """Invalid target must raise before any xadd occurs."""
        config = PublisherConfig(
            publisher_name="integ_pub",
            redis_host="127.0.0.1",
            redis_port=6379,
            redis_password="123456",
            redis_db=15,
            stream_name="test:integ:nogo:stream",
            registry_key_prefix="gpmq:",
        )
        pub = Publisher(redis_adapter, audit_store, config)

        # Register a subscriber that does NOT handle "target_test"
        redis_adapter.register_subscriber(
            "integ-wrong",
            {
                "message_types": ["other_type"],
                "name": "integ-wrong",
            },
        )

        try:
            msg = Message(
                id="pending",
                type="target_test",
                timestamp=datetime.now(timezone.utc),
                publisher="integ_pub",
                payload={},
                target_subscribers=["integ-wrong"],
            )
            with pytest.raises(InvalidTargetSubscriberError):
                pub.publish_message(msg)

            # Stream must be empty (no xadd)
            entries = redis_adapter.xrange("test:integ:nogo:stream", "-", "+")
            assert entries == []
        finally:
            try:
                redis_adapter.unregister_subscriber("integ-wrong")
                redis_adapter.clear_stream("test:integ:nogo:stream")
            except Exception:
                pass


class TestConcurrentPublishZeroLoss:
    """I14 regression: concurrent fan-out with an instant worker must not
    lose results (the quantas q05a flake shape: 24-way concurrent publish,
    sub-millisecond handler, blocked XREADGROUP worker).

    Before the fix (xadd before subscribe), 50-75% of results were published
    to channels nobody had subscribed to yet; callers then saw TIMEOUT while
    the worker had processed and ACKed everything.
    """

    def test_24_way_concurrent_publish_all_results_collected(
        self, redis_adapter
    ):
        """Every one of 24 concurrent publishes must get its SUCCESS result."""
        import threading
        from concurrent.futures import ThreadPoolExecutor

        from gpmq.client import GPMQClient
        from gpmq.models.message import Message
        from gpmq.subscriber.handler import HandlerRegistry, message_handler
        from gpmq.subscriber.worker import Worker
        from multiprocessing import Event

        HandlerRegistry.clear()

        @message_handler(message_types=["race_fanout"])
        def instant_handler(msg: Message) -> None:
            return None  # sub-millisecond: worst case for the old race

        HandlerRegistry.register(instant_handler)

        stream_name = "test:integ:race"
        group_name = "race-fanout-sub"
        subscriber_display = group_name

        config_dict = {
            "name": group_name,
            "handlers": [
                {
                    "message_types": ["race_fanout"],
                    "handler": "dummy.handler",
                }
            ],
            "stream_name": stream_name,
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "redis_password": "123456",
            "redis_db": 15,
            "heartbeat_interval": 300,
            "log_category": "gpmq",
        }
        worker = Worker(
            subscriber_name=group_name,
            stream_name=stream_name,
            message_types=["race_fanout"],
            worker_id=f"{group_name}-worker-0",
            hostname="test-host",
            config_dict=config_dict,
        )
        worker._redis = redis_adapter
        worker._audit = None

        client_config = PublisherConfig(
            publisher_name="race_pub",
            redis_host="127.0.0.1",
            redis_port=6379,
            redis_password="123456",
            redis_db=15,
            stream_name=stream_name,
            registry_key_prefix="gpmq:",
        )

        stop_event = Event()

        def _worker_loop():
            while not stop_event.is_set():
                worker._read_and_process()

        try:
            redis_adapter.xgroup_create(stream_name, group_name)
            redis_adapter.register_subscriber(
                subscriber_display,
                {"message_types": ["race_fanout"], "name": subscriber_display},
            )

            worker_thread = threading.Thread(target=_worker_loop, daemon=True)
            worker_thread.start()

            client = GPMQClient(client_config)
            client.connect()
            try:
                with ThreadPoolExecutor(max_workers=24) as executor:
                    futures = [
                        executor.submit(
                            client.publish,
                            "race_fanout",
                            {"i": i},
                            timeout=60.0,
                        )
                        for i in range(24)
                    ]
                    all_results = [f.result() for f in futures]
            finally:
                client.close()

            # Every publish must have collected exactly one SUCCESS result —
            # zero TIMEOUT despite 24-way contention and an instant handler.
            assert len(all_results) == 24
            for i, results in enumerate(all_results):
                assert len(results) == 1, f"publish {i}: expected 1 result"
                (result,) = results.values()
                assert result.status == ProcessStatus.SUCCESS, (
                    f"publish {i}: lost result (status={result.status})"
                )

            # Worker processed and ACKed everything: empty PEL.
            pending = redis_adapter.xpending(stream_name, group_name)
            assert pending["count"] == 0
        finally:
            stop_event.set()
            worker_thread.join(timeout=10)
            HandlerRegistry.clear()
            try:
                redis_adapter.unregister_subscriber(subscriber_display)
                redis_adapter.clear_stream(stream_name)
                redis_adapter.xgroup_destroy(stream_name, group_name)
            except Exception:
                pass
