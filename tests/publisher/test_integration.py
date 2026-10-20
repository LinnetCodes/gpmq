"""Integration tests for Publisher and ResultHandler with real Redis."""

import json
import time
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
        """ResultHandler should receive results published to result channels."""
        message_id = "integ-msg-1"
        sub_name = "test-sub"

        # Create handler (subscribes to channel)
        handler = ResultHandler(
            message_id, [sub_name], redis_adapter, key_prefix="gpmq:"
        )

        # Simulate subscriber publishing a result
        result = ProcessResult(
            message_id=message_id,
            status=ProcessStatus.SUCCESS,
            subscriber_name=sub_name,
            processing_time=0.05,
        )
        channel = f"gpmq:result:{message_id}:{sub_name}"
        redis_adapter.publish(channel, result.model_dump())

        # Small delay for Pub/Sub propagation
        time.sleep(0.2)

        results = handler.wait(timeout=2.0)

        assert len(results) == 1
        assert results[sub_name].status == ProcessStatus.SUCCESS
        assert results[sub_name].processing_time == 0.05

    def test_publisher_flow_with_subscriber_simulation(
        self, redis_adapter, audit_store
    ):
        """Full publish flow: xadd -> subscribe -> collect result."""
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

        # Simulate subscriber processing and publishing result
        result = ProcessResult(
            message_id=message_id,
            status=ProcessStatus.SUCCESS,
            subscriber_name="integ-sub",
            processing_time=0.01,
        )
        channel = f"gpmq:result:{message_id}:integ-sub"
        redis_adapter.publish(channel, result.model_dump())

        time.sleep(0.2)

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

            # Simulate A returning a result
            result = ProcessResult(
                message_id=message_id,
                status=ProcessStatus.SUCCESS,
                subscriber_name="integ-target-a",
                processing_time=0.01,
            )
            redis_adapter.publish(
                f"gpmq:result:{message_id}:integ-target-a", result.model_dump()
            )
            time.sleep(0.2)

            results = handler.wait(timeout=2.0)
            assert set(results.keys()) == {"integ-target-a"}
            assert results["integ-target-a"].status == ProcessStatus.SUCCESS

            # Stream entry must carry the target_subscribers field
            entries = redis_adapter.xrange("test:integ:target:stream", "-", "+")
            assert entries, "stream should contain the published message"
            _, last_fields = entries[-1]
            assert json.loads(last_fields["target_subscribers"]) == ["integ-target-a"]
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
