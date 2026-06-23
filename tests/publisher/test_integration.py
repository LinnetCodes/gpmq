"""Integration tests for Publisher and ResultHandler with real Redis."""

import time
from datetime import datetime, timezone


from gpmq.config import PublisherConfig
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
        handler = ResultHandler(message_id, [sub_name], redis_adapter, key_prefix="gpmq:")

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

    def test_publisher_flow_with_subscriber_simulation(self, redis_adapter, audit_store):
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
        redis_adapter.register_subscriber("integ-sub", {
            "message_types": ["integ_test"],
            "name": "integ-sub",
        })

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
