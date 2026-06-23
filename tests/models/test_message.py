"""Tests for Message model."""
from datetime import datetime

import pytest

from gpmq.models.message import Message


class TestMessage:
    """Test cases for Message model."""

    def test_message_creation_minimal(self):
        """Test creating a message with required fields only."""
        msg = Message(
            id="1234567890-0",
            type="user_created",
            timestamp=datetime(2026, 3, 25, 10, 30, 0),
            publisher="test_publisher",
            payload={"user_id": 123, "name": "John"},
        )

        assert msg.id == "1234567890-0"
        assert msg.type == "user_created"
        assert msg.timestamp == datetime(2026, 3, 25, 10, 30, 0)
        assert msg.publisher == "test_publisher"
        assert msg.payload == {"user_id": 123, "name": "John"}
        assert msg.correlation_id is None

    def test_message_creation_with_correlation_id(self):
        """Test creating a message with correlation_id."""
        msg = Message(
            id="1234567890-1",
            type="user_updated",
            timestamp=datetime(2026, 3, 25, 11, 0, 0),
            publisher="test_publisher",
            payload={"user_id": 456},
            correlation_id="req-abc-123",
        )

        assert msg.correlation_id == "req-abc-123"

    def test_message_validation_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            Message(
                id="123-0",
                # Missing type
                timestamp=datetime.now(),
                publisher="test",
                payload={},
            )

        with pytest.raises(Exception):
            Message(
                # Missing id
                type="test_type",
                timestamp=datetime.now(),
                publisher="test",
                payload={},
            )

    def test_message_payload_can_be_empty_dict(self):
        """Test that payload can be an empty dict."""
        msg = Message(
            id="123-0",
            type="ping",
            timestamp=datetime.now(),
            publisher="health_check",
            payload={},
        )

        assert msg.payload == {}

    def test_message_model_dump(self):
        """Test serializing message to dict."""
        msg = Message(
            id="123-0",
            type="user_created",
            timestamp=datetime(2026, 3, 25, 10, 30, 0),
            publisher="test_publisher",
            payload={"user_id": 123},
            correlation_id="req-123",
        )

        data = msg.model_dump()

        assert data["id"] == "123-0"
        assert data["type"] == "user_created"
        assert data["publisher"] == "test_publisher"
        assert data["payload"] == {"user_id": 123}
        assert data["correlation_id"] == "req-123"

    def test_message_model_json_serialization(self):
        """Test JSON serialization of message."""
        msg = Message(
            id="123-0",
            type="user_created",
            timestamp=datetime(2026, 3, 25, 10, 30, 0),
            publisher="test_publisher",
            payload={"user_id": 123},
        )

        json_str = msg.model_dump_json()
        assert isinstance(json_str, str)
        assert '"id":"123-0"' in json_str
        assert '"type":"user_created"' in json_str

    def test_message_from_dict(self):
        """Test creating message from dictionary."""
        data = {
            "id": "123-0",
            "type": "user_created",
            "timestamp": "2026-03-25T10:30:00",
            "publisher": "test_publisher",
            "payload": {"user_id": 123},
        }

        msg = Message.model_validate(data)

        assert msg.id == "123-0"
        assert msg.type == "user_created"
        assert msg.publisher == "test_publisher"
