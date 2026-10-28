"""Tests for WorkerStatus and WorkerState models."""

from datetime import datetime

import pytest

from gpmq.models.status import WorkerState, WorkerStatus


class TestWorkerState:
    """Test cases for WorkerState enum."""

    def test_state_values(self):
        """Test that all state values exist."""
        assert WorkerState.RUNNING == "running"
        assert WorkerState.BUSY == "busy"
        assert WorkerState.STOPPED == "stopped"
        assert WorkerState.ERROR == "error"

    def test_state_is_string_enum(self):
        """Test that WorkerState is a string enum."""
        assert WorkerState.RUNNING.value == "running"
        assert isinstance(WorkerState.RUNNING.value, str)

    def test_state_comparison(self):
        """Test that states can be compared."""
        assert WorkerState.RUNNING == WorkerState.RUNNING
        assert WorkerState.RUNNING != WorkerState.BUSY


class TestWorkerStatus:
    """Test cases for WorkerStatus model."""

    def test_status_creation_minimal(self):
        """Test creating a worker status with required fields only."""
        status = WorkerStatus(
            worker_id="worker-1",
            process_id=12345,
            status=WorkerState.RUNNING,
            last_activity=datetime(2026, 3, 25, 10, 30, 0),
        )

        assert status.worker_id == "worker-1"
        assert status.process_id == 12345
        assert status.status == WorkerState.RUNNING
        assert status.last_activity == datetime(2026, 3, 25, 10, 30, 0)
        assert status.last_message_id is None
        assert status.messages_processed == 0

    def test_status_creation_busy(self):
        """Test creating a busy worker status."""
        status = WorkerStatus(
            worker_id="worker-2",
            process_id=12346,
            status=WorkerState.BUSY,
            last_message_id="1234567890-0",
            messages_processed=42,
            last_activity=datetime(2026, 3, 25, 11, 0, 0),
        )

        assert status.status == WorkerState.BUSY
        assert status.last_message_id == "1234567890-0"
        assert status.messages_processed == 42

    def test_status_creation_stopped(self):
        """Test creating a stopped worker status."""
        status = WorkerStatus(
            worker_id="worker-3",
            process_id=0,
            status=WorkerState.STOPPED,
            messages_processed=100,
            last_activity=datetime(2026, 3, 25, 12, 0, 0),
        )

        assert status.status == WorkerState.STOPPED
        assert status.process_id == 0
        assert status.messages_processed == 100

    def test_status_creation_error(self):
        """Test creating an error worker status."""
        status = WorkerStatus(
            worker_id="worker-4",
            process_id=0,
            status=WorkerState.ERROR,
            messages_processed=5,
            last_activity=datetime(2026, 3, 25, 12, 30, 0),
        )

        assert status.status == WorkerState.ERROR

    def test_status_validation_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            WorkerStatus(
                # Missing worker_id
                process_id=12345,
                status=WorkerState.RUNNING,
                last_activity=datetime.now(),
            )

        with pytest.raises(Exception):
            WorkerStatus(
                worker_id="worker-1",
                # Missing status
                process_id=12345,
                last_activity=datetime.now(),
            )

    def test_status_model_dump(self):
        """Test serializing status to dict."""
        status = WorkerStatus(
            worker_id="worker-1",
            process_id=12345,
            status=WorkerState.BUSY,
            last_message_id="123-0",
            messages_processed=10,
            last_activity=datetime(2026, 3, 25, 10, 30, 0),
        )

        data = status.model_dump()

        assert data["worker_id"] == "worker-1"
        assert data["process_id"] == 12345
        assert data["status"] == WorkerState.BUSY
        assert data["last_message_id"] == "123-0"
        assert data["messages_processed"] == 10

    def test_status_json_serialization(self):
        """Test JSON serialization of status."""
        status = WorkerStatus(
            worker_id="worker-1",
            process_id=12345,
            status=WorkerState.RUNNING,
            last_activity=datetime(2026, 3, 25, 10, 30, 0),
        )

        json_str = status.model_dump_json()
        assert isinstance(json_str, str)
        assert '"worker_id":"worker-1"' in json_str
        assert '"status":"running"' in json_str

    def test_status_from_dict(self):
        """Test creating status from dictionary."""
        data = {
            "worker_id": "worker-1",
            "process_id": 12345,
            "status": "running",
            "last_activity": "2026-03-25T10:30:00",
        }

        status = WorkerStatus.model_validate(data)

        assert status.worker_id == "worker-1"
        assert status.process_id == 12345
        assert status.status == WorkerState.RUNNING

    def test_status_from_dict_with_optional_fields(self):
        """Test creating status from dict with optional fields."""
        data = {
            "worker_id": "worker-1",
            "process_id": 12345,
            "status": "busy",
            "last_message_id": "123-0",
            "messages_processed": 10,
            "last_activity": "2026-03-25T10:30:00",
        }

        status = WorkerStatus.model_validate(data)

        assert status.status == WorkerState.BUSY
        assert status.last_message_id == "123-0"
        assert status.messages_processed == 10
