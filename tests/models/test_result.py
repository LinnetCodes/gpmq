"""Tests for ProcessResult and ProcessStatus models."""

import pytest

from gpmq.models.result import ProcessResult, ProcessStatus


class TestProcessStatus:
    """Test cases for ProcessStatus enum."""

    def test_status_values(self):
        """Test that all status values exist."""
        assert ProcessStatus.SUCCESS == "success"
        assert ProcessStatus.FAILURE == "failure"
        assert ProcessStatus.EXCEPTION == "exception"
        assert ProcessStatus.TIMEOUT == "timeout"

    def test_status_is_string_enum(self):
        """Test that ProcessStatus is a string enum."""
        assert ProcessStatus.SUCCESS.value == "success"
        assert isinstance(ProcessStatus.SUCCESS.value, str)

    def test_status_comparison(self):
        """Test that status can be compared."""
        assert ProcessStatus.SUCCESS == ProcessStatus.SUCCESS
        assert ProcessStatus.SUCCESS != ProcessStatus.FAILURE


class TestProcessResult:
    """Test cases for ProcessResult model."""

    def test_result_creation_success(self):
        """Test creating a successful result."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.SUCCESS,
            subscriber_name="test_subscriber",
            processing_time=0.123,
        )

        assert result.message_id == "123-0"
        assert result.status == ProcessStatus.SUCCESS
        assert result.subscriber_name == "test_subscriber"
        assert result.processing_time == 0.123
        assert result.data is None
        assert result.error_message is None
        assert result.error_traceback is None
        assert result.timeout_seconds is None

    def test_result_creation_with_data(self):
        """Test creating a result with return data."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.SUCCESS,
            subscriber_name="test_subscriber",
            processing_time=0.5,
            data={"result": "ok", "count": 42},
        )

        assert result.data == {"result": "ok", "count": 42}

    def test_result_creation_failure(self):
        """Test creating a failure result."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.FAILURE,
            subscriber_name="test_subscriber",
            processing_time=0.1,
            error_message="Validation failed",
        )

        assert result.status == ProcessStatus.FAILURE
        assert result.error_message == "Validation failed"

    def test_result_creation_exception(self):
        """Test creating an exception result."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.EXCEPTION,
            subscriber_name="test_subscriber",
            processing_time=0.2,
            error_message="Division by zero",
            error_traceback="Traceback (most recent call last):\n  ...",
        )

        assert result.status == ProcessStatus.EXCEPTION
        assert result.error_message == "Division by zero"
        assert "Traceback" in result.error_traceback

    def test_result_creation_timeout(self):
        """Test creating a timeout result."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.TIMEOUT,
            subscriber_name="test_subscriber",
            processing_time=60.0,
            timeout_seconds=60.0,
        )

        assert result.status == ProcessStatus.TIMEOUT
        assert result.timeout_seconds == 60.0

    def test_result_validation_missing_required_fields(self):
        """Test that missing required fields raise validation error."""
        with pytest.raises(Exception):  # Pydantic ValidationError
            ProcessResult(
                # Missing message_id
                status=ProcessStatus.SUCCESS,
                subscriber_name="test",
                processing_time=0.1,
            )

        with pytest.raises(Exception):
            ProcessResult(
                message_id="123-0",
                # Missing status
                subscriber_name="test",
                processing_time=0.1,
            )

    def test_result_model_dump(self):
        """Test serializing result to dict."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.SUCCESS,
            subscriber_name="test_subscriber",
            processing_time=0.123,
            data={"key": "value"},
        )

        data = result.model_dump()

        assert data["message_id"] == "123-0"
        assert data["status"] == ProcessStatus.SUCCESS
        assert data["subscriber_name"] == "test_subscriber"
        assert data["processing_time"] == 0.123
        assert data["data"] == {"key": "value"}

    def test_result_json_serialization(self):
        """Test JSON serialization of result."""
        result = ProcessResult(
            message_id="123-0",
            status=ProcessStatus.SUCCESS,
            subscriber_name="test_subscriber",
            processing_time=0.123,
        )

        json_str = result.model_dump_json()
        assert isinstance(json_str, str)
        assert '"message_id":"123-0"' in json_str
        assert '"status":"success"' in json_str

    def test_result_from_dict(self):
        """Test creating result from dictionary."""
        data = {
            "message_id": "123-0",
            "status": "success",
            "subscriber_name": "test_subscriber",
            "processing_time": 0.123,
        }

        result = ProcessResult.model_validate(data)

        assert result.message_id == "123-0"
        assert result.status == ProcessStatus.SUCCESS
        assert result.subscriber_name == "test_subscriber"
        assert result.processing_time == 0.123

    def test_result_from_dict_with_optional_fields(self):
        """Test creating result from dict with optional fields."""
        data = {
            "message_id": "123-0",
            "status": "exception",
            "subscriber_name": "test_subscriber",
            "processing_time": 0.5,
            "error_message": "Something went wrong",
            "error_traceback": "Stack trace here",
        }

        result = ProcessResult.model_validate(data)

        assert result.status == ProcessStatus.EXCEPTION
        assert result.error_message == "Something went wrong"
        assert result.error_traceback == "Stack trace here"
