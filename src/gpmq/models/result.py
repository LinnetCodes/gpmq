"""ProcessResult and ProcessStatus models for GPMQ."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class ProcessStatus(str, Enum):
    """Message processing status."""

    SUCCESS = "success"  # Normal completion
    FAILURE = "failure"  # Business logic returned failure (not exception)
    EXCEPTION = "exception"  # Exception occurred during processing
    TIMEOUT = "timeout"  # Processing timed out, worker killed


class ProcessResult(BaseModel):
    """Result of message processing."""

    message_id: str = Field(..., min_length=1, description="Message ID")
    status: ProcessStatus = Field(..., description="Processing status")
    subscriber_name: str = Field(..., min_length=1, description="Subscriber name")
    processing_time: float = Field(..., ge=0, description="Processing time in seconds")

    data: Optional[dict[str, Any]] = Field(
        default=None, description="Return data (optional)"
    )
    error_message: Optional[str] = Field(default=None, description="Error message")
    error_traceback: Optional[str] = Field(
        default=None, description="Traceback for EXCEPTION status"
    )
    timeout_seconds: Optional[float] = Field(
        default=None, description="Timeout value for TIMEOUT status"
    )
