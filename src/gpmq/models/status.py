"""WorkerStatus and WorkerState models for GPMQ."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class WorkerState(str, Enum):
    """Worker process state."""

    IDLE = "idle"  # Waiting for messages
    BUSY = "busy"  # Processing a message
    STOPPED = "stopped"  # Process exited normally
    ERROR = "error"  # Process crashed


class WorkerStatus(BaseModel):
    """Worker process status."""

    worker_id: str = Field(..., min_length=1, description="Worker ID")
    process_id: int = Field(..., description="Process ID")
    status: WorkerState = Field(..., description="Worker state")
    current_message_id: Optional[str] = Field(
        default=None, description="Currently processing message ID"
    )
    messages_processed: int = Field(
        default=0, ge=0, description="Number of messages processed"
    )
    last_activity: datetime = Field(..., description="Last activity timestamp")
