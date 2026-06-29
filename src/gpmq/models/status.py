"""WorkerStatus and WorkerState models for GPMQ."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class WorkerState(str, Enum):
    """Worker process state."""

    RUNNING = "running"  # Alive and heartbeat fresh (able to process messages)
    BUSY = "busy"  # Processing a message (reserved for R-001 live status tracking)
    STOPPED = "stopped"  # Process exited
    ERROR = "error"  # Alive but heartbeat expired (zombie), or crashed


class WorkerStatus(BaseModel):
    """Worker process status."""

    worker_id: str = Field(..., min_length=1, description="Worker ID")
    process_id: int = Field(..., description="Process ID")
    status: WorkerState = Field(..., description="Worker state")
    current_message_id: Optional[str] = Field(
        default=None,
        description="Currently processing message ID (reserved; populated by R-001 live tracking)",
    )
    messages_processed: int = Field(
        default=0, ge=0,
        description="Number of messages processed (reserved; populated by R-001 live tracking)",
    )
    last_activity: datetime = Field(..., description="Last activity timestamp")
