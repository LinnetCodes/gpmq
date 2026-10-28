"""WorkerStatus and WorkerState models for GPMQ."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class WorkerState(str, Enum):
    """Worker process state."""

    RUNNING = "running"  # Alive and heartbeat fresh (able to process messages)
    BUSY = "busy"  # Reserved; heartbeat-cadence tracking cannot observe in-flight state
    STOPPED = "stopped"  # Process exited
    ERROR = "error"  # Alive but heartbeat expired (zombie), or crashed


class WorkerStatus(BaseModel):
    """Worker process status.

    Live fields (``last_message_id`` / ``messages_processed`` /
    ``last_activity``) are maintained in-process by the worker and flushed
    to Redis with the worker heartbeat (typically every ~5s; at worst
    every ~9s — 4s throttle + 5s read block), so values read from
    ``WorkerManager.get_worker_status()`` can be a few seconds stale, and
    the most recent counter increments are lost if the worker is killed.
    """

    worker_id: str = Field(..., min_length=1, description="Worker ID")
    process_id: int = Field(..., description="Process ID")
    status: WorkerState = Field(..., description="Worker state")
    last_message_id: Optional[str] = Field(
        default=None,
        description=(
            "ID of the last message this worker brought to a terminal "
            "outcome (handler result, handler exception, or poison "
            "disposition). Filtered/skipped messages are not recorded. "
            "A message currently stuck mid-processing is NOT visible here "
            "(heartbeat stalls during slow handlers); find it in the "
            "consumer's PEL via XPENDING."
        ),
    )
    messages_processed: int = Field(
        default=0,
        ge=0,
        description=(
            "Messages this worker brought to a terminal outcome (handler "
            "outcomes plus poison dispositions); flushed on the heartbeat "
            "cadence, so it can lag the true count by a few seconds."
        ),
    )
    last_activity: datetime = Field(
        ...,
        description=(
            "When this worker last completed a message (its registration "
            "time if it has not processed any yet; the query time only as "
            "a degraded fallback when no registry data is available)."
        ),
    )
