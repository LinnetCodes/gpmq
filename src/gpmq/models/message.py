"""Message data structure for GPMQ."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    """Core message data structure."""

    id: str = Field(..., min_length=1, description="Redis Streams generated ID")
    type: str = Field(..., min_length=1, description="Message type")
    timestamp: datetime = Field(..., description="Publish time")
    publisher: str = Field(..., min_length=1, description="Publisher name")
    payload: dict[str, Any] = Field(..., description="Message payload")
    correlation_id: Optional[str] = Field(
        default=None, description="For message chain tracking"
    )
    target_subscribers: Optional[list[str]] = Field(
        default=None, description="Optional allow-list of subscriber names"
    )
