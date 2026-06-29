"""Worker info models for consumer group visibility."""

from pydantic import BaseModel, Field, computed_field


class WorkerInfo(BaseModel):
    """Information about a single worker in a consumer group."""

    worker_id: str = Field(min_length=1, description="Unique worker identifier")
    hostname: str = Field(min_length=1, description="Host machine name")
    pid: int = Field(ge=0, description="Process ID")
    status: str = Field(default="running", description="Worker status: running or stale")
    started_at: float = Field(description="Unix timestamp when worker started")
    last_heartbeat: float = Field(description="Unix timestamp of last heartbeat")


class ConsumerGroupWorkers(BaseModel):
    """Workers registered under a consumer group."""

    subscriber_name: str = Field(min_length=1, description="Subscriber / consumer group name")
    workers: list[WorkerInfo] = Field(default_factory=list, description="List of workers")

    @computed_field
    @property
    def worker_count(self) -> int:
        """Number of workers (dynamically reflects len(workers))."""
        return len(self.workers)
