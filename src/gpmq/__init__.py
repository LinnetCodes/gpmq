"""gpmq - GPMQ (General Purpose Message Queue) - 面向个人项目的 Python 分布式消息队列包."""

# Process policy
from gpmq.process import use_spawn

# Logging
from gpmq.log import get_logger

# Config
from gpmq.config import (
    GPMQConfig,
    HandlerConfig,
    PublisherConfig,
    SubscriberConfig,
    set_config_manager,
    get_config_manager,
    get_subscriber,
    get_client,
)

# Models
from gpmq.models import (
    Message,
    ProcessResult,
    ProcessStatus,
    WorkerState,
    WorkerStatus,
    WorkerInfo,
    ConsumerGroupWorkers,
)

# Exceptions
from gpmq.exceptions import (
    DuplicateMessageTypeError,
    GPMQError,
    HandlerTypeMismatchError,
    InvalidMessageError,
    InvalidTargetSubscriberError,
    RedisConnectionError,
)

# Storage
from gpmq.storage import AuditStore, RedisAdapter

# Publisher
from gpmq.publisher import Publisher, ResultHandler

# Client
from gpmq.client import GPMQClient

# Progress
from gpmq.progress import GPMQProgress

# Async Batch
from gpmq.async_batch import GPMQAsyncBatchHandler

# Subscriber
from gpmq.subscriber import worker_context_processor
from gpmq.subscriber.handler import HandlerRegistry, message_handler
from gpmq.subscriber.subscriber import Subscriber

__version__ = "0.7.1"

__all__ = [
    # Process policy
    "use_spawn",
    # Logging
    "get_logger",
    # Config
    "GPMQConfig",
    "HandlerConfig",
    "PublisherConfig",
    "SubscriberConfig",
    "set_config_manager",
    "get_config_manager",
    "get_subscriber",
    "get_client",
    # Models
    "Message",
    "ProcessResult",
    "ProcessStatus",
    "WorkerState",
    "WorkerStatus",
    "WorkerInfo",
    "ConsumerGroupWorkers",
    # Exceptions
    "GPMQError",
    "RedisConnectionError",
    "InvalidMessageError",
    "InvalidTargetSubscriberError",
    "DuplicateMessageTypeError",
    "HandlerTypeMismatchError",
    # Storage
    "AuditStore",
    "RedisAdapter",
    # Publisher
    "Publisher",
    "ResultHandler",
    # Client
    "GPMQClient",
    # Progress
    "GPMQProgress",
    # Async Batch
    "GPMQAsyncBatchHandler",
    # Subscriber
    "message_handler",
    "worker_context_processor",
    "HandlerRegistry",
    "Subscriber",
]
