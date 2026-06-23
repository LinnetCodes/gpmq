"""GPMQ subscriber layer.

Provides message handler declaration, subscriber lifecycle management,
and multi-process worker message consumption.
"""

from gpmq.subscriber.decorators import run_forever_method, worker_context_processor
from gpmq.subscriber.handler import HandlerRegistry, message_handler
from gpmq.subscriber.subscriber import Subscriber

__all__ = [
    "HandlerRegistry",
    "Subscriber",
    "message_handler",
    "run_forever_method",
    "worker_context_processor",
]
