"""GPMQ exception classes.

This module defines a custom exception hierarchy for GPMQ to enable
quick identification of errors by their type and message content.
"""


class GPMQError(Exception):
    """Base exception for all GPMQ errors.

    All GPMQ-specific exceptions inherit from this class.
    """

    pass


class RedisConnectionError(GPMQError):
    """Raised when connection to Redis fails.

    Attributes:
        host: Redis server hostname
        port: Redis server port
        attempts: Number of connection attempts made
    """

    def __init__(
        self,
        host: str,
        port: int,
        attempts: int,
        message: str = "Connection refused",
    ):
        self.host = host
        self.port = port
        self.attempts = attempts
        super().__init__(
            f"Redis connection failed to {host}:{port} after {attempts} attempts: {message}"
        )


class InvalidMessageError(GPMQError):
    """Raised when a message is invalid or malformed.

    Attributes:
        message_type: Type of the invalid message
        reason: Description of why the message is invalid
    """

    def __init__(self, message_type: str, reason: str):
        self.message_type = message_type
        self.reason = reason
        super().__init__(f"Invalid message (type={message_type}): {reason}")


class DuplicateMessageTypeError(GPMQError):
    """Raised when a subscriber attempts to register duplicate message types.

    Attributes:
        subscriber_name: Name of the subscriber
        message_type: The duplicate message type
    """

    def __init__(self, subscriber_name: str, message_type: str):
        self.subscriber_name = subscriber_name
        self.message_type = message_type
        super().__init__(
            f"Subscriber '{subscriber_name}' already has handler for message type '{message_type}'"
        )


class HandlerTypeMismatchError(GPMQError):
    """Raised when handler decorator types don't match configuration types.

    This error occurs when the message types declared in the @handler decorator
    do not match the types specified in the handler configuration file.

    Attributes:
        handler_path: Import path to the handler function
        declared_types: Message types declared in the decorator
        config_types: Message types from the configuration
        conflict_types: Types in config but not in decorator declaration
    """

    def __init__(
        self,
        handler_path: str,
        declared_types: list[str],
        config_types: list[str],
        conflict_types: list[str],
    ):
        self.handler_path = handler_path
        self.declared_types = declared_types
        self.config_types = config_types
        self.conflict_types = conflict_types
        super().__init__(
            f"Handler '{handler_path}' config types {config_types} "
            f"is not a subset of decorator declaration {declared_types}. "
            f"Conflicting types: {conflict_types}"
        )
