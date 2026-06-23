"""Message publisher for GPMQ."""

import json
from typing import Optional

from gpmq.config import GPMQConfig
from gpmq.models.message import Message
from gpmq.models.result import ProcessResult
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter


class Publisher:
    """Message publisher with result collection.

    Publishes messages to Redis Streams and manages
    result collection from subscribers via Pub/Sub.

    Publish flow:
    1. Get active subscribers matching message type
    2. xadd to stream -> get message_id
    3. Subscribe to result channels
    4. Save message to audit store
    5. Return (message_id, ResultHandler)
    """

    def __init__(
        self,
        redis_adapter: RedisAdapter,
        audit_store: Optional[AuditStore],
        config: GPMQConfig,
    ) -> None:
        """Initialize publisher.

        Args:
            redis_adapter: Connected Redis adapter
            audit_store: Audit store for persistence, or None to skip auditing
            config: GPMQ configuration
        """
        self._redis = redis_adapter
        self._audit = audit_store
        self._config = config

    def publish_message(self, message: Message) -> tuple[str, ResultHandler]:
        """Publish message to Redis Stream.

        Args:
            message: Message to publish (id field will be updated with Redis ID)

        Returns:
            Tuple of (message_id, ResultHandler)
        """
        # Get active subscribers and filter by message type
        relevant_subs = self._get_matching_subscribers(message.type)

        # Publish to stream
        fields = self._message_to_fields(message)
        message_id = self._redis.xadd(self._config.stream_name, fields)

        # Update message with actual Redis-generated ID
        message.id = message_id

        # Create ResultHandler (subscribes to result channels immediately)
        handler = ResultHandler(
            message_id=message_id,
            subscriber_names=relevant_subs,
            redis_adapter=self._redis,
            key_prefix=self._config.registry_key_prefix,
            audit_store=self._audit
        )

        # Save to audit store
        if self._audit:
            self._audit.save_message(message)

        return (message_id, handler)

    def send_message(self, message: Message) -> str:
        """Fire-and-forget publish: send message without collecting results.

        Publishes message to Redis Stream and records audit log,
        but does NOT create a ResultHandler or subscribe to Pub/Sub channels.

        Args:
            message: Message to publish (id field will be updated with Redis ID)

        Returns:
            Redis-generated message ID
        """
        # Publish to stream
        fields = self._message_to_fields(message)
        message_id = self._redis.xadd(self._config.stream_name, fields)

        # Update message with actual Redis-generated ID
        message.id = message_id

        # Save to audit store
        if self._audit:
            self._audit.save_message(message)

        return message_id

    def get_active_subscribers(self) -> list[str]:
        """Get list of subscribers with recent heartbeats.

        Returns:
            List of active subscriber names
        """
        timeout = self._config.heartbeat_interval * 3
        return self._redis.get_active_subscribers(timeout=timeout)

    def wait_for_results(
        self,
        message_id: str,
        subscriber_names: list[str],
        timeout: Optional[float] = None,
    ) -> dict[str, ProcessResult]:
        """Convenience method: create handler and wait for results.

        Args:
            message_id: Message ID to wait for
            subscriber_names: Expected subscriber names
            timeout: Max seconds to wait

        Returns:
            Dict mapping subscriber_name to ProcessResult
        """
        handler = ResultHandler(
            message_id=message_id,
            subscriber_names=subscriber_names,
            redis_adapter=self._redis,
            key_prefix=self._config.registry_key_prefix,
            audit_store=self._audit
        )
        return handler.wait(timeout=timeout)

    def _get_matching_subscribers(self, message_type: str) -> list[str]:
        """Get subscribers that handle a specific message type.

        Args:
            message_type: Message type to match

        Returns:
            List of subscriber names that handle this type
        """
        active = self.get_active_subscribers()
        matching: list[str] = []
        for name in active:
            info = self._redis.get_subscriber_info(name)
            if info and message_type in info.get("message_types", []):
                matching.append(name)
        return matching

    @staticmethod
    def _message_to_fields(message: Message) -> dict[str, str]:
        """Convert Message to Redis Stream fields.

        Args:
            message: Message object

        Returns:
            Dict of field-value pairs for xadd
        """
        fields: dict[str, str] = {
            "type": message.type,
            "timestamp": message.timestamp.isoformat(),
            "publisher": message.publisher,
            "payload": json.dumps(message.payload),
        }
        if message.correlation_id:
            fields["correlation_id"] = message.correlation_id
        return fields
