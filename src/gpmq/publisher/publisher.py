"""Message publisher for GPMQ."""

import json
import uuid
from typing import Optional

from gpmq.config import GPMQConfig
from gpmq.exceptions import InvalidTargetSubscriberError
from gpmq.models.message import Message
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter


class Publisher:
    """Message publisher with result collection.

    Publishes messages to Redis Streams and manages
    result collection from subscribers via Pub/Sub.

    Publish flow:
    1. Get active subscribers matching message type
    2. Generate a reply token and subscribe to the token's result
       channels — the subscription is confirmed by Redis BEFORE the
       message becomes visible to workers, so no result can be lost
       to the subscribe/publish race
    3. xadd to stream (envelope carries the reply token) -> get message_id
    4. Bind the message_id into the ResultHandler
    5. Save message to audit store
    6. Return (message_id, ResultHandler)
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

        The result subscription (keyed by a caller-side ``reply_token``)
        is opened and confirmed BEFORE the xadd, so a worker can never
        publish a result to a channel nobody is listening on.

        Args:
            message: Message to publish (id field will be updated with Redis ID)

        Returns:
            Tuple of (message_id, ResultHandler)
        """
        # Resolve expected subscribers: explicit target list takes precedence
        # over the legacy "all active matching subscribers" path.
        if message.target_subscribers:
            self._validate_target_subscribers(message.type, message.target_subscribers)
            relevant_subs = list(message.target_subscribers)
        else:
            relevant_subs = self._get_matching_subscribers(message.type)

        # Subscribe to the result channels BEFORE the message exists for
        # workers: the token is generated here and travels in the envelope,
        # so the channel identity is known ahead of the xadd.
        reply_token = uuid.uuid4().hex
        handler = ResultHandler(
            reply_token=reply_token,
            subscriber_names=relevant_subs,
            redis_adapter=self._redis,
            key_prefix=self._config.registry_key_prefix,
            audit_store=self._audit,
        )

        # Publish to stream
        message.reply_token = reply_token
        fields = self._message_to_fields(message)
        message_id = self._redis.xadd(self._config.stream_name, fields)

        # Update message with actual Redis-generated ID
        message.id = message_id

        # ResultHandler needs the message_id for audit and TIMEOUT results
        handler.bind(message_id)

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
        if message.target_subscribers:
            self._validate_target_subscribers(message.type, message.target_subscribers)

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

    def _validate_target_subscribers(
        self, message_type: str, targets: list[str]
    ) -> None:
        """Verify every target subscriber can handle *message_type*.

        Checks registration + message-type capability only, NOT liveness.
        Collects all failures and raises once with the full list.

        Args:
            message_type: Message type to validate against.
            targets: Subscriber names to check.

        Raises:
            InvalidTargetSubscriberError: if any target is unknown or does not
                handle *message_type*.
        """
        failures: list[tuple[str, str]] = []
        for name in targets:
            info = self._redis.get_subscriber_info(name)
            if info is None:
                failures.append((name, "not registered"))
            elif message_type not in info.get("message_types", []):
                failures.append((name, "does not handle message type"))
        if failures:
            raise InvalidTargetSubscriberError(message_type, failures)

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
        if message.reply_token:
            fields["reply_token"] = message.reply_token
        if message.target_subscribers is not None:
            fields["target_subscribers"] = json.dumps(message.target_subscribers)
        return fields
