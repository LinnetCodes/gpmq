"""Message publisher for GPMQ."""

import json
import uuid
from typing import Any, Optional

from gpmq.config import GPMQConfig
from gpmq.exceptions import InvalidTargetSubscriberError
from gpmq.log import get_logger
from gpmq.models.message import Message
from gpmq.publisher.result_handler import ResultHandler
from gpmq.storage.audit_store import AuditStore
from gpmq.storage.redis_adapter import RedisAdapter

# Resolved in Publisher.__init__: an import-time get_logger() would consume
# gpmq.log's once-per-process config lookup before any config manager is set.
# gpclog does not ship py.typed yet (PEP 561), so its logger type is Any.
logger: Any = None


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

        global logger
        logger = get_logger(getattr(config, "log_category", "gpmq"))

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

        # Publisher-side retention gate (registration-based): skip the xadd
        # entirely when no REGISTERED subscriber declares this message type.
        # Registered-but-offline subscribers still count, preserving the
        # crash-recovery catch-up semantics.
        if (
            not message.target_subscribers
            and self._config.publish_require_subscriber
            and not relevant_subs
            and not self._registered_subscribers_handle(message.type)
        ):
            logger.info(
                "Skipping publish of type '{}': publish_require_subscriber "
                "is enabled and no registered subscriber handles it",
                message.type,
            )
            # Empty expected-set: wait() returns {} immediately, matching the
            # existing "no subscriber" fast path.
            return (
                "",
                ResultHandler(
                    reply_token="",
                    subscriber_names=[],
                    redis_adapter=self._redis,
                    key_prefix=self._config.registry_key_prefix,
                    audit_store=self._audit,
                ),
            )

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
        message_id = self._redis.xadd(
            self._config.stream_name,
            fields,
            maxlen=self._idle_trim_maxlen(),
            approximate=False,
        )

        # Update message with actual Redis-generated ID
        message.id = message_id

        # ResultHandler needs the message_id for audit and TIMEOUT results
        handler.bind(message_id)

        # Save to audit store
        if self._audit:
            self._audit.save_message(message)

        return (message_id, handler)

    def send_message(self, message: Message) -> Optional[str]:
        """Fire-and-forget publish: send message without collecting results.

        Publishes message to Redis Stream and records audit log,
        but does NOT create a ResultHandler or subscribe to Pub/Sub channels.

        Args:
            message: Message to publish (id field will be updated with Redis ID)

        Returns:
            Redis-generated message ID, or None when the message was skipped
            (publish_require_subscriber enabled and no registered subscriber
            handles its type)
        """
        if message.target_subscribers:
            self._validate_target_subscribers(message.type, message.target_subscribers)
        elif (
            self._config.publish_require_subscriber
            and not self._registered_subscribers_handle(message.type)
        ):
            logger.info(
                "Skipping send of type '{}': publish_require_subscriber is "
                "enabled and no registered subscriber handles it",
                message.type,
            )
            return None

        # Publish to stream
        fields = self._message_to_fields(message)
        message_id = self._redis.xadd(
            self._config.stream_name,
            fields,
            maxlen=self._idle_trim_maxlen(),
            approximate=False,
        )

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

    def _registered_subscribers_handle(self, message_type: str) -> bool:
        """Check whether any registered subscriber declares *message_type*.

        Registration-based (not liveness-based): a registered-but-offline
        subscriber counts, so its crash-recovery catch-up window is
        preserved.

        Args:
            message_type: Message type to match against declared types

        Returns:
            True if at least one registered subscriber declares the type
        """
        for name in self._redis.get_registered_subscribers():
            info = self._redis.get_subscriber_info(name)
            if info and message_type in info.get("message_types", []):
                return True
        return False

    def _idle_trim_maxlen(self) -> int | None:
        """Resolve the XADD MAXLEN cap for the empty-registry window.

        Returns:
            Configured cap when the registry is empty, else None
        """
        cap = self._config.stream_trim_idle_maxlen
        if cap is None:
            return None
        if self._redis.get_registered_subscribers():
            return None
        return cap

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
