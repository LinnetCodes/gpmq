"""Redis adapter with retry logic for GPMQ operations."""

import json
import time
from typing import Any, Callable, TypeVar

import redis

from gpmq.config import GPMQConfig
from gpmq.exceptions import RedisConnectionError

T = TypeVar("T")


class RedisAdapter:
    """Redis adapter with retry logic for GPMQ operations.

    All Redis operations are wrapped with exponential backoff retry
    for transient errors (ConnectionError, TimeoutError).
    """

    def __init__(self, config: GPMQConfig) -> None:
        """Initialize adapter with connection pool from config."""
        self._config = config
        self._client: redis.Redis | None = None

    def _retry_on_transient_errors(
        self,
        func: Callable[..., T],
        *args: Any,
        max_retries: int | None = None,
        base_delay: float = 1.0,
        **kwargs: Any,
    ) -> T:
        """Execute function with exponential backoff retry.

        Retries on:
        - redis.ConnectionError
        - redis.TimeoutError

        Delay calculation:
            delay = min(base_delay * (2 ** attempt), 30.0)

        Args:
            func: Function to execute
            *args: Positional arguments
            max_retries: Maximum retry attempts (None = use config value)
            base_delay: Base delay in seconds
            **kwargs: Keyword arguments

        Returns:
            Function return value

        Raises:
            RedisConnectionError: If all retries exhausted
        """
        if max_retries is None:
            max_retries = self._config.redis_max_retries

        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except (redis.ConnectionError, redis.TimeoutError) as e:
                last_exception = e
                if attempt < max_retries:
                    delay = min(base_delay * (2**attempt), 30.0)
                    time.sleep(delay)
                else:
                    break

        raise RedisConnectionError(
            host=self._config.redis_host,
            port=self._config.redis_port,
            attempts=max_retries + 1,
            message=str(last_exception),
        ) from last_exception

    def connect(self) -> None:
        """Establish Redis connection with retries.

        Raises:
            RedisConnectionError: If connection fails after max retries.
        """

        def _connect() -> None:
            self._client = redis.Redis(
                host=self._config.redis_host,
                port=self._config.redis_port,
                db=self._config.redis_db,
                password=self._config.redis_password
                if self._config.redis_password
                else None,
                decode_responses=True,
            )
            self._client.ping()  # Verify connection

        self._retry_on_transient_errors(
            _connect, base_delay=self._config.redis_retry_delay
        )

    def disconnect(self) -> None:
        """Close Redis connection pool."""
        if self._client:
            self._client.close()
            self._client = None

    def ping(self) -> bool:
        """Check if Redis is alive.

        Returns:
            True if Redis responds to PING, False otherwise.
        """
        if not self._client:
            return False
        try:
            return self._client.ping() is True
        except redis.RedisError:
            return False

    def xadd(self, stream: str, fields: dict[str, Any]) -> str:
        """Add entry to stream.

        Args:
            stream: Stream name
            fields: Field-value pairs for the entry

        Returns:
            Redis-generated message ID (e.g., "1678901234567-0")

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xadd() -> str:
            return self._client.xadd(stream, fields)  # type: ignore[return-value]

        return self._retry_on_transient_errors(_xadd)

    def xrange(
        self,
        stream: str,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict]]:
        """Read range of entries from stream.

        Args:
            stream: Stream name
            start: Start ID ("-" = beginning, specific ID = inclusive)
            end: End ID ("+" = end, specific ID = inclusive)
            count: Maximum entries to return

        Returns:
            List of [(message_id, fields)]

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xrange() -> list[tuple[str, dict]]:
            return self._client.xrange(stream, start, end, count=count)  # type: ignore[return-value]

        return self._retry_on_transient_errors(_xrange)

    def clear_stream(self, stream: str) -> None:
        """Delete and recreate stream.

        Args:
            stream: Stream name

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _clear() -> None:
            self._client.delete(stream)

        self._retry_on_transient_errors(_clear)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int = 1,
        block_ms: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict]]]]:
        """Read from consumer group.

        Args:
            group: Consumer group name
            consumer: Consumer name
            streams: Dict of {stream_name: start_id}
            count: Maximum entries per stream
            block_ms: Block timeout in milliseconds (None = non-blocking)

        Returns:
            List of [(stream, [(message_id, fields)])]
            Empty list if no messages available (and non-blocking or timeout)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xreadgroup() -> list[tuple[str, list[tuple[str, dict]]]]:
            result = self._client.xreadgroup(
                group, consumer, streams, count=count, block=block_ms
            )
            # Convert to list format
            return [(stream, entries) for stream, entries in result]

        return self._retry_on_transient_errors(_xreadgroup)

    def xack(self, stream: str, group: str, message_id: str) -> int:
        """Acknowledge message processing.

        Args:
            stream: Stream name
            group: Consumer group name
            message_id: Message ID to acknowledge

        Returns:
            Number of messages acknowledged (0 or 1)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xack() -> int:
            return self._client.xack(stream, group, message_id)

        return self._retry_on_transient_errors(_xack)

    def xgroup_create(self, stream: str, group: str, id: str = "0") -> None:
        """Create consumer group.

        Args:
            stream: Stream name
            group: Consumer group name
            id: Starting ID for the group ("0" = from beginning)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xgroup_create() -> None:
            try:
                self._client.xgroup_create(stream, group, id=id, mkstream=True)
            except redis.ResponseError as e:
                # Group already exists is OK
                if "BUSYGROUP" not in str(e):
                    raise

        self._retry_on_transient_errors(_xgroup_create)

    def xgroup_destroy(self, stream: str, group: str) -> int:
        """Destroy consumer group.

        Args:
            stream: Stream name
            group: Consumer group name

        Returns:
            Number of groups destroyed (0 or 1)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xgroup_destroy() -> int:
            try:
                return self._client.xgroup_destroy(stream, group)  # type: ignore[return-value]
            except redis.ResponseError:
                return 0

        return self._retry_on_transient_errors(_xgroup_destroy)

    def register_subscriber(self, name: str, info: dict[str, Any]) -> None:
        """Register subscriber in Redis.

        Creates entries in:
        - gpmq:subscribers SET
        - gpmq:subscriber:{name} HASH
        - gpmq:subscriber:{name}:heartbeat STRING

        Args:
            name: Subscriber name
            info: Subscriber info dict (includes name, handlers, etc.)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _register() -> None:
            key_prefix = self._config.registry_key_prefix
            self._client.sadd(f"{key_prefix}subscribers", name)
            # Serialize complex values to JSON
            serialized_info = {
                k: json.dumps(v) if isinstance(v, (list, dict)) else v
                for k, v in info.items()
            }
            self._client.hset(f"{key_prefix}subscriber:{name}", mapping=serialized_info)
            self._client.set(
                f"{key_prefix}subscriber:{name}:heartbeat",
                json.dumps({"timestamp": time.time()}),
            )

        self._retry_on_transient_errors(_register)

    def get_subscriber_info(self, name: str) -> dict[str, Any] | None:
        """Get subscriber info from hash.

        Args:
            name: Subscriber name

        Returns:
            Subscriber info dict, or None if not found

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _get() -> dict[str, Any] | None:
            key_prefix = self._config.registry_key_prefix
            result = self._client.hgetall(f"{key_prefix}subscriber:{name}")
            if not result:
                return None
            # Deserialize JSON values back to original types
            info = {}
            for k, v in result.items():
                try:
                    info[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    info[k] = v
            return info

        return self._retry_on_transient_errors(_get)

    def update_heartbeat(self, name: str) -> None:
        """Update subscriber heartbeat timestamp.

        Sets gpmq:subscriber:{name}:heartbeat to current timestamp.

        Args:
            name: Subscriber name

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _update() -> None:
            key_prefix = self._config.registry_key_prefix
            self._client.set(
                f"{key_prefix}subscriber:{name}:heartbeat",
                json.dumps({"timestamp": time.time()}),
            )

        self._retry_on_transient_errors(_update)

    def get_active_subscribers(self, timeout: int = 30) -> list[str]:
        """Get subscribers with recent heartbeats.

        Args:
            timeout: Timeout in seconds for "recent" heartbeat

        Returns:
            List of subscriber names with (now - heartbeat) < timeout seconds

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _get() -> list[str]:
            key_prefix = self._config.registry_key_prefix
            now = time.time()
            active: list[str] = []

            subscribers = self._client.smembers(f"{key_prefix}subscribers")
            if not subscribers:
                return active

            keys = [f"{key_prefix}subscriber:{sub}:heartbeat" for sub in subscribers]
            pipe = self._client.pipeline()
            for key in keys:
                pipe.get(key)
            raw_values = pipe.execute()

            for sub, data in zip(subscribers, raw_values):
                if data:
                    heartbeat_data = json.loads(data)
                    timestamp = heartbeat_data.get("timestamp")
                    if timestamp is not None and now - timestamp < timeout:
                        active.append(sub)

            return active

        return self._retry_on_transient_errors(_get)

    def unregister_subscriber(self, name: str) -> None:
        """Remove subscriber from registry.

        Deletes all subscriber-related keys.

        Args:
            name: Subscriber name

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _unregister() -> None:
            key_prefix = self._config.registry_key_prefix
            self._client.srem(f"{key_prefix}subscribers", name)
            self._client.delete(f"{key_prefix}subscriber:{name}")
            self._client.delete(f"{key_prefix}subscriber:{name}:heartbeat")

        self._retry_on_transient_errors(_unregister)

    def publish(self, channel: str, message: Any) -> None:
        """Publish message to Redis Pub/Sub channel.

        Args:
            channel: Channel name (format: gpmq:result:{msg_id}:{subscriber})
            message: Message data (will be JSON serialized)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _publish() -> None:
            import json

            self._client.publish(channel, json.dumps(message))

        self._retry_on_transient_errors(_publish)

    def subscribe(self, channels: list[str]) -> "redis.client.PubSub":
        """Create PubSub subscription.

        Args:
            channels: List of channel names to subscribe to

        Returns:
            redis.client.PubSub object

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _subscribe() -> "redis.client.PubSub":
            pubsub = self._client.pubsub()
            pubsub.subscribe(channels)
            return pubsub  # type: ignore[return-value]

        return self._retry_on_transient_errors(_subscribe)

    def get_message(
        self, pubsub: "redis.client.PubSub", timeout: float | None = None
    ) -> dict | None:
        """Get message from PubSub subscription.

        Args:
            pubsub: PubSub object from subscribe()
            timeout: Timeout in seconds (None = non-blocking)

        Returns:
            Dict with 'channel' and 'data' keys, or None on timeout

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _get() -> dict | None:
            import json

            msg = pubsub.get_message(timeout=timeout)
            if msg is None:
                return None
            if msg["type"] == "message":
                data = json.loads(msg["data"])
                return {"channel": msg["channel"], "data": data}
            return None

        return self._retry_on_transient_errors(_get)

    def unsubscribe(self, pubsub: "redis.client.PubSub") -> None:
        """Unsubscribe from all channels.

        Args:
            pubsub: PubSub object from subscribe()

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _unsubscribe() -> None:
            pubsub.unsubscribe()

        self._retry_on_transient_errors(_unsubscribe)

    def xpending(
        self,
        stream: str,
        group: str,
    ) -> dict[str, Any]:
        """Get pending message info.

        Args:
            stream: Stream name
            group: Consumer group name

        Returns:
            Dict with keys: count, min_id, max_id, consumers

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xpending() -> dict[str, Any]:
            result = self._client.xpending(stream, group)
            # result: {'pending': count, 'min': min_id, 'max': max_id, 'consumers': [...]}
            return {
                "count": result.get("pending", 0),
                "min_id": result.get("min"),
                "max_id": result.get("max"),
                "consumers": result.get("consumers", []),
            }

        return self._retry_on_transient_errors(_xpending)

    def xclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        message_ids: list[str],
    ) -> list[tuple[str, dict]]:
        """Claim idle messages for this consumer.

        Used to reassign messages from crashed or timed-out workers.

        Args:
            stream: Stream name
            group: Consumer group name
            consumer: Consumer name claiming the messages
            min_idle_time: Minimum idle time in milliseconds
            message_ids: List of message IDs to claim

        Returns:
            List of [(message_id, fields)] for claimed messages

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _xclaim() -> list[tuple[str, dict]]:
            result = self._client.xclaim(
                stream, group, consumer, min_idle_time, message_ids
            )
            return result  # type: ignore[return-value]

        return self._retry_on_transient_errors(_xclaim)

    def register_worker(
        self, subscriber_name: str, worker_id: str, info: dict[str, Any]
    ) -> None:
        """Register a worker in the consumer group's worker registry.

        Args:
            subscriber_name: Subscriber/consumer group name
            worker_id: Unique worker identifier
            info: Worker info dict (hostname, pid, status, etc.)

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _register() -> None:
            key_prefix = self._config.registry_key_prefix
            key = f"{key_prefix}workers:{subscriber_name}"
            self._client.hset(key, worker_id, json.dumps(info))

        self._retry_on_transient_errors(_register)

    def update_worker_heartbeat(
        self, subscriber_name: str, worker_id: str
    ) -> None:
        """Update a worker's heartbeat timestamp.

        Args:
            subscriber_name: Subscriber/consumer group name
            worker_id: Unique worker identifier

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _update() -> None:
            key_prefix = self._config.registry_key_prefix
            key = f"{key_prefix}workers:{subscriber_name}"
            raw = self._client.hget(key, worker_id)
            if raw:
                info = json.loads(raw)
                info["last_heartbeat"] = str(time.time())
                self._client.hset(key, worker_id, json.dumps(info))

        self._retry_on_transient_errors(_update)

    def unregister_worker(
        self, subscriber_name: str, worker_id: str
    ) -> None:
        """Remove a worker from the consumer group's registry.

        Args:
            subscriber_name: Subscriber/consumer group name
            worker_id: Unique worker identifier

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _unregister() -> None:
            key_prefix = self._config.registry_key_prefix
            key = f"{key_prefix}workers:{subscriber_name}"
            self._client.hdel(key, worker_id)

        self._retry_on_transient_errors(_unregister)

    def get_consumer_group_workers(
        self, subscriber_name: str
    ) -> list[dict[str, Any]]:
        """Get all worker info for a consumer group.

        Args:
            subscriber_name: Subscriber/consumer group name

        Returns:
            List of worker info dicts, each with an added 'worker_id' field

        Raises:
            RedisConnectionError: On connection failure after retries
        """

        def _get() -> list[dict[str, Any]]:
            key_prefix = self._config.registry_key_prefix
            key = f"{key_prefix}workers:{subscriber_name}"
            raw_map = self._client.hgetall(key)
            if not raw_map:
                return []
            workers = []
            for worker_id, raw_info in raw_map.items():
                info = json.loads(raw_info)
                info["worker_id"] = worker_id
                workers.append(info)
            return workers

        return self._retry_on_transient_errors(_get)
