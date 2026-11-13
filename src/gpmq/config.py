"""GPMQ configuration classes.

This module defines configuration classes for GPMQ components,
integrating with gpconfig for configuration management.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from gpconfig import GPConfig, GPConfigManager

if TYPE_CHECKING:
    from gpmq.client import GPMQClient
    from gpmq.subscriber.subscriber import Subscriber


class HandlerConfig(BaseModel):
    """Configuration for a single message handler.

    Attributes:
        message_types: List of message types this handler processes
        handler: Import path to the handler function (e.g., "myapp.handlers.on_user")
    """

    message_types: list[str] = Field(
        min_length=1, description="Message types to handle"
    )
    handler: str = Field(min_length=1, description="Handler function import path")


class GPMQConfig(GPConfig):
    """Base configuration for all GPMQ components.

    This class defines shared configuration items for both publishers
    and subscribers.

    Attributes:
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (empty string if none)
        redis_max_retries: Maximum connection retry attempts
        redis_retry_delay: Base delay for retry in seconds
        redis_socket_timeout: Redis socket timeout in seconds (on
            SubscriberConfig it must exceed worker_read_block_ms / 1000;
            see the SubscriberConfig model_validator)
        stream_name: Name of the Redis Stream
        heartbeat_interval: Heartbeat interval in seconds
        registry_key_prefix: Prefix for registry keys in Redis
        stream_trim_interval: Seconds between stream-retention trims
            run by each subscriber's heartbeat thread (0 disables
            subscriber-side trimming; publisher-side idle cap independent)
        stream_trim_on_last_stop: Whether the last registered subscriber
            performs a final trim on stop
        stream_trim_idle_maxlen: Hard entry cap applied via XADD MAXLEN
            only while the registry is empty (None = unbounded)
        publish_require_subscriber: Skip publishing when no REGISTERED
            subscriber declares the message type
        pel_abandon_idle_seconds: Seconds a PEL entry must stay idle before
            the subscriber's heartbeat scan abandons it (0 disables)
        audit_db_path: Path to SQLite audit database
        enable_audit: Enable or disable audit store
        log_category: Log category for gpclog
    """

    cfg_class_name: ClassVar[str] = "GPMQConfig"
    # gpconfig save contract: folder path (slash style); must NOT contain '.'
    # (gpconfig >= 0.4 rejects '.' in default_cfg_path at class-definition
    # time via GPConfig.__init_subclass__).
    default_cfg_path: ClassVar[str] = "gpmq/common"

    @classmethod
    def dotted_default_cfg_path(cls) -> str:
        """Dot-notation cfg_path for GPConfigManager.get_config() addressing.

        gpconfig >= 0.4 requires dot notation in get_config() paths (literal
        '/' or '\\' raise IllegalPathError), while default_cfg_path itself is
        a save-contract folder path that must stay slash-style. This derived
        accessor converts one to the other; subclasses inherit it, deriving
        from their own default_cfg_path.

        Returns:
            Dot-notation addressing path, e.g. ``"gpmq.common"``. Empty
            segments (double/edge slashes) are dropped, so ``"gpmq//common/"``
            derives ``"gpmq.common"``.
        """
        segments = [s for s in cls.default_cfg_path.replace("\\", "/").split("/") if s]
        return ".".join(segments)

    # Redis connection
    redis_host: str = Field(default="localhost", description="Redis server host")
    redis_port: int = Field(default=6379, ge=1, le=65535, description="Redis port")
    redis_db: int = Field(
        default=0, ge=0, le=65535, description="Redis database number"
    )
    redis_password: str = Field(default="", description="Redis password")
    redis_max_retries: int = Field(default=3, ge=0, description="Max retry attempts")
    redis_retry_delay: float = Field(
        default=1.0, ge=0.1, description="Base retry delay in seconds"
    )
    redis_socket_timeout: float = Field(
        default=10.0,
        description=(
            "Redis socket timeout in seconds (SubscriberConfig additionally "
            "requires it to exceed worker_read_block_ms / 1000; see the "
            "SubscriberConfig model_validator)"
        ),
    )

    # Stream config
    stream_name: str = Field(default="gpmq:messages", description="Redis Stream name")

    # Registry
    heartbeat_interval: int = Field(
        default=15,
        ge=5,
        description="Heartbeat interval in seconds (minimum 5; see validator)",
    )
    registry_key_prefix: str = Field(
        default="gpmq:", description="Redis key prefix for registry"
    )

    # Stream retention (see dev_docs/specs/2026-11-07-stream-retention.md)
    stream_trim_interval: float = Field(
        default=60.0,
        ge=0,
        description=(
            "Seconds between stream-retention trims run by each subscriber's "
            "heartbeat thread; 0 disables subscriber-side trimming "
            "(publisher-side idle cap independent)"
        ),
    )
    stream_trim_on_last_stop: bool = Field(
        default=True,
        description="Final trim by the last registered subscriber on stop",
    )
    stream_trim_idle_maxlen: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Hard entry cap applied via XADD MAXLEN only while the "
            "registry is empty; None = unbounded"
        ),
    )
    publish_require_subscriber: bool = Field(
        default=False,
        description=(
            "Skip publishing when no REGISTERED subscriber declares the "
            "message type (registration-based, preserves crash catch-up)"
        ),
    )

    # Orphan PEL recovery — semantics documented in docs/api "Stream
    # retention" (Orphaned pending entries) and CHANGELOG 0.11.1.
    pel_abandon_idle_seconds: float = Field(
        default=60.0,
        ge=0,
        description=(
            "Seconds a pending (PEL) entry must stay idle before the "
            "subscriber's heartbeat scan abandons it (XACK out of the PEL + "
            "ABANDONED audit record; never redelivered); 0 disables orphan "
            "recovery. Keep comfortably above the longest handler runtime; "
            "RPC caller timeouts should stay well below it"
        ),
    )

    @field_validator("registry_key_prefix")
    @classmethod
    def _ensure_trailing_colon(cls, v: str) -> str:
        """Auto-append trailing ':' if missing."""
        if v and not v.endswith(":"):
            v += ":"
        return v

    @field_validator("heartbeat_interval")
    @classmethod
    def _ensure_heartbeat_above_cadence(cls, v: int) -> int:
        """Reject heartbeat_interval below 5.

        Workers flush their worker-level heartbeat on a 4-second throttle
        inside the XREADGROUP block (worker.py
        HEARTBEAT_THROTTLE_SECONDS; block now defaults to 1000ms — see
        ``worker_read_block_ms``), so the worst-case gap between heartbeats
        is throttle + block = 4 + 1 = 5s (traffic ending right after a
        skipped check pushes two checks one full block apart).
        The stale-detection threshold is heartbeat_interval * 3; the
        invariant it relies on is ``heartbeat_interval * 3 > 5s``, which
        even the floor of 5 satisfies with 3x headroom (15s vs 5s). The
        ge=5 floor is kept so configs that lengthen the block again
        (e.g. worker_read_block_ms=5000 -> worst case 4 + 5 = 9s) still
        leave comfortable margin (threshold 15s at the floor).
        """
        if v < 5:
            raise ValueError(
                f"heartbeat_interval must be >= 5 (got {v}); workers flush "
                f"their heartbeat at worst every ~5s (4s throttle + 1s read "
                f"block, see worker_read_block_ms), and the stale-detection "
                f"threshold is heartbeat_interval*3, so a smaller interval "
                f"would falsely flag healthy workers as stale"
            )
        return v

    # Audit
    audit_db_path: str = Field(
        default="gpmq_audit.db", description="Path to SQLite audit database"
    )
    enable_audit: bool = Field(
        default=True, description="Enable or disable audit store"
    )
    log_category: str = Field(default="gpmq", description="Log category for gpclog")


class PublisherConfig(GPMQConfig):
    """Publisher-specific configuration.

    Extends GPMQConfig with publisher-specific settings.

    Attributes:
        publisher_name: Unique name for this publisher (required)
        publish_timeout: Total timeout for sync publish (None = no limit)
        async_batch_queue_size: Default queue size for async_batch()
    """

    cfg_class_name: ClassVar[str] = "PublisherConfig"
    default_cfg_path: ClassVar[str] = "gpmq/publisher/main"

    publisher_name: str = Field(min_length=1, description="Publisher name")
    publish_timeout: Optional[float] = Field(
        default=None, ge=1.0, description="Total publish timeout (None = no limit)"
    )
    async_batch_queue_size: int = Field(
        default=32, ge=1, description="Default queue size for async_batch()"
    )


class SubscriberConfig(GPMQConfig):
    """Subscriber-specific configuration.

    Extends GPMQConfig with subscriber-specific settings.

    Attributes:
        name: Unique name for this subscriber (also consumer group name)
        handlers: List of handler configurations
        workers: Number of worker processes
        worker_read_batch_size: Number of messages each worker reads per XREADGROUP round
        subscriber_timeout: Timeout for individual subscriber in seconds
        worker_read_block_ms: Worker XREADGROUP block time in ms for reading
            NEW entries (own-PEL poll is non-blocking); redis_socket_timeout
            must exceed worker_read_block_ms / 1000
        embedded_publisher: publisher config path for an embedded publisher in a subscriber that do message relay
        worker_context_processor: Import path to a worker context processor function
        worker_context_finalizer: Import path to a worker context finalizer function
            (worker stop-side counterpart of worker_context_processor)
        worker_context: Custom worker context data dict
        run_forever_method: Import path to a run_forever_method decorated function
        run_forever_method_paras: Custom parameters dict passed to run_forever_method function's paras argument
    """

    cfg_class_name: ClassVar[str] = "SubscriberConfig"
    default_cfg_path: ClassVar[str] = "gpmq/subscribers"

    name: str = Field(min_length=1, description="Subscriber name (consumer group)")
    handlers: list[HandlerConfig] = Field(
        min_length=1, description="Handler configurations"
    )
    workers: int = Field(default=1, ge=1, description="Number of worker processes")
    worker_read_batch_size: int = Field(
        default=1,
        ge=1,
        description="Number of messages each worker reads per XREADGROUP round",
    )
    subscriber_timeout: float = Field(
        default=60.0, ge=1.0, description="Subscriber processing timeout in seconds"
    )
    worker_read_block_ms: int = Field(
        default=1000,
        ge=100,
        description=(
            "Worker XREADGROUP block time in milliseconds for reading NEW "
            "entries (the own-PEL poll is non-blocking); bounded pickup "
            "latency for mis-routed sticky handoffs"
        ),
    )

    @model_validator(mode="after")
    def _ensure_socket_timeout_above_block(self) -> "SubscriberConfig":
        """Reject redis_socket_timeout <= the worker XREADGROUP block time.

        redis-py requires socket_timeout strictly greater than any blocking
        command duration; the block time is now ``worker_read_block_ms``
        (was hardcoded 5000).
        """
        block_s = self.worker_read_block_ms / 1000.0
        if self.redis_socket_timeout <= block_s:
            raise ValueError(
                f"redis_socket_timeout must be > {block_s}s (got "
                f"{self.redis_socket_timeout}); workers issue a blocking "
                f"XREADGROUP with worker_read_block_ms={self.worker_read_block_ms}"
            )
        return self

    embedded_publisher: str | None = Field(
        default=None,
        description="Publisher config path for embedded publisher in worker context",
    )
    worker_context_processor: str | None = Field(
        default=None, description="Import path to a worker context processor function"
    )
    worker_context_finalizer: str | None = Field(
        default=None,
        description="Import path to a worker context finalizer function "
        "(called when the worker stops gracefully)",
    )
    worker_context: Optional[dict[str, Any]] = Field(
        default=None, description="Custom worker context data dict"
    )
    run_forever_method: Optional[str] = Field(
        default=None,
        description="Import path to a run_forever_method decorated function",
    )
    run_forever_method_paras: Optional[dict[str, Any]] = Field(
        default=None,
        description="Custom parameters dict passed to run_forever_method function's paras argument",
    )

    def get_message_types(self) -> list[str]:
        """Get all subscribed message types.

        Aggregates message types from all handlers and deduplicates.

        Returns:
            List of unique message types this subscriber handles
        """
        types: list[str] = []
        for handler in self.handlers:
            types.extend(handler.message_types)
        return list(dict.fromkeys(types))


# Auto-register config and classes with GPConfigManager
GPConfigManager.register_config_class(GPMQConfig)
GPConfigManager.register_config_class(PublisherConfig)
GPConfigManager.register_config_class(SubscriberConfig)


_gpmq_cfg_mgr: GPConfigManager | None = (
    None  # keep a reference of initialized config manager instance
)


def set_config_manager(cfg_mgr: GPConfigManager) -> None:
    """Set the configuration manager of the project.

    When called, an instance of config manager will be initialized
    from YAML files in the specified configuration folder.

    Args:
        cfg_mgr: an instance of GPConfigManager class
    """
    global _gpmq_cfg_mgr

    if not isinstance(cfg_mgr, GPConfigManager):
        raise TypeError(
            f"cfg_mgr must be an instance of GPConfigManager, not {type(cfg_mgr)}"
        )

    _gpmq_cfg_mgr = cfg_mgr


def get_config_manager() -> GPConfigManager | None:
    """Return the current config manager instance, or ``None`` if unset."""
    return _gpmq_cfg_mgr


def _require_cfg_mgr() -> GPConfigManager:
    """Return the config manager or raise RuntimeError if not set."""
    if _gpmq_cfg_mgr is None:
        raise RuntimeError(
            "GPConfigManager has not been set. "
            "Call set_config_manager() before using get_subscriber() or get_client()."
        )
    return _gpmq_cfg_mgr


def get_subscriber(cfg_path: str) -> "Subscriber":
    """Create a Subscriber from merged common + specific config.

    Loads the base GPMQ config from ``gpmq.common`` and merges it with
    the subscriber-specific config at *cfg_path*.  The subscriber config
    only needs to contain its own fields (name, handlers, workers, etc.)
    and any common fields it wants to override.

    The caller must have called ``set_config_manager()`` beforehand.

    Args:
        cfg_path: Dot-notation config path, e.g. ``"gpmq.subscribers.notifier"``.

    Returns:
        A constructed (but not started) Subscriber instance.

    Raises:
        RuntimeError: If ``set_config_manager()`` has not been called.
    """
    from gpmq.subscriber.subscriber import Subscriber

    cfg_mgr = _require_cfg_mgr()

    # 1) Load base common config
    common_dict = cfg_mgr.get_config(GPMQConfig.dotted_default_cfg_path()).model_dump()

    # 2) Load specific subscriber config
    specific = cfg_mgr.get_config(cfg_path)

    # 3) Merge: common as base, only explicitly-set fields from specific override
    specific_set = specific.model_dump(exclude_unset=True)
    merged = {**common_dict, **specific_set}

    # 4) Construct final SubscriberConfig
    config = SubscriberConfig(**merged)

    subscriber = Subscriber(config)
    subscriber.apply_worker_context(
        project_name=cfg_mgr.project_name, config_manager_folder_path=cfg_mgr.cfg_folder
    )

    if config.embedded_publisher:
        subscriber.apply_worker_context(publisher_cfg_path=config.embedded_publisher)

    if config.worker_context_processor:
        subscriber.apply_worker_context(
            worker_context_processor=config.worker_context_processor
        )

    if config.worker_context_finalizer:
        subscriber.apply_worker_context(
            worker_context_finalizer=config.worker_context_finalizer
        )

    if config.worker_context:
        subscriber.apply_worker_context(**config.worker_context)

    return subscriber


def get_client(
    cfg_path: Optional[str] = None,
    publisher_name: Optional[str] = None,
    timeout: Optional[float] = None,
) -> "GPMQClient":
    """Create a GPMQClient from merged common + specific config.

    Three usage modes:

    1. ``get_client("gpmq.publisher.main")``
       - Load common config + PublisherConfig from *cfg_path*.
       - ``publisher_name`` comes from the config file.

    2. ``get_client(publisher_name="my_app")``
       - Load common config and construct PublisherConfig with the
         given *publisher_name*.

    3. ``get_client()``
       - Load common config only; creates an anonymous client.

    The caller must have called ``set_config_manager()`` beforehand.

    Args:
        cfg_path: Dot-notation path to a PublisherConfig yaml.
        publisher_name: Publisher name. Used to construct PublisherConfig
            when no *cfg_path* is given; when both *cfg_path* and
            *publisher_name* are given, they must match.
        timeout: If given, overrides ``publish_timeout`` in the final config.
            Requires *cfg_path* or *publisher_name* (not applicable for
            anonymous clients).

    Returns:
        A constructed (but not connected) GPMQClient instance.

    Raises:
        RuntimeError: If ``set_config_manager()`` has not been called.
        ValueError: If both *cfg_path* and *publisher_name* are given
            and the names conflict, or if *timeout* is given for an
            anonymous client.
    """
    from gpmq.client import GPMQClient

    cfg_mgr = _require_cfg_mgr()

    # 1) Always load common config as base
    common_dict = cfg_mgr.get_config(GPMQConfig.dotted_default_cfg_path()).model_dump()

    if cfg_path is not None:
        # Mode 1: load specific config file
        specific = cfg_mgr.get_config(cfg_path)
        if not isinstance(specific, PublisherConfig):
            raise ValueError(
                f"cfg_path must resolve to a PublisherConfig, "
                f"got {type(specific).__name__}"
            )
        specific_set = specific.model_dump(exclude_unset=True)
        merged = {**common_dict, **specific_set}
        config_class = type(specific)

        # Validate publisher_name consistency
        if publisher_name is not None:
            cfg_name = merged.get("publisher_name")
            if cfg_name != publisher_name:
                raise ValueError(
                    f"publisher_name conflict: parameter says '{publisher_name}' "
                    f"but config '{cfg_path}' says '{cfg_name}'"
                )
    elif publisher_name is not None:
        # Mode 2: common + publisher_name -> PublisherConfig
        merged = {**common_dict, "publisher_name": publisher_name}
        config_class = PublisherConfig
    else:
        # Mode 3: common only -> GPMQConfig (anonymous)
        if timeout is not None:
            raise ValueError(
                "timeout is not applicable for anonymous clients; "
                "provide cfg_path or publisher_name"
            )
        config = GPMQConfig(**common_dict)
        return GPMQClient(config)

    config = config_class(**merged)

    # Apply timeout override if given
    if timeout is not None:
        config.publish_timeout = timeout

    return GPMQClient(config)
