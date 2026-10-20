"""GPMQ CLI - Command-line interface for GPMQ message queue management."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

import click
from gpconfig import GPConfigManager

from gpmq.config import (
    GPMQConfig,
    get_config_manager,
    get_subscriber,
    set_config_manager,
)
from gpmq.models.result import ProcessStatus
from gpmq.storage.audit_store import AuditStore

if TYPE_CHECKING:
    from gpmq.storage.redis_adapter import RedisAdapter

# Identity-preserving bound for click decorator pass-throughs.
FC = TypeVar("FC", bound="Callable[..., Any] | click.Command")


def _init_cfg_mgr(config_path: str | None) -> GPConfigManager:
    """Initialize GPConfigManager and set it as global config manager.

    Args:
        config_path: Optional path to config folder containing global_env.yaml.
            If not provided, GPConfigManager will search via env variable
            GPMQ_CLI_CFG_PATH or user home directory.

    Returns:
        Initialized GPConfigManager instance.
    """
    cfg_mgr = GPConfigManager(
        project_name="gpmq_cli",
        cfg_folder=config_path,
    )
    set_config_manager(cfg_mgr)
    return cfg_mgr


def _lazy_init_cfg_mgr(ctx: click.Context, param: click.Parameter, value: str | None) -> None:
    """Callback for --config option on subcommands: init GPConfigManager if not done."""
    if get_config_manager() is not None:
        return
    _init_cfg_mgr(value)


def _config_option(func: FC) -> FC:
    """Decorator that adds --config option to a subcommand for lazy init."""
    return click.option(
        "--config",
        default=None,
        expose_value=False,
        callback=_lazy_init_cfg_mgr,
        help="Configuration folder path (must contain global_env.yaml). "
        "If not provided, searches GPMQ_CLI_CFG_PATH env var or user home.",
    )(func)


def _format_output(data: Any, fmt: str) -> str:
    """Format data for CLI output.

    Args:
        data: Data to format (list of dicts or dict).
        fmt: Output format - "json" or "table".

    Returns:
        Formatted string.
    """
    if fmt == "json":
        return json.dumps(data, indent=2, default=str)

    if fmt == "table":
        if not data:
            return ""

        # Handle dict (stats) vs list of dicts (query results)
        if isinstance(data, dict):
            lines = []
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"{key}:")
                    for k, v in value.items():
                        lines.append(f"  {k}: {v}")
                else:
                    lines.append(f"{key}: {value}")
            return "\n".join(lines)

        # List of dicts - column-aligned table
        if isinstance(data, list) and len(data) > 0:
            columns = list(data[0].keys())
            # Calculate column widths
            widths = {}
            for col in columns:
                widths[col] = len(str(col))
                for row in data:
                    widths[col] = max(widths[col], len(str(row.get(col, ""))))

            # Build header
            header = "  ".join(str(col).ljust(widths[col]) for col in columns)
            separator = "  ".join("-" * widths[col] for col in columns)
            # Build rows
            rows = []
            for row in data:
                line = "  ".join(
                    str(row.get(col, "")).ljust(widths[col]) for col in columns
                )
                rows.append(line)

            return "\n".join([header, separator] + rows)

        return ""

    raise ValueError(f"Unsupported output format: {fmt!r} (expected 'json' or 'table')")


def _resolve_audit_config(cfg_path: str) -> tuple[str, bool]:
    """Resolve audit_db_path and enable_audit for a given component config path.

    Merges common config with the specific component config, following the
    same pattern as get_subscriber() and get_client().

    Args:
        cfg_path: Dot-notation config path, e.g. "gpmq.subscribers.data_loader"
            or "gpmq.publisher.main".

    Returns:
        Tuple of (audit_db_path, enable_audit).
    """
    cfg_mgr = get_config_manager()
    if cfg_mgr is None:
        raise click.ClickException(
            "config manager is not initialized; pass --config or set GPMQ_CLI_CFG_PATH"
        )
    common_dict = cfg_mgr.get_config(GPMQConfig.default_cfg_path).model_dump()
    specific = cfg_mgr.get_config(cfg_path)
    specific_set = specific.model_dump(exclude_unset=True)
    merged = {**common_dict, **specific_set}

    return merged.get("audit_db_path", "gpmq_audit.db"), merged.get("enable_audit", True)


@contextmanager
def _get_audit_store(db_path: str) -> Iterator[AuditStore]:
    """Context manager that creates and closes an AuditStore.

    Args:
        db_path: Path to the SQLite audit database file.

    Yields:
        AuditStore instance.
    """
    store = AuditStore(db_path)
    store.connect()
    try:
        yield store
    finally:
        store.close()


def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO format datetime string.

    Args:
        value: ISO datetime string (e.g., '2026-01-01T00:00:00').

    Returns:
        datetime object.

    Raises:
        click.BadParameter: If the string cannot be parsed.
    """
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise click.BadParameter(
            f"Invalid datetime format: {value}. Use ISO format like 2026-01-01T00:00:00"
        )


@contextmanager
def _create_redis_adapter() -> Iterator["RedisAdapter"]:
    """Context manager that creates, connects, and disconnects a RedisAdapter.

    Uses the global GPConfigManager to load configuration.

    Yields:
        Connected RedisAdapter instance.
    """
    from gpmq.storage.redis_adapter import RedisAdapter

    cfg_mgr = get_config_manager()
    if cfg_mgr is None:
        raise click.ClickException(
            "config manager is not initialized; pass --config or set GPMQ_CLI_CFG_PATH"
        )
    config = cfg_mgr.get_config(GPMQConfig.default_cfg_path)
    adapter = RedisAdapter(config)
    adapter.connect()
    try:
        yield adapter
    finally:
        adapter.disconnect()


@click.group()
@click.option(
    "--config",
    default=None,
    help="Configuration folder path (must contain global_env.yaml). "
    "If not provided, searches GPMQ_CLI_CFG_PATH env var or user home.",
)
@click.pass_context
def cli(ctx: click.Context, config: str | None) -> None:
    """GPMQ - General Purpose Message Queue CLI."""
    from gpmq import use_spawn
    use_spawn()
    ctx.ensure_object(dict)
    if config is not None:
        _init_cfg_mgr(config)


@cli.group()
@click.pass_context
def audit(ctx: click.Context) -> None:
    """Audit message and result records."""
    pass


@audit.command("query")
@_config_option
@click.argument("component", metavar="COMPONENT_NAME")
@click.option("--type", "message_type", default=None, help="Filter by message type.")
@click.option(
    "--status",
    "status_filter",
    default=None,
    help="Filter by processing status (success/failure/exception/timeout).",
)
@click.option("--correlation-id", default=None, help="Filter by correlation ID.")
@click.option("--from", "from_time", default=None, help="Start time (ISO format).")
@click.option("--to", "to_time", default=None, help="End time (ISO format).")
@click.option("--limit", default=100, type=int, help="Max results (default: 100).")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "table"]),
    default="table",
    help="Output format.",
)
@click.pass_context
def audit_query(
    ctx: click.Context,
    component: str,
    message_type: str | None,
    status_filter: str | None,
    correlation_id: str | None,
    from_time: str | None,
    to_time: str | None,
    limit: int,
    fmt: str,
) -> None:
    """Query audit records with optional filters."""
    audit_db_path, enable_audit = _resolve_audit_config(component)
    if not enable_audit:
        click.echo(f"Audit store is disabled for '{component}'.")
        return

    click.echo(f"Audit store: {audit_db_path}")

    with _get_audit_store(audit_db_path) as store:
        results = []

        if correlation_id:
            results = store.query_by_correlation_id(correlation_id)
        elif status_filter:
            try:
                status_enum = ProcessStatus(status_filter)
            except ValueError:
                valid = ", ".join(s.value for s in ProcessStatus)
                raise click.BadParameter(
                    f"Invalid status: {status_filter}. Valid values: {valid}"
                )
            results = store.query_by_status(status_enum, limit=limit)
        elif message_type:
            start = _parse_iso_datetime(from_time) if from_time else None
            end = _parse_iso_datetime(to_time) if to_time else None
            results = store.query_by_type(
                message_type, start_time=start, end_time=end, limit=limit
            )
        elif from_time and to_time:
            start = _parse_iso_datetime(from_time)
            end = _parse_iso_datetime(to_time)
            results = store.query_by_time_range(start, end, limit=limit)
        else:
            # Fallback: no filters, get recent
            click.echo(
                "No filters specified. Use --type, --status, --correlation-id, or --from/--to."
            )
            return

        click.echo(_format_output(results, fmt))


@audit.command("cleanup")
@_config_option
@click.argument("component", metavar="COMPONENT_NAME")
@click.option(
    "--before",
    required=True,
    help="Delete records before this timestamp (ISO format).",
)
@click.option("--type", "message_type", default=None, help="Delete by message type.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "table"]),
    default="table",
    help="Output format.",
)
@click.pass_context
def audit_cleanup(
    ctx: click.Context, component: str, before: str, message_type: str | None, fmt: str
) -> None:
    """Delete old audit records."""
    audit_db_path, enable_audit = _resolve_audit_config(component)
    if not enable_audit:
        click.echo(f"Audit store is disabled for '{component}'.")
        return

    click.echo(f"Audit store: {audit_db_path}")
    before_dt = _parse_iso_datetime(before)

    with _get_audit_store(audit_db_path) as store:
        if message_type:
            count = store.cleanup_by_type_before(message_type, before_dt)
        else:
            count = store.cleanup_by_time(before_dt)

        result = {"deleted_count": count}
        click.echo(_format_output(result, fmt))


@audit.command("stats")
@_config_option
@click.argument("component", metavar="COMPONENT_NAME")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "table"]),
    default="table",
    help="Output format.",
)
@click.pass_context
def audit_stats(ctx: click.Context, component: str, fmt: str) -> None:
    """Show audit statistics."""
    audit_db_path, enable_audit = _resolve_audit_config(component)
    if not enable_audit:
        click.echo(f"Audit store is disabled for '{component}'.")
        return

    click.echo(f"Audit store: {audit_db_path}")

    with _get_audit_store(audit_db_path) as store:
        stats = store.stats()
        click.echo(_format_output(stats, fmt))


@cli.command()
@_config_option
@click.option("--stream", required=True, help="Stream name to clear.")
@click.pass_context
def clear(ctx: click.Context, stream: str) -> None:
    """Clear a Redis Stream (requires confirmation)."""
    with _create_redis_adapter() as adapter:
        if click.confirm(f"Are you sure you want to clear stream '{stream}'?"):
            adapter.clear_stream(stream)
            click.echo(f"Stream '{stream}' cleared.")
        else:
            click.echo("Cancelled.")


@cli.command()
@_config_option
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["json", "table"]),
    default="table",
    help="Output format.",
)
@click.pass_context
def status(ctx: click.Context, fmt: str) -> None:
    """Show system status: active subscribers and their info."""
    with _create_redis_adapter() as adapter:
        names = adapter.get_active_subscribers()
        subscribers = []
        for name in names:
            info = adapter.get_subscriber_info(name)
            if info:
                entry = {"name": name}
                entry.update(info)
                subscribers.append(entry)
        click.echo(_format_output(subscribers, fmt))


def _run_worker(subscriber_name: str) -> None:
    """Common worker startup logic.

    Args:
        subscriber_name: Dot-notation config path, e.g. "gpmq.subscribers.data_loader".
    """
    sub = get_subscriber(subscriber_name)
    click.echo(f"Subscriber '{sub.name}' started. Press Ctrl+C to stop.")
    sub.run_forever()
    click.echo("Subscriber stopped.")


@cli.command()
@_config_option
@click.argument("subscriber_name")
@click.pass_context
def worker(ctx: click.Context, subscriber_name: str) -> None:
    """Start a GPMQ subscriber worker process."""
    _run_worker(subscriber_name)


@click.command()
@click.argument("subscriber_name")
@click.option(
    "--config",
    default=None,
    help="Configuration folder path (must contain global_env.yaml). "
    "If not provided, searches GPMQ_CLI_CFG_PATH env var or user home.",
)
def gpmq_worker(subscriber_name: str, config: str | None) -> None:
    """Start a GPMQ subscriber worker process."""
    from gpmq import use_spawn
    use_spawn()
    _init_cfg_mgr(config)
    _run_worker(subscriber_name)
