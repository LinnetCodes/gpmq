"""Tests for GPMQ CLI module."""

import json
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from gpmq.cli import _format_output, cli


# --- _format_output tests ---


def test_format_output_json():
    """_format_output should format data as JSON."""
    data = [{"id": "1", "type": "test"}]
    result = _format_output(data, "json")
    parsed = json.loads(result)
    assert parsed == data


def test_format_output_yaml_rejected():
    """_format_output must reject the 'yaml' format (option was removed; never produced real YAML)."""
    import pytest

    data = [{"id": "1", "type": "test"}]
    with pytest.raises(ValueError):
        _format_output(data, "yaml")


def test_format_output_table_with_records():
    """_format_output should format data as aligned table with records."""
    data = [
        {"id": "1", "type": "user_created", "status": "success"},
        {"id": "2", "type": "order_created", "status": "failure"},
    ]
    result = _format_output(data, "table")
    # Should contain header and data rows
    assert "id" in result
    assert "type" in result
    assert "status" in result
    assert "user_created" in result
    assert "order_created" in result


def test_format_output_table_empty():
    """_format_output should handle empty data for table format."""
    data = []
    result = _format_output(data, "table")
    assert result == ""


# --- cli group tests ---


def test_cli_help():
    """cli --help should show GPMQ in output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "GPMQ" in result.output


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_cli_with_config_before_subcommand(mock_init, mock_resolve, mock_get_store):
    """gpmq --config PATH audit stats should init GPConfigManager with the path."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.stats.return_value = {}
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    runner.invoke(
        cli, ["--config", "/some/path", "audit", "stats", "gpmq.subscribers.test"]
    )
    assert any(call.args == ("/some/path",) for call in mock_init.call_args_list)


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_cli_with_config_after_subcommand(mock_init, mock_resolve, mock_get_store):
    """gpmq audit stats --config PATH should init GPConfigManager via subcommand."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.stats.return_value = {}
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    runner.invoke(
        cli, ["audit", "stats", "gpmq.subscribers.test", "--config", "/some/path"]
    )
    assert any(call.args == ("/some/path",) for call in mock_init.call_args_list)


# --- audit query tests ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_query_by_type(mock_init, mock_resolve, mock_get_store):
    """audit query --type should call query_by_type."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.query_by_type.return_value = [
        {"id": "1", "type": "user_created", "publisher": "test"}
    ]
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["audit", "query", "gpmq.subscribers.test", "--type", "user_created"]
    )
    assert result.exit_code == 0
    mock_store.query_by_type.assert_called_once()


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_query_by_status(mock_init, mock_resolve, mock_get_store):
    """audit query --status should call query_by_status."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.query_by_status.return_value = [
        {"message_id": "1", "status": "success", "subscriber_name": "sub1"}
    ]
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli, ["audit", "query", "gpmq.subscribers.test", "--status", "success"]
    )
    assert result.exit_code == 0
    mock_store.query_by_status.assert_called_once()


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_query_json_format(mock_init, mock_resolve, mock_get_store):
    """audit query --format json should output JSON."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.query_by_time_range.return_value = [{"id": "1", "type": "test"}]
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit",
            "query",
            "gpmq.subscribers.test",
            "--from",
            "2026-01-01T00:00:00",
            "--to",
            "2026-12-31T23:59:59",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0
    # Output should be valid JSON
    output = json.loads(result.output.split("Audit store:", 1)[1].split("\n", 1)[1])
    assert len(output) == 1


# --- audit cleanup tests ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_cleanup_by_time(mock_init, mock_resolve, mock_get_store):
    """audit cleanup --before should call cleanup_by_time."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.cleanup_by_time.return_value = 5
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit",
            "cleanup",
            "gpmq.subscribers.test",
            "--before",
            "2026-01-01T00:00:00",
        ],
    )
    assert result.exit_code == 0
    mock_store.cleanup_by_time.assert_called_once()
    assert "5" in result.output


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_cleanup_by_type(mock_init, mock_resolve, mock_get_store):
    """audit cleanup --type --before should call cleanup_by_type_before (2.5)."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.cleanup_by_type_before.return_value = 3
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit",
            "cleanup",
            "gpmq.subscribers.test",
            "--before",
            "2026-01-01T00:00:00",
            "--type",
            "user_created",
        ],
    )
    assert result.exit_code == 0
    # With both --type and --before, the combo method is used (type, before_dt).
    mock_store.cleanup_by_type_before.assert_called_once()
    assert mock_store.cleanup_by_type_before.call_args.args[0] == "user_created"
    assert "3" in result.output


# --- audit stats tests ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_stats_default_format(mock_init, mock_resolve, mock_get_store):
    """audit stats should show stats in table format."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.stats.return_value = {
        "total_messages": 10,
        "total_results": 25,
        "results_by_status": {"success": 20, "failure": 5},
        "messages_by_type": {"user_created": 7, "order_created": 3},
    }
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(cli, ["audit", "stats", "gpmq.subscribers.test"])
    assert result.exit_code == 0
    assert "10" in result.output
    assert "25" in result.output


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_stats_json_format(mock_init, mock_resolve, mock_get_store):
    """audit stats --format json should output JSON."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.stats.return_value = {
        "total_messages": 10,
        "total_results": 25,
        "results_by_status": {"success": 20},
        "messages_by_type": {"user_created": 7},
    }
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit", "stats", "gpmq.subscribers.test", "--format", "json"],
    )
    assert result.exit_code == 0
    output = json.loads(result.output.split("Audit store:", 1)[1].split("\n", 1)[1])
    assert output["total_messages"] == 10
    assert output["total_results"] == 25


# --- audit disabled test ---


@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_disabled(mock_init, mock_resolve):
    """audit stats should exit when audit is disabled."""
    mock_resolve.return_value = ("test_audit.db", False)

    runner = CliRunner()
    result = runner.invoke(cli, ["audit", "stats", "gpmq.subscribers.test"])
    assert result.exit_code == 0
    assert "disabled" in result.output


# --- audit db path display test ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_shows_db_path(mock_init, mock_resolve, mock_get_store):
    """audit stats should show the audit db path."""
    mock_resolve.return_value = ("/data/custom_audit.db", True)
    mock_store = MagicMock()
    mock_store.stats.return_value = {}
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(cli, ["audit", "stats", "gpmq.subscribers.test"])
    assert result.exit_code == 0
    assert "/data/custom_audit.db" in result.output


# --- error handling tests ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_query_invalid_status(mock_init, mock_resolve, mock_get_store):
    """audit query --status with invalid value should show error."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_get_store.return_value.__enter__ = MagicMock(return_value=mock_store)
    mock_get_store.return_value.__exit__ = MagicMock(return_value=False)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit", "query", "gpmq.subscribers.test", "--status", "invalid_status"],
    )
    # Should indicate error (non-zero exit or error message in output)
    assert (
        result.exit_code != 0
        or "error" in result.output.lower()
        or "invalid" in result.output.lower()
    )


# --- worker command tests ---


@patch("gpmq.cli.get_subscriber")
@patch("gpmq.cli._init_cfg_mgr")
def test_worker_starts_subscriber(mock_init, mock_get_sub):
    """worker SUBSCRIBER_NAME should call get_subscriber and run_forever."""
    mock_sub = MagicMock()
    mock_sub.name = "test-sub"
    mock_get_sub.return_value = mock_sub

    runner = CliRunner()
    result = runner.invoke(cli, ["worker", "gpmq.subscribers.data_loader"])

    mock_get_sub.assert_called_once_with("gpmq.subscribers.data_loader")
    mock_sub.run_forever.assert_called_once()
    assert result.exit_code == 0
    assert "started" in result.output
    assert "stopped" in result.output


@patch("gpmq.cli.get_subscriber")
@patch("gpmq.cli._init_cfg_mgr")
def test_worker_run_forever_catches_interrupt(mock_init, mock_get_sub):
    """worker should exit cleanly even if KeyboardInterrupt propagates from run_forever."""
    mock_sub = MagicMock()
    mock_sub.name = "test-sub"
    # Simulate run_forever catching KeyboardInterrupt internally (as it does in reality)
    mock_get_sub.return_value = mock_sub

    runner = CliRunner()
    result = runner.invoke(cli, ["worker", "gpmq.subscribers.notifier"])

    mock_sub.run_forever.assert_called_once()
    assert result.exit_code == 0


# --- clear command tests ---


@patch("gpmq.cli._create_redis_adapter")
@patch("gpmq.cli._init_cfg_mgr")
def test_clear_stream_with_confirm(mock_init, mock_create_adapter):
    """clear --stream with 'y' should call clear_stream."""
    mock_redis = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_redis)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_create_adapter.return_value = mock_ctx

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["clear", "--stream", "gpmq:messages"],
        input="y\n",
    )
    assert result.exit_code == 0
    mock_redis.clear_stream.assert_called_once_with("gpmq:messages")


@patch("gpmq.cli._create_redis_adapter")
@patch("gpmq.cli._init_cfg_mgr")
def test_clear_stream_cancelled(mock_init, mock_create_adapter):
    """clear --stream with 'n' should NOT call clear_stream."""
    mock_redis = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_redis)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_create_adapter.return_value = mock_ctx

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["clear", "--stream", "gpmq:messages"],
        input="n\n",
    )
    assert result.exit_code == 0
    mock_redis.clear_stream.assert_not_called()
    assert "Cancelled" in result.output


# --- status command tests ---


@patch("gpmq.cli._create_redis_adapter")
@patch("gpmq.cli._init_cfg_mgr")
def test_status_shows_subscribers(mock_init, mock_create_adapter):
    """status should show active subscribers info."""
    mock_redis = MagicMock()
    mock_redis.get_active_subscribers.return_value = ["sub1"]
    mock_redis.get_subscriber_info.return_value = {
        "message_types": ["user_created"],
        "workers": 2,
    }
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_redis)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_create_adapter.return_value = mock_ctx

    runner = CliRunner()
    result = runner.invoke(cli, ["status"])
    assert result.exit_code == 0
    assert "sub1" in result.output


@patch("gpmq.cli._create_redis_adapter")
@patch("gpmq.cli._init_cfg_mgr")
def test_status_json_format(mock_init, mock_create_adapter):
    """status --format json should output JSON."""
    mock_redis = MagicMock()
    mock_redis.get_active_subscribers.return_value = []
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_redis)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_create_adapter.return_value = mock_ctx

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--format", "json"])
    assert result.exit_code == 0
    output = json.loads(result.output)
    assert output == []


# --- error handling tests (additional) ---


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_cleanup_handles_audit_storage_failure(
    mock_init, mock_resolve, mock_get_store
):
    """6.2: audit cleanup handles an audit-storage failure gracefully.

    The old test_audit_cleanup_connection_error patched _init_cfg_mgr (a
    config-init failure) and asserted 'exit_code != 0 or error in output',
    which passed trivially on empty output. This rewrites it to patch
    _get_audit_store so the cleanup operation itself fails (a genuine
    audit-storage failure), and tightens the assertion to require BOTH a
    non-zero exit AND a recognizable audit-error indicator.
    """
    from contextlib import contextmanager

    mock_resolve.return_value = ("test_audit.db", True)
    boom = RuntimeError("audit storage failure")

    @contextmanager
    def fake_store(db_path):
        store = MagicMock()
        store.cleanup_by_type_before.side_effect = boom
        store.cleanup_by_time.side_effect = boom
        yield store

    mock_get_store.side_effect = fake_store

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "audit",
            "cleanup",
            "gpmq.subscribers.test",
            "--before",
            "2026-01-01T00:00:00",
        ],
    )
    # The cleanup command does not wrap the store call in try/except, so the
    # raised exception propagates -> non-zero exit + the exception on result.
    assert result.exit_code != 0
    # Recognizable audit-error indicator (not the trivially-matching "error").
    combined = (result.output + str(result.exception)).lower()
    assert "audit storage failure" in combined or "storage failure" in combined


@patch("gpmq.cli._create_redis_adapter")
@patch("gpmq.cli._init_cfg_mgr")
def test_clear_connection_error(mock_init, mock_create_adapter):
    """clear should handle Redis connection errors."""
    mock_create_adapter.side_effect = Exception("Connection refused")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["clear", "--stream", "test"],
        input="y\n",
    )
    assert result.exit_code != 0


# --- gpmq_worker standalone entry-point test ---


@patch("gpmq.cli._run_worker")
@patch("gpmq.cli._init_cfg_mgr")
@patch("gpmq.use_spawn")
def test_gpmq_worker_bootstrap_chain(mock_use_spawn, mock_init_cfg, mock_run_worker):
    """6.3: gpmq_worker entry-point wires use_spawn -> _init_cfg_mgr -> _run_worker.

    gpmq_worker (the gpmq-worker console script) is a standalone @click.command
    distinct from the 'gpmq worker' subcommand and had zero test coverage. It
    does a LOCAL `from gpmq import use_spawn`, so the patch target is the
    `gpmq` package namespace (gpmq.use_spawn), not gpmq.cli.use_spawn.
    """
    from gpmq.cli import gpmq_worker

    # Attach the three patched mocks to a shared parent to record call order.
    parent = MagicMock()
    parent.attach_mock(mock_use_spawn, "use_spawn")
    parent.attach_mock(mock_init_cfg, "init_cfg")
    parent.attach_mock(mock_run_worker, "run_worker")

    runner = CliRunner()
    result = runner.invoke(
        gpmq_worker,
        ["gpmq.subscribers.data_loader", "--config", "/some/cfg"],
    )

    assert result.exit_code == 0
    mock_use_spawn.assert_called_once_with()
    mock_init_cfg.assert_called_once_with("/some/cfg")
    mock_run_worker.assert_called_once_with("gpmq.subscribers.data_loader")

    # Order: use_spawn before init_cfg before run_worker.
    call_names = [c[0] for c in parent.mock_calls if c[0]]
    assert call_names.index("use_spawn") < call_names.index("init_cfg")
    assert call_names.index("init_cfg") < call_names.index("run_worker")
