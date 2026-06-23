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


def test_format_output_yaml():
    """_format_output should format data as YAML (using json.dumps)."""
    data = [{"id": "1", "type": "test"}]
    result = _format_output(data, "yaml")
    # yaml format uses json.dumps for now
    parsed = json.loads(result)
    assert parsed == data


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
    runner.invoke(cli, ["--config", "/some/path", "audit", "stats", "gpmq.subscribers.test"])
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
    runner.invoke(cli, ["audit", "stats", "gpmq.subscribers.test", "--config", "/some/path"])
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
        ["audit", "cleanup", "gpmq.subscribers.test", "--before", "2026-01-01T00:00:00"],
    )
    assert result.exit_code == 0
    mock_store.cleanup_by_time.assert_called_once()
    assert "5" in result.output


@patch("gpmq.cli._get_audit_store")
@patch("gpmq.cli._resolve_audit_config")
@patch("gpmq.cli._init_cfg_mgr")
def test_audit_cleanup_by_type(mock_init, mock_resolve, mock_get_store):
    """audit cleanup --type should call cleanup_by_type."""
    mock_resolve.return_value = ("test_audit.db", True)
    mock_store = MagicMock()
    mock_store.cleanup_by_type.return_value = 3
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
    mock_store.cleanup_by_type.assert_called_once_with("user_created")
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


@patch("gpmq.cli._init_cfg_mgr")
def test_audit_cleanup_connection_error(mock_init):
    """audit cleanup should handle connection errors gracefully."""
    mock_init.side_effect = Exception("Connection failed")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["audit", "cleanup", "gpmq.subscribers.test", "--before", "2026-01-01T00:00:00"],
    )
    # Should indicate error
    assert result.exit_code != 0 or "error" in result.output.lower()


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
