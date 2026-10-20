"""Tests for gpmq.log get_logger."""

import pytest
from gpconfig import ConfigNotFoundError


@pytest.fixture(autouse=True)
def _reset_log_globals():
    """Reset log module globals between tests."""
    from gpmq import log

    log._lookup_done = False
    log._logger_cfg_folder_obj = None
    yield
    log._lookup_done = False
    log._logger_cfg_folder_obj = None


def test_lookup_happens_once_when_config_missing():
    """4.4: when 'logs' config is missing, the lookup+warning happen only on the first call."""
    from unittest.mock import MagicMock, patch
    from gpmq import log
    from gpconfig import GPConfigManager

    cfg_mgr = MagicMock(spec=GPConfigManager)
    cfg_mgr.get_config.side_effect = ConfigNotFoundError("logs")

    with patch("gpmq.get_config_manager", return_value=cfg_mgr):
        log.get_logger("gpmq")
        log.get_logger("gpmq")  # second call should NOT re-lookup

    # The lookup happened exactly once across two get_logger calls.
    assert cfg_mgr.get_config.call_count == 1


def test_lookup_skipped_after_success():
    """4.4: after a successful lookup, subsequent calls skip it."""
    from unittest.mock import MagicMock, patch
    from gpmq import log
    from gpconfig import GPConfigManager

    cfg_mgr = MagicMock(spec=GPConfigManager)
    cfg_mgr.get_config.return_value = MagicMock()  # success

    with patch("gpmq.get_config_manager", return_value=cfg_mgr):
        log.get_logger("gpmq")
        log.get_logger("gpmq")

    assert cfg_mgr.get_config.call_count == 1


def test_lookup_skipped_when_no_config_manager():
    """4.4: with no config manager set (None), get_logger never attempts a lookup."""
    from unittest.mock import patch
    from gpmq import log

    with patch("gpmq.get_config_manager", return_value=None):
        log.get_logger("gpmq")
        log.get_logger("gpmq")

    # get_config is never called because the cfg_mgr is not a GPConfigManager.
    # Nothing to assert on a real object here beyond no exception; the key
    # property is covered by the other two tests.
    assert log._lookup_done is True
