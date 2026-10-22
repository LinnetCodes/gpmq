"""Logging configuration for GPMQ using gpclog.

Provides a centralized logging interface for all GPMQ components.
If :func:`gpmq.set_config_manager()` is called before getting a logger, gpclog
will attempt to load logger configurations from YAML files in 'logs' folder
of config manager. Otherwise, gpclog uses its default settings.
"""

from __future__ import annotations
from typing import Any, Optional
from gpconfig import GPConfigFolder, GPConfigManager, ConfigNotFoundError
import gpclog


_logger_cfg_folder_obj: GPConfigFolder | None = None
_lookup_done: bool = False


def get_logger(name: str = "gpmq", sn: Optional[int] = None) -> Any:
    """Get a logger instance via gpclog.

    If :func:`gpmq.set_config_manager()` was called, and a config manager was set,
    gpmq will try loading gpclog configuration from 'logs' folder of the
    config manager folder, Otherwise, it falls back to gpclog's default
    settings.

    The 'logs' folder lookup is attempted at most once per process: the
    first call resolves it (and emits a warning on miss), then every
    subsequent call reuses that outcome without repeating the lookup.

    Args:
        name: Logger / category name.  Defaults to ``"gpmq"``.
        sn: serial number of the logger instance with sn extension. Defaults to ``None``.
    Returns:
        A gpclog logger instance.
    """
    from gpmq import get_config_manager

    global _logger_cfg_folder_obj, _lookup_done

    _cfg_mgr = get_config_manager()

    if not _lookup_done:
        if isinstance(_cfg_mgr, GPConfigManager):
            # get and setup logger config folder if a config manager is available,
            # then a logger can be configured automatically by YAML config file
            try:
                folder = _cfg_mgr.get_config("logs")
            except ConfigNotFoundError:
                folder = None
            # Only a real GPConfigFolder may be installed into gpclog's
            # process-global state. Anything else (a raw dict when 'logs' is
            # an unregistered yaml file, or a MagicMock from a spec-mock
            # manager in tests) would break every later gpclog.get_logger()
            # call with an uncached name.
            if isinstance(folder, GPConfigFolder):
                _logger_cfg_folder_obj = folder
                gpclog.set_config_folder(folder)
            else:
                _logger = gpclog.get_logger(name, sn)
                _logger.warning("No logger configuration folder 'logs' found")
        _lookup_done = True

    return gpclog.get_logger(name, sn)
