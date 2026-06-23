"""Logging configuration for GPMQ using gpclog.

Provides a centralized logging interface for all GPMQ components.
If :func:`gpmq.set_config_manager()` is called before getting a logger, gpclog
will attempt to load logger configurations from YAML files in 'logs' folder
of config manager. Otherwise, gpclog uses its default settings (stdout only).
"""

from __future__ import annotations
from typing import Optional
from gpconfig import GPConfigFolder, GPConfigManager, ConfigNotFoundError
import gpclog


_logger_cfg_folder_obj: GPConfigFolder | None = None


def get_logger(name: str = "gpmq", sn: Optional[int] = None):
    """Get a logger instance via gpclog.

    If :func:`gpmq.set_config_manager()` was called, and a config manager was set,
    gpmq will try loading gpclog configuration from 'logs' folder of the
    config manager folder, Otherwise it falls back to default behavior
    (stdout only).

    Args:
        name: Logger / category name.  Defaults to ``"gpmq"``.
        sn: serial number of the logger instance with sn extension. Defaults to ``None``.
    Returns:
        A gpclog logger instance.
    """
    from gpmq import get_config_manager
    global _logger_cfg_folder_obj

    _cfg_mgr = get_config_manager()

    if _logger_cfg_folder_obj is None and isinstance(_cfg_mgr, GPConfigManager):
        # get and setup logger config folder if a config manager is available,
        # then a logger can be configured automatically by YAML config file
        try:
            _logger_cfg_folder_obj = _cfg_mgr.get_config("logs")
            gpclog.set_config_folder(_logger_cfg_folder_obj)
        except ConfigNotFoundError:
            _logger = gpclog.get_logger(name, sn)
            _logger.warning("No logger configuration folder 'logs' found")

    return gpclog.get_logger(name, sn)
