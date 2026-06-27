"""Shared helpers for the examples config-link scripts.

Used by init_configs.py and unlink_configs.py. Holds the OS-robust link
detection (POSIX symlink + Windows directory junction), the path computation,
and the link-removal fallback chain so both scripts stay in sync via a single
source of truth.

init_configs.py 与 unlink_configs.py 共用的工具函数：操作系统无关的链接检测
（POSIX 符号链接 + Windows 目录联接）、examples 根路径计算，以及删除链接的
兜底链。两脚本通过此单一事实源保持一致。
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Name of the link under examples/ that points at the active cfgs_* directory.
CONFIGS_NAME = "configs"

# FILE_ATTRIBUTE_REPARSE_POINT — marks a path as a reparse point (junctions,
# mounted volumes, etc.) on Windows. Junctions are NOT reported by os.path.islink.
_REPARSE_POINT = 0x400


def examples_root() -> Path:
    """Absolute path to the ``examples/`` directory (cwd-independent).

    Resolved from this module's own ``__file__`` so both scripts work regardless
    of the current working directory they are launched from.
    """
    return Path(__file__).resolve().parent


def is_link(path: Path) -> bool:
    """Return True if ``path`` is a symlink or a Windows directory junction.

    Uses the OS-appropriate interface:
      - Any OS: ``os.path.islink`` catches POSIX symlinks.
      - Windows: directory junctions are reparse points that
        ``os.path.islink`` can miss (reports False on many builds), so we also
        check the ``FILE_ATTRIBUTE_REPARSE_POINT`` bit via ``os.lstat``.

    Args:
        path: path to test.

    Returns:
        True if ``path`` is a link/junction, False otherwise (including when
        the path does not exist, or is a real directory/file).
    """
    if os.path.islink(path):
        return True
    if sys.platform == "win32":
        try:
            attrs = os.lstat(path).st_file_attributes
        except (OSError, AttributeError):
            return False
        return bool(attrs & _REPARSE_POINT)
    return False


def remove_link(link: Path) -> None:
    """Remove a symlink or directory junction at ``link``.

    Tries the least-assumption method first and falls back for cross-version
    robustness (junctions: ``os.unlink`` works on Python >= 3.12; older
    versions need ``os.rmdir``). Only call this after :func:`is_link` (or an
    equivalent reversibility check) has confirmed the path is actually a link.

    Args:
        link: path to the link to remove.

    Raises:
        OSError: if all removal attempts fail (propagated to caller -> error).
    """
    # 1. unlink — works for symlinks (all versions) and junctions (>= 3.12)
    try:
        os.unlink(link)
        return
    except (IsADirectoryError, PermissionError):
        pass
    # 2. rmdir — older Python junction / empty-dir link
    try:
        os.rmdir(link)
        return
    except OSError:
        pass
    # 3. rmtree — last resort for a reparse-point dir that resists the above
    shutil.rmtree(link, ignore_errors=False)
