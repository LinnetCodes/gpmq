"""Unconditionally remove the examples/configs link, regardless of target.

Companion to init_configs.py. This removes the ``configs`` link (POSIX symlink
or Windows directory junction) without checking where it points — useful for
resetting the examples before re-linking to a different OS's cfgs directory,
or for cleaning up before switching branches.

Safety: it only removes ``configs`` when it is provably a *link* (symlink or
junction). If ``configs`` is a real directory or file (i.e. user data, not a
link we or init_configs.py could have created), it refuses to delete and exits
non-zero — same conservative policy as init_configs.py.

Linux/Windows: link detection uses the appropriate interface per OS (see
``is_link`` in _link_utils.py).

解除 examples/configs 的链接（不论其指向哪个目标）。与 init_configs.py 配套使用。
仅当 configs 是链接（符号链接或目录联接）时才会删除；若它是真实目录/文件则拒绝
删除并报错退出，避免误删用户数据。
"""

from __future__ import annotations

import sys
from pathlib import Path

# When run directly (``python examples/unlink_configs.py``) the script's own
# directory is on sys.path but the repo root (where the ``examples`` package
# lives) is not, so ``from examples._link_utils import ...`` would fail. Add the
# repo root (parent of this file's directory) before the sibling import.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from examples._link_utils import (  # noqa: E402
    CONFIGS_NAME,
    examples_root,
    is_link,
    remove_link,
)

__all__ = ["main"]


def main() -> int:
    """Unconditionally remove the ``examples/configs`` link.

    Removes ``configs`` if it is a symlink or Windows directory junction
    (regardless of where it points). If it is a real directory/file (i.e. user
    data), refuses to delete and exits non-zero. If it does not exist, exits 0.

    Returns:
        0 on success (removed, or already absent), 1 if ``configs`` exists but
        is not a link (refused to avoid destroying user data).
    """
    configs = examples_root() / CONFIGS_NAME

    print(f"[unlink_configs] examples root: {examples_root()}")

    if not configs.exists() and not is_link(configs):
        print(f"[unlink_configs] '{CONFIGS_NAME}' does not exist — nothing to do")
        return 0

    if not is_link(configs):
        # exists but is a real directory/file -> user data, do not touch
        print(
            f"[unlink_configs] ERROR: '{CONFIGS_NAME}' exists but is not a link "
            f"(real directory or file). Refusing to delete user data.\n"
            "[unlink_configs] To remove it manually:\n"
            f'    Windows: rmdir /S /Q "{CONFIGS_NAME}"   (dir)  |  del "{CONFIGS_NAME}"   (file)\n'
            f"    Linux:   rm -rf {CONFIGS_NAME}"
        )
        return 1

    print(f"[unlink_configs] removing link '{CONFIGS_NAME}'")
    remove_link(configs)
    print(f"[unlink_configs] removed '{CONFIGS_NAME}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
