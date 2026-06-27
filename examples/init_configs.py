"""Idempotently link examples/configs to the OS-specific cfgs_* directory.

Run this once before using the example demos (demo_client.py,
demo_data_loader_worker.py, demo_notifier_worker.py). Safe to re-run any number
of times — it converges to the same result for a given OS.

使用示例前运行一次本脚本，将 examples/configs 链接到当前操作系统对应的
cfgs_* 目录。脚本可重入：多次运行结果一致。
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

# When run directly (``python examples/init_configs.py``) the script's own
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

CFG_WINDOWS = "cfgs_windows"
CFG_LINUX = "cfgs_linux"


def detect_target_dir() -> str:
    """Return the cfgs_* directory name for the current operating system.

    Returns:
        ``"cfgs_windows"`` on Windows, ``"cfgs_linux"`` on Linux.

    Raises:
        SystemExit: on any other OS (e.g. macOS/Darwin) — no cfgs dir exists
            for it, so we refuse to guess.
    """
    system = platform.system()
    if system == "Windows":
        return CFG_WINDOWS
    if system == "Linux":
        return CFG_LINUX
    raise SystemExit(
        f"[init_configs] unsupported OS '{system}'; only Windows and Linux are "
        f"supported. Add a cfgs_<os> directory to extend support."
    )


def known_cfgs_paths() -> tuple[Path, Path]:
    """Return the resolved absolute paths of ``(cfgs_windows, cfgs_linux)``.

    Raises:
        SystemExit: if either cfgs directory does not exist (examples are
            incomplete / corrupted).
    """
    root = examples_root()
    win = (root / CFG_WINDOWS).resolve()
    linux = (root / CFG_LINUX).resolve()
    missing = [p for p in (win, linux) if not p.is_dir()]
    if missing:
        raise SystemExit(
            "[init_configs] expected config directory not found: "
            + ", ".join(str(p) for p in missing)
            + ". Ensure the examples directory is complete."
        )
    return win, linux


class ConfigsState(NamedTuple):
    """Classification result for the ``configs`` path.

    Attributes:
        kind: one of ``"missing"``, ``"known_target"``, ``"refuse"``.

            * ``missing`` — ``configs`` does not exist and is not a dangling link.
            * ``known_target`` — ``configs`` resolves to (or, if dangling,
              intends) one of our own cfgs directories. The caller decides
              "correct" vs "wrong" by comparing ``actual`` against the
              current-OS target.
            * ``refuse`` — ``configs`` exists but is real user data (a plain
              directory/file, or a link to an unrelated location). Never
              auto-deleted.
        actual: resolved (or intended) absolute path if ``configs`` exists or
            links, else ``None``.
    """

    kind: str
    actual: Path | None


def classify_configs(configs: Path, known: tuple[Path, Path]) -> ConfigsState:
    """Classify the current state of the ``configs`` path.

    Args:
        configs: absolute path to the ``configs`` entry under ``examples/``.
        known: the ``(cfgs_windows, cfgs_linux)`` resolved absolute paths from
            :func:`known_cfgs_paths`.

    Returns:
        A :class:`ConfigsState`. See its docstring for the meaning of ``kind``.

    Reversibility rule (spec §4 authority): a path is auto-removable only if it
    provably points at one of our own cfgs dirs — either via ``resolve()``
    (a live symlink/junction) or, for a dangling link, via its intended
    target string read with :func:`os.readlink` and normalized. Anything else is
    treated as user data and never auto-deleted.

    Link detection uses the OS-robust :func:`is_link` (handles POSIX symlinks
    AND Windows directory junctions, which ``Path.is_symlink`` can miss).
    """
    win, linux = known
    is_link_val = is_link(configs)
    exists = configs.exists()

    if not exists and not is_link_val:
        return ConfigsState("missing", None)

    if exists:
        actual = configs.resolve()
    else:
        # dangling link: inspect its intended target string
        try:
            intended = Path(os.readlink(configs))
        except OSError:
            return ConfigsState("refuse", None)
        if not intended.is_absolute():
            intended = configs.parent / intended
        actual = intended.resolve(strict=False)

    if actual == win or actual == linux:
        return ConfigsState("known_target", actual)

    return ConfigsState("refuse", actual)


def create_link(link: Path, target: Path) -> str:
    """Create a link at ``link`` pointing to directory ``target``.

    On Linux, creates a symlink. On Windows, first tries ``os.symlink`` and,
    if that raises ``OSError`` (symbolic-link privilege required / developer
    mode off), falls back to a directory junction via ``mklink /J``, which
    needs no privilege.

    Args:
        link: path of the link to create (must not already exist).
        target: absolute path of the cfgs directory to link to.

    Returns:
        ``"symlink"`` or ``"junction"`` — which mechanism was used.

    Raises:
        SystemExit: if symlink fails and the junction fallback also fails.
    """
    try:
        os.symlink(target, link)
        return "symlink"
    except OSError as sym_err:
        if platform.system() != "Windows":
            raise SystemExit(
                f"[init_configs] failed to create symlink {link} -> {target}: {sym_err}"
            ) from sym_err
        # Windows: fall back to a directory junction (no privilege needed)
        print(
            "[init_configs] symlink creation failed "
            f"({sym_err}); falling back to directory junction"
        )
        try:
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(link), str(target)],
                check=True,
                capture_output=True,
                # cmd.exe output uses the OEM codepage (often GBK on zh-CN
                # Windows); decode leniently so reading stderr on failure can't
                # itself raise.
                text=True,
                errors="replace",
                encoding="locale",
            )
            return "junction"
        except subprocess.CalledProcessError as jn_err:
            raise SystemExit(
                "[init_configs] could not create symlink (need Developer Mode "
                "or admin) and directory junction fallback also failed.\n"
                f"  mklink stderr: {jn_err.stderr.strip()}\n"
                "  Manual fix: open an admin shell and run: "
                f'mklink /D "{link}" "{target}"'
            ) from jn_err


def main() -> int:
    """Ensure ``examples/configs`` links to the current OS's cfgs_* directory.

    Idempotent: repeat runs converge to the same filesystem state.

    Returns:
        0 on success (no-op or fix), 1 on any refusal/error.
    """
    root = examples_root()
    target_name = detect_target_dir()
    desired = (root / target_name).resolve()
    win, linux = known_cfgs_paths()  # also validates both cfgs dirs exist
    configs = root / CONFIGS_NAME

    print(f"[init_configs] examples root: {root}")
    print(f"[init_configs] detected OS: {platform.system()} -> target {target_name}")

    state = classify_configs(configs, (win, linux))

    if state.kind == "missing":
        print(f"[init_configs] '{CONFIGS_NAME}' does not exist — creating link")
        method = create_link(configs, desired)
        print(f"[init_configs] created {method}: {CONFIGS_NAME} -> {target_name}")
        return 0

    if state.kind == "known_target":
        if state.actual == desired:
            print(
                f"[init_configs] '{CONFIGS_NAME}' resolves to "
                f"'{target_name}' (correct) — nothing to do"
            )
            return 0
        # points at the OTHER cfgs dir -> wrong for this OS -> relink
        wrong_name = CFG_LINUX if target_name == CFG_WINDOWS else CFG_WINDOWS
        print(
            f"[init_configs] '{CONFIGS_NAME}' resolves to '{wrong_name}' "
            f"(wrong for this OS) — relinking"
        )
        remove_link(configs)
        method = create_link(configs, desired)
        print(f"[init_configs] created {method}: {CONFIGS_NAME} -> {target_name}")
        return 0

    # state.kind == "refuse"
    print(
        f"[init_configs] ERROR: '{CONFIGS_NAME}' exists and resolves to "
        f"'{state.actual}', which is not a known cfgs_* target. "
        "Refusing to delete it automatically.\n"
        "[init_configs] To proceed, remove or rename it manually:\n"
        f'    Windows: rmdir /S /Q "{CONFIGS_NAME}"\n'
        f"    Linux:   rm -rf {CONFIGS_NAME}"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
