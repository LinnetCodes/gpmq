from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import examples.unlink_configs as uc


def _make_dir_link(link: Path, target: Path) -> None:
    """Create a directory link at ``link`` -> ``target``.

    Uses a Windows directory junction (no privilege needed) on win32, else a
    POSIX symlink. Mirrors the host capabilities used by init_configs.py.
    """
    if sys.platform == "win32":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )
    else:
        os.symlink(target, link, target_is_directory=True)


def _make_dangling_symlink(link: Path, target: Path) -> None:
    """Create a dangling symlink at ``link`` -> nonexistent ``target``."""
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")


# ── is_link() detection ──────────────────────────────────────────────────────


def test_is_link_true_for_junction_or_symlink(tmp_path):
    target = tmp_path / "tgt"
    target.mkdir()
    link = tmp_path / "link"
    _make_dir_link(link, target)
    assert uc.is_link(link) is True


def test_is_link_false_for_real_dir(tmp_path):
    real = tmp_path / "realdir"
    real.mkdir()
    assert uc.is_link(real) is False


def test_is_link_false_for_real_file(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("data")
    assert uc.is_link(f) is False


def test_is_link_false_for_missing(tmp_path):
    assert uc.is_link(tmp_path / "nope") is False


def test_is_link_true_for_dangling_symlink(tmp_path):
    link = tmp_path / "dangling"
    _make_dangling_symlink(link, tmp_path / "nope")
    # only asserted when the symlink could actually be created
    assert uc.is_link(link) is True


# ── main() behavior ──────────────────────────────────────────────────────────


def _run_main_in_sandbox(tmp_path, monkeypatch):
    """Point unlink_configs at tmp_path as its examples root and run main()."""
    monkeypatch.setattr(uc, "examples_root", lambda: tmp_path)
    return uc.main()


def test_main_removes_link(tmp_path, monkeypatch):
    target = tmp_path / "cfgs_windows"
    target.mkdir()
    configs = tmp_path / "configs"
    _make_dir_link(configs, target)
    assert configs.exists()  # sanity

    rc = _run_main_in_sandbox(tmp_path, monkeypatch)

    assert rc == 0
    assert not configs.exists()
    assert not uc.is_link(configs)


def test_main_refuses_real_dir(tmp_path, monkeypatch):
    configs = tmp_path / "configs"
    configs.mkdir()
    (configs / "user_file.txt").write_text("important data")

    rc = _run_main_in_sandbox(tmp_path, monkeypatch)

    assert rc == 1
    # real dir + its contents must be untouched
    assert configs.is_dir()
    assert (configs / "user_file.txt").read_text() == "important data"


def test_main_refuses_real_file(tmp_path, monkeypatch):
    configs = tmp_path / "configs"
    configs.write_text("not a link")

    rc = _run_main_in_sandbox(tmp_path, monkeypatch)

    assert rc == 1
    assert configs.is_file()
    assert configs.read_text() == "not a link"


def test_main_missing_is_noop(tmp_path, monkeypatch):
    configs = tmp_path / "configs"  # does not exist

    rc = _run_main_in_sandbox(tmp_path, monkeypatch)

    assert rc == 0
    assert not configs.exists()


def test_main_idempotent_second_run(tmp_path, monkeypatch):
    target = tmp_path / "cfgs_windows"
    target.mkdir()
    configs = tmp_path / "configs"
    _make_dir_link(configs, target)

    rc1 = _run_main_in_sandbox(tmp_path, monkeypatch)
    rc2 = _run_main_in_sandbox(tmp_path, monkeypatch)  # now missing

    assert rc1 == 0
    assert rc2 == 0  # missing -> no-op, still success
    assert not configs.exists()
