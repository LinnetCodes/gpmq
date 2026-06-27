from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import examples._link_utils as lu


def _make_dir_link(link: Path, target: Path) -> None:
    """Create a directory link: Windows junction (no privilege) or POSIX symlink."""
    if sys.platform == "win32":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )
    else:
        os.symlink(target, link, target_is_directory=True)


def test_examples_root_is_examples_dir():
    root = lu.examples_root()
    assert root.is_dir()
    # this module lives under tests/examples/, so the shared util's __file__
    # is directly under examples/
    assert root.name == "examples"
    assert (root / "init_configs.py").exists()
    assert (root / "unlink_configs.py").exists()


def test_configs_name_constant():
    assert lu.CONFIGS_NAME == "configs"


def test_is_link_true_for_junction_or_symlink(tmp_path):
    target = tmp_path / "tgt"
    target.mkdir()
    link = tmp_path / "link"
    _make_dir_link(link, target)
    assert lu.is_link(link) is True


def test_is_link_false_for_real_dir(tmp_path):
    real = tmp_path / "realdir"
    real.mkdir()
    assert lu.is_link(real) is False


def test_is_link_false_for_real_file(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("data")
    assert lu.is_link(f) is False


def test_is_link_false_for_missing_path(tmp_path):
    assert lu.is_link(tmp_path / "nope") is False


def test_is_link_true_for_dangling_symlink(tmp_path):
    link = tmp_path / "dangling"
    try:
        os.symlink(tmp_path / "nope", link, target_is_directory=True)
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")
    assert lu.is_link(link) is True


def test_remove_link_deletes_junction_or_symlink(tmp_path):
    target = tmp_path / "tgt"
    target.mkdir()
    link = tmp_path / "link"
    _make_dir_link(link, target)
    lu.remove_link(link)
    assert not lu.is_link(link)
    assert not link.exists()


def test_remove_link_deletes_dangling_symlink(tmp_path):
    link = tmp_path / "dangling"
    try:
        os.symlink(tmp_path / "nope", link, target_is_directory=True)
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")
    lu.remove_link(link)
    assert not lu.is_link(link)
