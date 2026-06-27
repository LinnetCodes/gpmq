from __future__ import annotations

import os
import platform
import subprocess
import sys

import pytest

import examples.init_configs as ic


def _make_dir_link(link, target):
    """Create a directory link at ``link`` pointing to ``target``.

    Works without privilege on both Windows (directory junction) and POSIX
    (symlink). Used by tests because the CI/dev host may lack symlink
    privilege (Windows WinError 1314).
    """
    if sys.platform == "win32":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )
    else:
        os.symlink(target, link, target_is_directory=True)


def test_detect_target_dir_windows(monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    assert ic.detect_target_dir() == "cfgs_windows"


def test_detect_target_dir_linux(monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    assert ic.detect_target_dir() == "cfgs_linux"


def test_known_cfgs_paths(tmp_path, monkeypatch):
    # build a fake examples root with the two cfgs dirs
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)

    win, linux = ic.known_cfgs_paths()

    assert win == (tmp_path / "cfgs_windows").resolve()
    assert linux == (tmp_path / "cfgs_linux").resolve()


def test_known_cfgs_paths_missing_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    # neither cfgs dir exists
    try:
        ic.known_cfgs_paths()
    except SystemExit:
        return
    raise AssertionError("expected SystemExit when cfgs dirs are missing")


def test_classify_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    configs = tmp_path / "configs"  # does not exist
    state = ic.classify_configs(configs, ic.known_cfgs_paths())
    assert state.kind == "missing"
    assert state.actual is None


def test_classify_correct(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    win = tmp_path / "cfgs_windows"
    win.mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    _make_dir_link(tmp_path / "configs", win)
    state = ic.classify_configs(tmp_path / "configs", ic.known_cfgs_paths())
    assert state.kind == "known_target"
    assert state.actual == win.resolve()


def test_classify_wrong_target(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    (tmp_path / "cfgs_windows").mkdir()
    linux = tmp_path / "cfgs_linux"
    linux.mkdir()
    _make_dir_link(tmp_path / "configs", linux)
    state = ic.classify_configs(tmp_path / "configs", ic.known_cfgs_paths())
    assert state.kind == "known_target"
    assert state.actual == linux.resolve()


def test_classify_refuse_real_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    (tmp_path / "configs").mkdir()  # a plain directory, not a link
    (tmp_path / "configs" / "user_file.txt").write_text("data")
    state = ic.classify_configs(tmp_path / "configs", ic.known_cfgs_paths())
    assert state.kind == "refuse"


def test_classify_refuse_file(tmp_path, monkeypatch):
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    (tmp_path / "configs").write_text("not a link")  # a plain file
    state = ic.classify_configs(tmp_path / "configs", ic.known_cfgs_paths())
    assert state.kind == "refuse"


def test_classify_handles_junction_via_robust_is_link(tmp_path, monkeypatch):
    """Regression: classify_configs must recognize a Windows directory junction
    (which Path.is_symlink reports as False on many builds) as known_target.

    This is the whole reason init_configs.py now uses the shared is_link() from
    _link_utils instead of configs.is_symlink(). On POSIX this still creates a
    symlink, so the assertion holds on both platforms.
    """
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    win = tmp_path / "cfgs_windows"
    win.mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    _make_dir_link(tmp_path / "configs", win)  # junction on Windows
    # sanity: confirm the detection that previously could fail on Windows
    assert ic.is_link(tmp_path / "configs") is True
    state = ic.classify_configs(tmp_path / "configs", ic.known_cfgs_paths())
    assert state.kind == "known_target"
    assert state.actual == win.resolve()


def test_classify_dangling_to_known_cfgs(tmp_path, monkeypatch):
    """A dangling symlink whose intended target normalizes to a known cfgs path.

    Skipped on Windows hosts without symlink privilege (can't create a symlink
    to set up the dangling case).
    """
    win = tmp_path / "cfgs_windows"
    linux = tmp_path / "cfgs_linux"
    # do NOT create the dirs — link target will dangle
    try:
        os.symlink(win, tmp_path / "configs", target_is_directory=True)
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")
    state = ic.classify_configs(tmp_path / "configs", (win, linux))
    assert state.kind == "known_target"
    assert state.actual == win


def test_classify_dangling_to_unknown_path(tmp_path, monkeypatch):
    """A dangling symlink to a path that is NOT a known cfgs dir -> refuse.

    Skipped on Windows hosts without symlink privilege.
    """
    win = tmp_path / "cfgs_windows"
    linux = tmp_path / "cfgs_linux"
    try:
        os.symlink(
            tmp_path / "some_unrelated_dir",
            tmp_path / "configs",
            target_is_directory=True,
        )
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")
    state = ic.classify_configs(tmp_path / "configs", (win, linux))
    assert state.kind == "refuse"


def test_remove_link_junction_or_symlink(tmp_path):
    """remove_link deletes a live directory link (junction on Windows)."""
    target = tmp_path / "real_cfgs"
    target.mkdir()
    link = tmp_path / "configs"
    _make_dir_link(link, target)
    assert link.exists()  # sanity: link is live
    ic.remove_link(link)
    assert not link.exists() and not link.is_symlink()


def test_remove_link_dangling(tmp_path):
    """remove_link deletes a dangling symlink.

    Skipped on hosts without symlink privilege.
    """
    link = tmp_path / "configs"
    try:
        os.symlink(tmp_path / "does_not_exist", link, target_is_directory=True)
    except OSError:
        pytest.skip("cannot create symlink on this host (no privilege)")
    ic.remove_link(link)
    assert not link.is_symlink()


def test_create_link_creates_resolvable_link(tmp_path):
    """create_link produces a link whose resolve() == the target.

    On hosts with symlink privilege this is a symlink; otherwise it is a
    Windows directory junction. Either way resolve() must hit the target.
    """
    target = tmp_path / "cfgs_windows"
    target.mkdir()
    link = tmp_path / "configs"
    method = ic.create_link(link, target)
    assert method in ("symlink", "junction")
    assert link.resolve() == target.resolve()


def test_create_link_junction_fallback_on_symlink_failure(tmp_path, monkeypatch):
    """When os.symlink raises OSError on Windows, fall back to a junction.

    The junction fallback shells out to ``cmd /c mklink /J``, which only exists
    on real Windows. To keep the test portable across all hosts (Linux/WSL/macOS
    AND Windows with/without symlink privilege), we spy on ``subprocess.run``:
    assert it dispatches the correct ``mklink /J`` command, then stand up a real
    link via the host-appropriate mechanism so ``link.resolve() ==
    target.resolve()`` still holds.

    Note: on Windows the spy must NOT just call ``os.symlink`` — the scenario
    under test is precisely "no symlink privilege", so the spy's link creation
    falls back to a real ``mklink /J`` (junctions need no privilege), mirroring
    ``_make_dir_link`` above.
    """
    target = tmp_path / "cfgs_windows"
    target.mkdir()
    link = tmp_path / "configs"

    # Capture the real symlink & subprocess.run before patching; the spy uses
    # them to create the link that mklink /J would have produced on Windows.
    real_symlink = os.symlink
    real_run = subprocess.run

    def _fail_symlink(*args, **kwargs):
        raise OSError("symbolic link privilege not held")

    def _make_host_link():
        """Create a dir link at ``link`` -> ``target`` using whatever the host
        actually supports (symlink on POSIX / privileged Windows, mklink /J on
        privilege-less Windows)."""
        try:
            real_symlink(target, link, target_is_directory=True)
        except OSError:
            real_run(
                ["cmd", "/c", "mklink", "/J", str(link), str(target)],
                check=True,
                capture_output=True,
            )

    def _fake_mklink(args, **kwargs):
        assert args == ["cmd", "/c", "mklink", "/J", str(link), str(target)], args
        _make_host_link()
        return subprocess.CompletedProcess(args=args, returncode=0)

    monkeypatch.setattr(os, "symlink", _fail_symlink)
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    monkeypatch.setattr(subprocess, "run", _fake_mklink)

    method = ic.create_link(link, target)
    assert method == "junction"
    assert link.resolve() == target.resolve()


def test_main_creates_link_when_missing(tmp_path, monkeypatch):
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    rc = ic.main()
    configs = tmp_path / "configs"
    assert configs.resolve() == (tmp_path / "cfgs_windows").resolve()
    assert rc == 0


def test_main_idempotent_second_run(tmp_path, monkeypatch):
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    ic.main()
    rc2 = ic.main()  # second run must be a no-op
    assert rc2 == 0
    assert (tmp_path / "configs").resolve() == (tmp_path / "cfgs_windows").resolve()


def test_main_relinks_wrong_target(tmp_path, monkeypatch):
    (tmp_path / "cfgs_windows").mkdir()
    linux = tmp_path / "cfgs_linux"
    linux.mkdir()
    # pre-link configs to the WRONG target (cfgs_linux) for this OS (Windows)
    _make_dir_link(tmp_path / "configs", linux)
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    rc = ic.main()
    assert rc == 0
    assert (tmp_path / "configs").resolve() == (tmp_path / "cfgs_windows").resolve()


def test_main_refuses_real_dir(tmp_path, monkeypatch):
    (tmp_path / "cfgs_windows").mkdir()
    (tmp_path / "cfgs_linux").mkdir()
    (tmp_path / "configs").mkdir()  # real dir, not a link
    monkeypatch.setattr(ic, "examples_root", lambda: tmp_path)
    monkeypatch.setattr(platform, "system", lambda: "Windows")
    rc = ic.main()
    assert rc == 1
    # the real dir is still intact
    assert (tmp_path / "configs").is_dir()
