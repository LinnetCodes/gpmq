"""Packaging checks (PEP 561 type-marker shipping)."""

from pathlib import Path

import gpmq


def test_ships_py_typed_marker():
    """The gpmq package must ship a ``py.typed`` marker (PEP 561).

    Without the marker, downstream type checkers (e.g. ``mypy --strict``)
    treat every gpmq API as ``Any`` instead of using the inline annotations.
    """
    package_dir = Path(gpmq.__file__).parent
    assert (package_dir / "py.typed").is_file()
