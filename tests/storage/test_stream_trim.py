"""Unit tests for RedisAdapter.trim_stream_to_registered_progress."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
import redis as redis_lib

from gpmq.config import GPMQConfig
from gpmq.storage.redis_adapter import RedisAdapter

STREAM = "gpmq:messages"


@pytest.fixture
def adapter(redis_config: GPMQConfig) -> RedisAdapter:
    """Unconnected adapter; the trim algorithm's primitives are shadowed."""
    return RedisAdapter(redis_config)


def _shadow(
    adapter: RedisAdapter,
    names: list[str],
    groups: list[dict[str, Any]],
    pending: dict[str, dict[str, Any]] | None = None,
    infos: dict[str, dict[str, Any]] | None = None,
    trimmed: int = 0,
) -> list[str]:
    """Shadow the adapter's Redis primitives with deterministic fakes.

    Returns the list of minid values passed to xtrim_minid.
    """
    pending = pending or {}
    infos = infos if infos is not None else {n: {"stream_name": STREAM} for n in names}
    adapter.get_registered_subscribers = lambda: list(names)  # type: ignore[method-assign]
    adapter.get_subscriber_info = lambda n: infos.get(n, {})  # type: ignore[method-assign]
    adapter.xinfo_groups = lambda _s: list(groups)  # type: ignore[method-assign]
    adapter.xpending = lambda _s, g: pending.get(  # type: ignore[method-assign]
        g, {"pending": 0, "min": None}
    )
    xtrim_calls: list[str] = []

    def _xtrim(_stream: str, minid: str, approximate: bool = True) -> int:
        xtrim_calls.append(minid)
        return trimmed

    adapter.xtrim_minid = _xtrim  # type: ignore[method-assign]
    return xtrim_calls


def _logged(log: list[str], name: str, inner: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a shadowed adapter method so every call appends name to log."""

    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        log.append(name)
        return inner(*args, **kwargs)

    return _wrapper


def test_trims_to_min_group_cursor(adapter):
    """Safe point is the minimum cursor across registered groups."""
    calls = _shadow(
        adapter,
        names=["a", "b"],
        groups=[
            {"name": "a", "last-delivered-id": "100-0", "pending": 0},
            {"name": "b", "last-delivered-id": "80-0", "pending": 0},
        ],
        trimmed=5,
    )
    assert adapter.trim_stream_to_registered_progress() == 5
    assert calls == ["80-0"]


def test_pel_min_blocks_below_it(adapter):
    """A pending (unacked) entry lowers the safe point to its ID."""
    calls = _shadow(
        adapter,
        names=["a"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 1}],
        pending={"a": {"pending": 1, "min": "60-3"}},
        trimmed=2,
    )
    assert adapter.trim_stream_to_registered_progress() == 2
    assert calls == ["60-3"]


def test_pending_without_min_counts_as_nothing_pending(adapter):
    """A PEL summary with pending > 0 but min None (pathological) counts as
    nothing pending: no crash, and the cursor alone sets the safe point."""
    calls = _shadow(
        adapter,
        names=["a"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 2}],
        pending={"a": {"pending": 2, "min": None}},
        trimmed=3,
    )
    assert adapter.trim_stream_to_registered_progress() == 3
    assert calls == ["100-0"]


def test_numeric_id_comparison_not_lexicographic(adapter):
    """ "999-0" must sort below "1000-0" (lexicographic order is wrong)."""
    calls = _shadow(
        adapter,
        names=["a", "b"],
        groups=[
            {"name": "a", "last-delivered-id": "999-0", "pending": 0},
            {"name": "b", "last-delivered-id": "1000-0", "pending": 0},
        ],
        trimmed=1,
    )
    adapter.trim_stream_to_registered_progress()
    assert calls == ["999-0"]


def test_no_registered_subscribers_skips(adapter):
    """Empty registry: no trim (publisher-side idle cap covers that window)."""
    calls = _shadow(adapter, names=[], groups=[], trimmed=9)
    assert adapter.trim_stream_to_registered_progress() == 0
    assert calls == []


def test_registered_without_group_blocks(adapter):
    """A registered subscriber whose group is missing blocks the whole trim."""
    calls = _shadow(
        adapter,
        names=["a", "ghost"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 0}],
        trimmed=9,
    )
    assert adapter.trim_stream_to_registered_progress() == 0
    assert calls == []


def test_zero_cursor_skips(adapter):
    """A group still at 0-0 has delivered nothing: keep everything."""
    calls = _shadow(
        adapter,
        names=["a"],
        groups=[{"name": "a", "last-delivered-id": "0-0", "pending": 0}],
        trimmed=9,
    )
    assert adapter.trim_stream_to_registered_progress() == 0
    assert calls == []


def test_unregistered_group_ignored(adapter):
    """Leftover groups of unregistered subscribers do not block the trim."""
    calls = _shadow(
        adapter,
        names=["a"],
        groups=[
            {"name": "a", "last-delivered-id": "100-0", "pending": 0},
            {"name": "leftover", "last-delivered-id": "5-0", "pending": 3},
        ],
        pending={"leftover": {"pending": 3, "min": "5-0"}},
        trimmed=4,
    )
    assert adapter.trim_stream_to_registered_progress() == 4
    assert calls == ["100-0"]


def test_subscriber_on_other_stream_ignored(adapter):
    """Registered subscribers of a different stream_name don't block."""
    calls = _shadow(
        adapter,
        names=["a", "other"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 0}],
        infos={
            "a": {"stream_name": STREAM},
            "other": {"stream_name": "another:stream"},
        },
        trimmed=7,
    )
    assert adapter.trim_stream_to_registered_progress() == 7
    assert calls == ["100-0"]


def test_missing_stream_returns_zero(adapter):
    """A missing stream (ResponseError) is not an error: nothing to trim."""
    adapter.get_registered_subscribers = lambda: ["a"]  # type: ignore[method-assign]
    adapter.get_subscriber_info = lambda _n: {"stream_name": STREAM}  # type: ignore[method-assign]

    def _boom(_s: str) -> list[dict[str, Any]]:
        raise redis_lib.ResponseError("no such key")

    adapter.xinfo_groups = _boom  # type: ignore[method-assign]
    assert adapter.trim_stream_to_registered_progress() == 0


@pytest.mark.parametrize(
    "error",
    [
        redis_lib.ResponseError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        ),
        redis_lib.exceptions.NoPermissionError(
            "User default has no permissions to run the 'xinfo|groups' command"
        ),
    ],
    ids=["wrongtype", "noperm"],
)
def test_other_xinfo_errors_propagate(adapter, error):
    """Only a missing stream is benign. Any other XINFO GROUPS error (e.g.
    WRONGTYPE, NOPERM) must surface instead of silently disabling trimming."""
    calls = _shadow(adapter, names=["a"], groups=[], trimmed=9)

    def _fail(_s: str) -> list[dict[str, Any]]:
        raise error

    adapter.xinfo_groups = _fail  # type: ignore[method-assign]
    with pytest.raises(redis_lib.ResponseError) as excinfo:
        adapter.trim_stream_to_registered_progress()
    assert excinfo.value is error
    assert calls == []


@pytest.mark.parametrize(
    "ghost_info",
    [None, {}],
    ids=["no-info-hash", "legacy-no-stream-name"],
)
def test_unknown_stream_counts_as_this_stream(adapter, ghost_info):
    """A registered name with no info hash (register/unregister race) or a
    legacy info without stream_name is assumed to be on this stream: the safe
    direction, so its missing group blocks the trim (no crash, not ignored)."""
    calls = _shadow(
        adapter,
        names=["a", "ghost"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 0}],
        trimmed=9,
    )
    adapter.get_subscriber_info = lambda n: (  # type: ignore[method-assign]
        ghost_info if n == "ghost" else {"stream_name": STREAM}
    )
    assert adapter.trim_stream_to_registered_progress() == 0
    assert calls == []


def test_trim_is_exact_on_configured_stream(adapter):
    """XTRIM targets the configured stream and is exact (approximate=False):
    '~' only drops whole radix nodes (~100 entries), a no-op on small streams."""
    _shadow(
        adapter,
        names=["a"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 0}],
    )
    seen: list[tuple[str, str, bool]] = []

    def _xtrim(stream: str, minid: str, approximate: bool = True) -> int:
        seen.append((stream, minid, approximate))
        return 3

    adapter.xtrim_minid = _xtrim  # type: ignore[method-assign]
    assert adapter.trim_stream_to_registered_progress() == 3
    assert seen == [(STREAM, "100-0", False)]


def test_reads_groups_before_registry(adapter):
    """XINFO GROUPS is read BEFORE the registry; the order is load-bearing.

    A subscriber registers before creating its group, so with groups first one
    that starts mid-trim is in both snapshots or registry-only (which blocks
    the trim). Registry-first could take its new group for an unregistered
    leftover and trim its earliest messages.
    """
    log: list[str] = []
    _shadow(
        adapter,
        names=["a"],
        groups=[{"name": "a", "last-delivered-id": "100-0", "pending": 0}],
        trimmed=1,
    )
    for name in ("xinfo_groups", "get_registered_subscribers", "xtrim_minid"):
        setattr(adapter, name, _logged(log, name, getattr(adapter, name)))
    assert adapter.trim_stream_to_registered_progress() == 1
    assert log == ["xinfo_groups", "get_registered_subscribers", "xtrim_minid"]
