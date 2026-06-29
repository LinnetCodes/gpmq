"""Subscriber decorators for validating user-defined function signatures."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Optional


def _is_subscriber_annotation(ann) -> bool:
    """Return True if ann is the real Subscriber class.

    Lazy import to avoid a circular import: decorators.py -> subscriber.py
    would be circular because subscriber.py imports decorators at module load.
    """
    try:
        from gpmq.subscriber.subscriber import Subscriber

        return ann is Subscriber
    except Exception:
        return False


def worker_context_processor(func: Callable) -> Callable:
    """Decorator that validates and marks a function as a worker context processor.

    The decorated function must accept exactly one parameter named ``context``
    (case-insensitive).  If type annotations are present, both the parameter
    and return type must be ``dict[str, Any]``.  Bare ``func(context)`` without
    annotations is also accepted.

    On success, sets ``_gpmq_ctx_proc = True`` on the function and returns it
    unchanged (no wrapping).

    Args:
        func: The function to validate and mark.

    Returns:
        The original function, with ``_gpmq_ctx_proc`` attribute set.

    Raises:
        TypeError: If the function signature does not match the expected pattern.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # 1. Must have exactly 1 parameter
    if len(params) != 1:
        raise TypeError(
            f"worker context processor '{func.__name__}' must accept exactly "
            f"1 parameter named 'context', got {len(params)}"
        )

    param = params[0]

    # 2. Parameter must be named "context" (case-insensitive)
    if param.name.lower() != "context":
        raise TypeError(
            f"worker context processor '{func.__name__}' parameter must be "
            f"named 'context', got '{param.name}'"
        )

    # Expected annotation string for dict[str, Any]
    _expected = "dict[str, Any]"

    # 3. If annotation present, must be dict[str, Any]
    if param.annotation is not inspect.Parameter.empty:
        ann = param.annotation
        if ann != dict[str, Any] and str(ann) != _expected:
            raise TypeError(
                f"worker context processor '{func.__name__}' parameter "
                f"'context' annotation must be dict[str, Any], "
                f"got {ann!r}"
            )

    # 4. If return annotation present, must be dict[str, Any]
    if sig.return_annotation is not inspect.Signature.empty:
        ret = sig.return_annotation
        if ret != dict[str, Any] and str(ret) != _expected:
            raise TypeError(
                f"worker context processor '{func.__name__}' return "
                f"annotation must be dict[str, Any], got {ret!r}"
            )

    # 5. Set marker attribute
    func._gpmq_ctx_proc = True  # type: ignore[attr-defined]

    # 6. Return the original function unchanged
    return func


def run_forever_method(func: Callable) -> Callable:
    """Decorator that validates and marks a function as a run_forever_method.

    The decorated function must accept exactly two parameters:
    - First: a Subscriber instance
    - Second: named ``paras`` (case-insensitive), an Optional[dict[str, Any]]

    Return annotation if present must be ``None``.
    The function should be a blocking call that runs indefinitely.

    Sets ``_gpmq_run_forever = True`` on the function and returns it unchanged.

    Args:
        func: The function to validate and mark.

    Returns:
        The original function, with ``_gpmq_run_forever`` attribute set.

    Raises:
        TypeError: If the function signature does not match the expected pattern.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # 1. Must have exactly 2 parameters
    if len(params) != 2:
        raise TypeError(
            f"run_forever_method '{func.__name__}' must accept exactly "
            f"2 parameters (subscriber, paras), got {len(params)}"
        )

    # 2. First parameter must be named "subscriber" (case-insensitive)
    first_param = params[0]
    if first_param.name.lower() != "subscriber":
        raise TypeError(
            f"run_forever_method '{func.__name__}' first parameter must be "
            f"named 'subscriber', got '{first_param.name}'"
        )
    if first_param.annotation is not inspect.Parameter.empty:
        ann = first_param.annotation
        if ann != "Subscriber" and not _is_subscriber_annotation(ann):
            raise TypeError(
                f"run_forever_method '{func.__name__}' parameter 'subscriber' "
                f"annotation must be Subscriber, got {ann!r}"
            )

    # 3. Second parameter must be named "paras" (case-insensitive)
    second_param = params[1]
    if second_param.name.lower() != "paras":
        raise TypeError(
            f"run_forever_method '{func.__name__}' second parameter must be "
            f"named 'paras', got '{second_param.name}'"
        )

    # 4. If annotation present, must be dict[str, Any] or Optional[dict[str, Any]]
    if second_param.annotation is not inspect.Parameter.empty:
        ann = second_param.annotation
        if (ann != dict[str, Any] and str(ann) != "dict[str, Any]" and
                ann != Optional[dict[str, Any]] and str(ann) != "Optional[dict[str, Any]]"):
            raise TypeError(
                f"run_forever_method '{func.__name__}' parameter "
                f"'paras' annotation must be Optional[dict[str, Any]] or dict[str, Any], "
                f"got {ann!r}"
            )

    # 5. Return annotation if present must be None
    if sig.return_annotation is not inspect.Signature.empty:
        if sig.return_annotation != "None" and sig.return_annotation is not None:
            raise TypeError(
                f"run_forever_method '{func.__name__}' return annotation must be None, "
                f"got {sig.return_annotation!r}, type {type(sig.return_annotation)}"
            )

    # 6. Set marker attribute
    func._gpmq_run_forever = True  # type: ignore[attr-defined]

    return func
