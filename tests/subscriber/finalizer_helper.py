"""Importable worker context finalizer used by integration tests.

The finalizer must be resolvable via a dotted import path in a *spawned*
worker process, so it lives in a real module (not a test-function local or a
``sys.modules`` stub, which the fresh child interpreter cannot see).  It
records its invocation to a marker file whose path is supplied through the
worker context, proving the finalizer received the live context.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from gpmq.subscriber.decorators import worker_context_finalizer


@worker_context_finalizer
def record_finalizer_call(context: dict[str, Any]) -> None:
    """Write a marker file recording the finalizer call.

    Reads the target path and the recorded fields from the live worker
    context — the marker can only be complete if the real context was
    passed.
    """
    marker = Path(context["finalizer_marker_path"])
    payload = {
        "worker_id": context["worker_id"],
        "subscriber_name": context["subscriber_name"],
    }
    marker.write_text(json.dumps(payload), encoding="utf-8")
