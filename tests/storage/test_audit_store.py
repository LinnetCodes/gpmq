"""SQLite audit store for GPMQ messages and results."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor


def test_audit_store_init(temp_db_path):
    """AuditStore should initialize database with tables."""
    from gpmq.storage.audit_store import AuditStore

    store = AuditStore(temp_db_path)
    store.connect()

    # Verify tables exist
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('messages', 'results')"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    assert "messages" in tables
    assert "results" in tables
    store.close()


def test_save_message(audit_store):
    """save_message should save message to database."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    message = Message(
        id="1234567890-0",
        type="user_created",
        timestamp=datetime.now(timezone.utc),
        publisher="test_publisher",
        payload={"user_id": 123},
    )
    audit_store.save_message(message)

    # Verify
    conn = sqlite3.connect(audit_store._db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM messages WHERE id = ?", (message.id,))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row["type"] == "user_created"
    assert row["publisher"] == "test_publisher"


def test_save_result(audit_store):
    """save_result should save processing result."""
    from gpmq.models.result import ProcessStatus, ProcessResult

    result = ProcessResult(
        message_id="test-id",
        status=ProcessStatus.SUCCESS,
        subscriber_name="test_sub",
        processing_time=0.5,
        data={"output": "test"},
    )
    audit_store.save_result("test-id", result)

    # Verify
    conn = sqlite3.connect(audit_store._db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM results WHERE message_id = ?", (result.message_id,))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row["status"] == "success"
    assert row["subscriber_name"] == "test_sub"
    assert row["processing_time"] == 0.5


def test_save_fail_fast_on_error(audit_store, temp_db_path):
    """save operations should silently return when connection is None."""
    from gpmq.models.result import ProcessStatus, ProcessResult

    # Close the connection and try to use it
    audit_store._conn.close()
    audit_store._conn = None

    result = ProcessResult(
        message_id="test-id",
        status=ProcessStatus.SUCCESS,
        subscriber_name="test_sub",
        processing_time=0.5,
    )

    # With lazy connect, save_result silently returns when not connected
    audit_store.save_result("test-id", result)


def test_save_result_guard_is_inside_lock(temp_db_path):
    """Regression for 1.5: after close(), save_result must be a silent no-op
    (guard observed under the lock), not raise on a None/closed cursor."""
    from gpmq.models.result import ProcessResult, ProcessStatus
    from gpmq.storage.audit_store import AuditStore

    store = AuditStore(temp_db_path)
    store.connect()
    store.close()  # _conn is now None

    result = ProcessResult(
        message_id="closed-1",
        status=ProcessStatus.SUCCESS,
        subscriber_name="sub",
        processing_time=0.1,
    )
    # Must be a silent no-op, NOT raise ProgrammingError on a None cursor.
    store.save_result("closed-1", result)
    # Also exercise a query and a cleanup method to confirm they no-op cleanly.
    assert store.query_by_type("x") == []
    assert store.cleanup_by_time(__import__("datetime").datetime.now()) == 0


def test_audit_store_guards_are_inside_lock():
    """Structural check (1.5): every None-guard must be textually INSIDE the
    lock block (guard line must appear AFTER the 'with self._lock:' line)."""
    import inspect
    from gpmq.storage.audit_store import AuditStore

    guarded = [
        "save_message", "save_result",
        "query_by_type", "query_by_time_range", "query_by_status",
        "query_by_correlation_id", "cleanup_by_time", "cleanup_by_type", "stats",
    ]
    for name in guarded:
        src = inspect.getsource(getattr(AuditStore, name))
        assert "if self._conn is None" in src, f"{name} missing None guard"
        guard_idx = src.index("if self._conn is None")
        lock_idx = src.index("with self._lock:")
        assert guard_idx > lock_idx, (
            f"{name}: None guard is BEFORE the lock block (TOCTOU still present)"
        )


def test_query_by_type(audit_store):
    """query_by_type should filter messages by type."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    # Add test messages
    msg1 = Message(
        id="1-0", type="user_created", timestamp=datetime.now(timezone.utc),
        publisher="test", payload={"id": 1}
    )
    msg2 = Message(
        id="2-0", type="order_created", timestamp=datetime.now(timezone.utc),
        publisher="test", payload={"id": 2}
    )
    audit_store.save_message(msg1)
    audit_store.save_message(msg2)

    # Query
    results = audit_store.query_by_type("user_created", limit=10)
    assert len(results) == 1
    assert results[0]["type"] == "user_created"


def test_query_by_time_range(audit_store):
    """query_by_time_range should filter messages by time."""
    from datetime import datetime, timezone, timedelta
    from gpmq.models.message import Message

    now = datetime.now(timezone.utc)
    old = now - timedelta(hours=1)
    recent = now - timedelta(minutes=5)

    msg1 = Message(id="old-0", type="test", timestamp=old, publisher="test", payload={})
    msg2 = Message(id="recent-0", type="test", timestamp=recent, publisher="test", payload={})
    audit_store.save_message(msg1)
    audit_store.save_message(msg2)

    start = recent - timedelta(minutes=1)
    results = audit_store.query_by_time_range(start, now, limit=10)
    assert len(results) == 1
    assert results[0]["id"] == "recent-0"


def test_query_by_status(audit_store):
    """query_by_status should filter results by status."""
    from gpmq.models.result import ProcessStatus, ProcessResult

    result1 = ProcessResult(
        message_id="1", status=ProcessStatus.SUCCESS,
        subscriber_name="test", processing_time=0.5
    )
    result2 = ProcessResult(
        message_id="2", status=ProcessStatus.FAILURE,
        subscriber_name="test", processing_time=0.5
    )
    audit_store.save_result("1", result1)
    audit_store.save_result("2", result2)

    results = audit_store.query_by_status(ProcessStatus.SUCCESS, limit=10)
    assert len(results) == 1
    assert results[0]["status"] == "success"


def test_query_by_correlation_id(audit_store):
    """query_by_correlation_id should find messages with matching correlation_id."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    msg1 = Message(
        id="1-0", type="test", timestamp=datetime.now(timezone.utc),
        publisher="test", payload={}, correlation_id="chain-123"
    )
    msg2 = Message(
        id="2-0", type="test", timestamp=datetime.now(timezone.utc),
        publisher="test", payload={}, correlation_id="chain-456"
    )
    audit_store.save_message(msg1)
    audit_store.save_message(msg2)

    results = audit_store.query_by_correlation_id("chain-123")
    assert len(results) == 1
    assert results[0]["correlation_id"] == "chain-123"


def test_cleanup_by_time(audit_store):
    """cleanup_by_time should delete old records."""
    from datetime import datetime, timezone, timedelta
    from gpmq.models.message import Message

    old = datetime.now(timezone.utc) - timedelta(hours=2)
    recent = datetime.now(timezone.utc) - timedelta(minutes=5)

    msg_old = Message(id="old-0", type="test", timestamp=old, publisher="test", payload={})
    msg_new = Message(id="new-0", type="test", timestamp=recent, publisher="test", payload={})
    audit_store.save_message(msg_old)
    audit_store.save_message(msg_new)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    count = audit_store.cleanup_by_time(cutoff)
    assert count >= 1  # At least old message deleted

    # Verify new message still exists
    results = audit_store.query_by_type("test", limit=10)
    assert len(results) == 1
    assert results[0]["id"] == "new-0"


def test_cleanup_by_type(audit_store):
    """cleanup_by_type should delete messages of specific type."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    msg1 = Message(id="1-0", type="type1", timestamp=datetime.now(timezone.utc), publisher="test", payload={})
    msg2 = Message(id="2-0", type="type2", timestamp=datetime.now(timezone.utc), publisher="test", payload={})
    audit_store.save_message(msg1)
    audit_store.save_message(msg2)

    count = audit_store.cleanup_by_type("type1")
    assert count >= 1

    # Verify type2 still exists
    results = audit_store.query_by_type("type2", limit=10)
    assert len(results) == 1


def test_cleanup_by_type_before(audit_store):
    """cleanup_by_type_before must delete only the type+before subset.

    2.5: audit cleanup's --type branch previously called cleanup_by_type and
    ignored --before, deleting ALL of that type. This verifies the combo method
    deletes 'type_a older than cutoff' while keeping 'type_a newer than cutoff'
    and 'any other type'.
    """
    from datetime import datetime, timezone, timedelta
    from gpmq.models.message import Message
    from gpmq.models.result import ProcessResult, ProcessStatus

    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    old_ts = cutoff - timedelta(minutes=30)
    new_ts = cutoff + timedelta(minutes=30)

    # type_a: one older (delete) + one newer (keep)
    a_old = Message(id="a-old-0", type="type_a", timestamp=old_ts, publisher="t", payload={})
    a_new = Message(id="a-new-0", type="type_a", timestamp=new_ts, publisher="t", payload={})
    # type_b: any time (keep)
    b_msg = Message(id="b-0", type="type_b", timestamp=old_ts, publisher="t", payload={})

    audit_store.save_message(a_old)
    audit_store.save_message(a_new)
    audit_store.save_message(b_msg)

    # Results for each message (results deleted before their messages for FK).
    audit_store.save_result("a-old-0", ProcessResult(
        message_id="a-old-0", status=ProcessStatus.SUCCESS,
        subscriber_name="sub", processing_time=0.1,
    ))
    audit_store.save_result("a-new-0", ProcessResult(
        message_id="a-new-0", status=ProcessStatus.SUCCESS,
        subscriber_name="sub", processing_time=0.1,
    ))

    count = audit_store.cleanup_by_type_before("type_a", cutoff)

    # Older type_a message (1) + its result (1) deleted.
    assert count == 2

    # Newer type_a message KEPT.
    a_remaining = audit_store.query_by_type("type_a", limit=10)
    assert [m["id"] for m in a_remaining] == ["a-new-0"]

    # type_b messages KEPT.
    b_remaining = audit_store.query_by_type("type_b", limit=10)
    assert [m["id"] for m in b_remaining] == ["b-0"]

    # The older type_a result is gone; the newer one still present.
    success_results = audit_store.query_by_status(ProcessStatus.SUCCESS, limit=10)
    remaining_ids = {r["message_id"] for r in success_results}
    assert "a-old-0" not in remaining_ids
    assert "a-new-0" in remaining_ids


def test_stats(audit_store):
    """stats should return aggregate statistics."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message
    from gpmq.models.result import ProcessStatus, ProcessResult

    # Add test data
    msg = Message(id="1-0", type="test", timestamp=datetime.now(timezone.utc), publisher="test", payload={})
    audit_store.save_message(msg)

    result1 = ProcessResult(
        message_id="1", status=ProcessStatus.SUCCESS,
        subscriber_name="sub_a", processing_time=0.5
    )
    result2 = ProcessResult(
        message_id="1", status=ProcessStatus.FAILURE,
        subscriber_name="sub_b", processing_time=0.5
    )
    audit_store.save_result("1", result1)
    audit_store.save_result("1", result2)

    stats = audit_store.stats()
    assert stats["total_messages"] >= 1
    assert stats["total_results"] >= 2
    assert "results_by_status" in stats
    assert "messages_by_type" in stats


def test_close_behavior(audit_store, temp_db_path):
    """close should properly close connection."""
    assert audit_store._conn is not None
    audit_store.close()
    assert audit_store._conn is None

    # Should not error when closing already closed connection
    audit_store.close()


def test_large_payload(audit_store):
    """AuditStore should handle large payloads."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message
    import json

    large_payload = {"data": "x" * 10000}
    msg = Message(
        id="large-0", type="test", timestamp=datetime.now(timezone.utc),
        publisher="test", payload=large_payload
    )
    audit_store.save_message(msg)

    results = audit_store.query_by_type("test", limit=10)
    assert len(results) == 1
    assert len(results[0]["payload"]) == len(json.dumps(large_payload))


def test_result_with_error_details(audit_store):
    """save_result should preserve error details."""
    from gpmq.models.result import ProcessStatus, ProcessResult

    result = ProcessResult(
        message_id="error-1",
        status=ProcessStatus.EXCEPTION,
        subscriber_name="test",
        processing_time=0.5,
        error_message="Test error",
        error_traceback="Line 1\nLine 2\nLine 3",
    )
    audit_store.save_result("error-1", result)

    results = audit_store.query_by_status(ProcessStatus.EXCEPTION, limit=10)
    assert len(results) == 1
    assert results[0]["error_message"] == "Test error"
    assert "Line 1" in results[0]["error_traceback"]


def test_save_result_real_overwrites_timeout(audit_store):
    """1.2 case 1: a real result (SUCCESS) arriving after a TIMEOUT row overwrites it."""
    from gpmq.models.result import ProcessResult, ProcessStatus

    audit_store.save_result("p1", ProcessResult(
        message_id="p1", status=ProcessStatus.TIMEOUT,
        subscriber_name="sub", processing_time=5.0, timeout_seconds=5.0,
    ))
    audit_store.save_result("p1", ProcessResult(
        message_id="p1", status=ProcessStatus.SUCCESS,
        subscriber_name="sub", processing_time=0.2,
    ))

    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.SUCCESS)] == ["success"]
    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.TIMEOUT)] == []


def test_save_result_timeout_after_real_is_ignored(audit_store):
    """1.2 case 3: a late TIMEOUT arriving after a real result is silently ignored."""
    from gpmq.models.result import ProcessResult, ProcessStatus

    audit_store.save_result("p2", ProcessResult(
        message_id="p2", status=ProcessStatus.SUCCESS,
        subscriber_name="sub", processing_time=0.2,
    ))
    audit_store.save_result("p2", ProcessResult(
        message_id="p2", status=ProcessStatus.TIMEOUT,
        subscriber_name="sub", processing_time=5.0, timeout_seconds=5.0,
    ))

    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.SUCCESS)] == ["success"]
    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.TIMEOUT)] == []


def test_save_result_real_vs_real_raises(audit_store):
    """1.2 case 2: two real results for same (msg, subscriber) raise."""
    import pytest
    from gpmq.models.result import ProcessResult, ProcessStatus

    audit_store.save_result("p3", ProcessResult(
        message_id="p3", status=ProcessStatus.SUCCESS,
        subscriber_name="sub", processing_time=0.2,
    ))
    with pytest.raises(Exception):
        audit_store.save_result("p3", ProcessResult(
            message_id="p3", status=ProcessStatus.FAILURE,
            subscriber_name="sub", processing_time=0.3,
        ))


def test_save_result_exception_overwrites_timeout(audit_store):
    """1.2 + 1.1 linkage: an EXCEPTION result (poison-message) overwrites a TIMEOUT."""
    from gpmq.models.result import ProcessResult, ProcessStatus

    audit_store.save_result("p4", ProcessResult(
        message_id="p4", status=ProcessStatus.TIMEOUT,
        subscriber_name="sub", processing_time=5.0, timeout_seconds=5.0,
    ))
    audit_store.save_result("p4", ProcessResult(
        message_id="p4", status=ProcessStatus.EXCEPTION,
        subscriber_name="sub", processing_time=0.0, error_message="poison",
    ))
    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.EXCEPTION)] == ["exception"]
    assert [r["status"] for r in audit_store.query_by_status(ProcessStatus.TIMEOUT)] == []


def test_save_message_dedup_same_id(audit_store):
    """save_message should not duplicate when called twice with same message id."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    msg = Message(
        id="dedup-0",
        type="test",
        timestamp=datetime.now(timezone.utc),
        publisher="test_publisher",
        payload={"key": "value"},
    )
    audit_store.save_message(msg)
    audit_store.save_message(msg)  # second call should be ignored

    stats = audit_store.stats()
    assert stats["total_messages"] == 1


def test_save_result_dedup_same_message_and_subscriber(audit_store):
    """save_result should not duplicate for same message_id + subscriber_name.

    Under the 1.2 priority rule, a second real result would raise (real-vs-real
    collision). The 'no duplicate row' intent is preserved by sending a late
    TIMEOUT after the real result, which is silently ignored.
    """
    from gpmq.models.result import ProcessStatus, ProcessResult

    audit_store.save_result("msg-1", ProcessResult(
        message_id="msg-1", status=ProcessStatus.SUCCESS,
        subscriber_name="sub_a", processing_time=0.5,
    ))
    audit_store.save_result("msg-1", ProcessResult(
        message_id="msg-1", status=ProcessStatus.TIMEOUT,
        subscriber_name="sub_a", processing_time=5.0, timeout_seconds=5.0,
    ))  # ignored — no dup row

    stats = audit_store.stats()
    assert stats["total_results"] == 1


def test_save_result_different_subscribers_not_deduped(audit_store):
    """save_result should allow different subscribers for same message."""
    from gpmq.models.result import ProcessStatus, ProcessResult

    r1 = ProcessResult(
        message_id="msg-1", status=ProcessStatus.SUCCESS,
        subscriber_name="sub_a", processing_time=0.5,
    )
    r2 = ProcessResult(
        message_id="msg-1", status=ProcessStatus.SUCCESS,
        subscriber_name="sub_b", processing_time=0.3,
    )
    audit_store.save_result("msg-1", r1)
    audit_store.save_result("msg-1", r2)

    stats = audit_store.stats()
    assert stats["total_results"] == 2


def test_concurrent_save_result_from_multiple_threads(audit_store):
    """save_result must be safe when called from multiple threads concurrently.

    Regression test: ThreadPoolExecutor in GPMQClient.wait_all() caused
    sqlite3.InterfaceError / sqlite3.ProgrammingError when multiple workers
    called save_result() on the same connection simultaneously.
    """
    from gpmq.models.result import ProcessStatus, ProcessResult

    num_workers = 10
    results_per_worker = 50

    def write_results(worker_id: int) -> int:
        for i in range(results_per_worker):
            result = ProcessResult(
                message_id=f"msg-{worker_id}-{i}",
                status=ProcessStatus.SUCCESS,
                subscriber_name=f"sub-{worker_id}",
                processing_time=0.01,
            )
            audit_store.save_result(f"msg-{worker_id}-{i}", result)
        return results_per_worker

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(write_results, wid) for wid in range(num_workers)]
        written = [f.result() for f in futures]

    assert sum(written) == num_workers * results_per_worker

    stats = audit_store.stats()
    assert stats["total_results"] == num_workers * results_per_worker


def test_concurrent_save_message_from_multiple_threads(audit_store):
    """save_message must be safe when called from multiple threads concurrently."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message

    num_workers = 10

    def write_message(worker_id: int) -> str:
        msg = Message(
            id=f"msg-{worker_id}",
            type="test",
            timestamp=datetime.now(timezone.utc),
            publisher="test",
            payload={"worker": worker_id},
        )
        audit_store.save_message(msg)
        return msg.id

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(write_message, wid) for wid in range(num_workers)]
        ids = [f.result() for f in futures]

    assert len(ids) == num_workers
    stats = audit_store.stats()
    assert stats["total_messages"] == num_workers


def test_concurrent_mixed_read_write(audit_store):
    """Reads and writes must coexist safely across threads."""
    from datetime import datetime, timezone
    from gpmq.models.message import Message
    from gpmq.models.result import ProcessStatus, ProcessResult

    # Pre-populate some data
    for i in range(5):
        msg = Message(
            id=f"pre-{i}", type="test", timestamp=datetime.now(timezone.utc),
            publisher="test", payload={},
        )
        audit_store.save_message(msg)

    errors: list[Exception] = []

    def writer(writer_id: int) -> None:
        try:
            for i in range(20):
                result = ProcessResult(
                    message_id=f"rw-{writer_id}-msg-{i}",
                    status=ProcessStatus.SUCCESS,
                    subscriber_name=f"sub-{writer_id}-{i}",
                    processing_time=0.01,
                )
                audit_store.save_result(f"rw-{writer_id}-msg-{i}", result)
        except Exception as e:
            errors.append(e)

    def reader() -> None:
        try:
            for _ in range(20):
                audit_store.stats()
                audit_store.query_by_type("test", limit=10)
        except Exception as e:
            errors.append(e)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for wid in range(2):
            futures.append(executor.submit(writer, wid))
            futures.append(executor.submit(reader))
        for f in futures:
            f.result()

    assert errors == [], f"Concurrent read/write errors: {errors}"


# ------------------------------------------------------------------
# Lazy connect tests
# ------------------------------------------------------------------


class TestAuditStoreLazyConnect:
    """Verify that AuditStore delays connection until connect() is called."""

    def test_init_does_not_open_connection(self, temp_db_path):
        """__init__ must not open a SQLite connection."""
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        assert store._conn is None
        store.close()  # safe no-op

    def test_connect_opens_connection(self, temp_db_path):
        """connect() must open the SQLite connection and create tables."""
        import sqlite3
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        assert store._conn is None

        store.connect()
        assert store._conn is not None

        # Verify tables were created
        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('messages', 'results')"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "messages" in tables
        assert "results" in tables
        store.close()

    def test_connect_is_reentrant(self, temp_db_path):
        """Calling connect() twice must not raise and must keep same connection."""
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        store.connect()
        first_conn = store._conn
        store.connect()  # second call — should be no-op
        assert store._conn is first_conn
        store.close()

    def test_close_on_unconnected_is_safe(self, temp_db_path):
        """close() on a never-connected store must not raise."""
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        store.close()  # should not raise
        assert store._conn is None

    def test_save_message_silently_skips_when_not_connected(self, temp_db_path):
        """save_message must return silently if not connected."""
        from datetime import datetime, timezone
        from gpmq.storage.audit_store import AuditStore
        from gpmq.models.message import Message

        store = AuditStore(temp_db_path)
        msg = Message(
            id="skip-0",
            type="test",
            timestamp=datetime.now(timezone.utc),
            publisher="test",
            payload={},
        )
        # Must not raise
        store.save_message(msg)
        store.close()

    def test_save_result_silently_skips_when_not_connected(self, temp_db_path):
        """save_result must return silently if not connected."""
        from gpmq.storage.audit_store import AuditStore
        from gpmq.models.result import ProcessResult, ProcessStatus

        store = AuditStore(temp_db_path)
        result = ProcessResult(
            message_id="skip-0",
            status=ProcessStatus.SUCCESS,
            subscriber_name="test",
            processing_time=0.01,
        )
        # Must not raise
        store.save_result("skip-0", result)
        store.close()

    def test_query_returns_empty_when_not_connected(self, temp_db_path):
        """All query methods must return [] if not connected."""
        from datetime import datetime, timezone
        from gpmq.storage.audit_store import AuditStore
        from gpmq.models.result import ProcessStatus

        store = AuditStore(temp_db_path)
        assert store.query_by_type("test") == []
        assert store.query_by_time_range(
            datetime.now(timezone.utc), datetime.now(timezone.utc)
        ) == []
        assert store.query_by_status(ProcessStatus.SUCCESS) == []
        assert store.query_by_correlation_id("abc") == []
        store.close()

    def test_stats_returns_zeroes_when_not_connected(self, temp_db_path):
        """stats() must return zeroed dict if not connected."""
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        s = store.stats()
        assert s == {
            "total_messages": 0,
            "total_results": 0,
            "results_by_status": {},
            "messages_by_type": {},
        }
        store.close()

    def test_cleanup_returns_zero_when_not_connected(self, temp_db_path):
        """cleanup methods must return 0 if not connected."""
        from datetime import datetime, timezone
        from gpmq.storage.audit_store import AuditStore

        store = AuditStore(temp_db_path)
        assert store.cleanup_by_time(datetime.now(timezone.utc)) == 0
        assert store.cleanup_by_type("test") == 0
        store.close()




