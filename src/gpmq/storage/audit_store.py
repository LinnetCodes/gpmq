"""SQLite audit store for GPMQ messages and results."""

import json
import sqlite3
import threading

from datetime import datetime
from typing import Any

from gpmq.models.message import Message
from gpmq.models.result import ProcessResult, ProcessStatus


class AuditStore:
    """SQLite audit store for GPMQ messages and results.

    This provides persistent audit logging for all messages and
    processing results. SQLite errors fail fast to ensure data integrity.
    """

    def __init__(self, db_path: str) -> None:
        """Initialize with database path.

        Stores the path but does not open a connection.  Call
        :meth:`connect` to open the SQLite database and create tables.

        Args:
            db_path: Path to SQLite database file
        """
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Open the SQLite database connection and create tables.

        Reentrant -- calling ``connect()`` when already connected is a
        no-op.  Thread-safe: uses ``_lock`` to prevent concurrent
        connection leaks.

        Raises:
            sqlite3.Error: If database initialization fails
        """
        with self._lock:
            if self._conn is not None:
                return
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.row_factory = sqlite3.Row
            self._create_tables()

    def _create_tables(self) -> None:
        """Create database tables and indexes."""
        cursor = self._conn.cursor()

        # Messages table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                publisher TEXT NOT NULL,
                payload TEXT NOT NULL,
                correlation_id TEXT,
                created_at TEXT NOT NULL
            )
        """)

        # Results table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL,
                subscriber_name TEXT NOT NULL,
                status TEXT NOT NULL,
                processing_time REAL NOT NULL,
                data TEXT,
                error_message TEXT,
                error_traceback TEXT,
                timeout_seconds REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (message_id) REFERENCES messages(id),
                UNIQUE(message_id, subscriber_name)
            )
        """)

        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(type)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_status ON results(status)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_results_message_id ON results(message_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_correlation_id ON messages(correlation_id)"
        )

        self._conn.commit()

    def close(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None

    def save_message(self, message: Message) -> None:
        """Save message to audit store.

        Args:
            message: Message to save

        Raises:
            sqlite3.Error: If save fails (fail fast)
        """
        with self._lock:
            if self._conn is None:
                return
            cursor = self._conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO messages (id, type, timestamp, publisher, payload, correlation_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message.id,
                    message.type,
                    message.timestamp.isoformat(),
                    message.publisher,
                    json.dumps(message.payload),
                    message.correlation_id,
                    datetime.now().isoformat(),
                ),
            )
            self._conn.commit()

    def save_result(self, message_id: str, result: ProcessResult) -> None:
        """Save processing result with status-priority semantics.

        Priority rules for an existing row with the same
        (message_id, subscriber_name):

        * existing = TIMEOUT, new = real (SUCCESS/FAILURE/EXCEPTION)
          -> overwrite (real replaces the placeholder).
        * existing = real, new = real -> raise (genuine collision).
        * existing = real, new = TIMEOUT (late) -> silently ignore.
        * no existing row -> insert.

        Args:
            message_id: Associated message ID
            result: ProcessResult to save

        Raises:
            sqlite3.Error: If save fails (fail fast)
            RuntimeError: If a real result collides with an existing real result
        """
        with self._lock:
            if self._conn is None:
                return
            cursor = self._conn.cursor()
            existing = cursor.execute(
                "SELECT status FROM results WHERE message_id = ? AND subscriber_name = ?",
                (message_id, result.subscriber_name),
            ).fetchone()
            new_status = result.status.value
            real_statuses = {
                ProcessStatus.SUCCESS.value,
                ProcessStatus.FAILURE.value,
                ProcessStatus.EXCEPTION.value,
            }
            if existing is not None:
                existing_status = existing["status"]
                if existing_status == ProcessStatus.TIMEOUT.value and new_status in real_statuses:
                    action = "overwrite"
                elif existing_status in real_statuses and new_status in real_statuses:
                    raise RuntimeError(
                        f"Conflicting real results for message_id={message_id!r}, "
                        f"subscriber={result.subscriber_name!r}: "
                        f"existing={existing_status!r}, new={new_status!r}"
                    )
                elif existing_status in real_statuses and new_status == ProcessStatus.TIMEOUT.value:
                    # Late TIMEOUT after a real result already stored — ignore.
                    return
                else:
                    # Any other combination (incl. TIMEOUT/TIMEOUT) — ignore.
                    return
            else:
                action = "insert"

            if action == "overwrite":
                cursor.execute(
                    """
                    DELETE FROM results
                    WHERE message_id = ? AND subscriber_name = ?
                    """,
                    (message_id, result.subscriber_name),
                )
            cursor.execute(
                """
                INSERT INTO results (message_id, subscriber_name, status, processing_time,
                                  data, error_message, error_traceback, timeout_seconds, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    message_id,
                    result.subscriber_name,
                    new_status,
                    result.processing_time,
                    json.dumps(result.data) if result.data else None,
                    result.error_message,
                    result.error_traceback,
                    result.timeout_seconds,
                    datetime.now().isoformat(),
                ),
            )
            self._conn.commit()

    def query_by_type(
        self,
        message_type: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Query messages by type.

        Args:
            message_type: Message type to filter
            start_time: Optional start of time range
            end_time: Optional end of time range
            limit: Maximum results to return

        Returns:
            List of message dicts

        Raises:
            sqlite3.Error: If query fails
        """
        with self._lock:
            if self._conn is None:
                return []
            cursor = self._conn.cursor()

            query = "SELECT * FROM messages WHERE type = ?"
            params: list[Any] = [message_type]

            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())
            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())

            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)

            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def query_by_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int = 100,
    ) -> list[dict]:
        """Query messages in time range.

        Args:
            start_time: Start of time range (inclusive)
            end_time: End of time range (inclusive)
            limit: Maximum results to return

        Returns:
            List of message dicts

        Raises:
            sqlite3.Error: If query fails
        """
        with self._lock:
            if self._conn is None:
                return []
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT * FROM messages
                WHERE timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp DESC LIMIT ?
            """,
                (start_time.isoformat(), end_time.isoformat(), limit),
            )
            return [dict(row) for row in cursor.fetchall()]

    def query_by_status(self, status: ProcessStatus, limit: int = 100) -> list[dict]:
        """Query results by status.

        Args:
            status: ProcessStatus to filter
            limit: Maximum results to return

        Returns:
            List of result dicts

        Raises:
            sqlite3.Error: If query fails
        """
        with self._lock:
            if self._conn is None:
                return []
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT * FROM results
                WHERE status = ?
                ORDER BY created_at DESC LIMIT ?
            """,
                (status.value, limit),
            )
            return [dict(row) for row in cursor.fetchall()]

    def query_by_correlation_id(self, correlation_id: str) -> list[dict]:
        """Query messages by correlation ID.

        Args:
            correlation_id: Correlation ID to filter

        Returns:
            List of message dicts

        Raises:
            sqlite3.Error: If query fails
        """
        with self._lock:
            if self._conn is None:
                return []
            cursor = self._conn.cursor()
            cursor.execute(
                """
                SELECT * FROM messages
                WHERE correlation_id = ?
                ORDER BY timestamp DESC
            """,
                (correlation_id,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def cleanup_by_time(self, before: datetime) -> int:
        """Delete messages and results before timestamp.

        Args:
            before: Cutoff timestamp (exclusive)

        Returns:
            Total count of deleted records

        Raises:
            sqlite3.Error: If cleanup fails
        """
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.cursor()

            # Delete results first (foreign key constraint)
            cursor.execute(
                """
                DELETE FROM results
                WHERE message_id IN (SELECT id FROM messages WHERE timestamp < ?)
            """,
                (before.isoformat(),),
            )

            result_count = cursor.rowcount

            # Delete messages
            cursor.execute(
                "DELETE FROM messages WHERE timestamp < ?", (before.isoformat(),)
            )
            message_count = cursor.rowcount

            self._conn.commit()
            return result_count + message_count

    def cleanup_by_type(self, message_type: str) -> int:
        """Delete messages of specific type.

        Args:
            message_type: Message type to delete

        Returns:
            Total count of deleted records

        Raises:
            sqlite3.Error: If cleanup fails
        """
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.cursor()

            # Delete results first
            cursor.execute(
                """
                DELETE FROM results
                WHERE message_id IN (SELECT id FROM messages WHERE type = ?)
            """,
                (message_type,),
            )

            result_count = cursor.rowcount

            # Delete messages
            cursor.execute("DELETE FROM messages WHERE type = ?", (message_type,))
            message_count = cursor.rowcount

            self._conn.commit()
            return result_count + message_count

    def cleanup_by_type_before(self, message_type: str, before: datetime) -> int:
        """Delete messages of a specific type older than a timestamp.

        Args:
            message_type: Message type to filter.
            before: Cutoff timestamp (exclusive).

        Returns:
            Total count of deleted records (results + messages).

        Raises:
            sqlite3.Error: If cleanup fails
        """
        with self._lock:
            if self._conn is None:
                return 0
            cursor = self._conn.cursor()

            cursor.execute(
                """
                DELETE FROM results
                WHERE message_id IN (
                    SELECT id FROM messages WHERE type = ? AND timestamp < ?
                )
            """,
                (message_type, before.isoformat()),
            )
            result_count = cursor.rowcount

            cursor.execute(
                "DELETE FROM messages WHERE type = ? AND timestamp < ?",
                (message_type, before.isoformat()),
            )
            message_count = cursor.rowcount

            self._conn.commit()
            return result_count + message_count

    def stats(self) -> dict[str, Any]:
        """Get statistics.

        Returns:
            Dict with keys:
            - total_messages: int
            - total_results: int
            - results_by_status: dict[str, int]
            - messages_by_type: dict[str, int]

        Raises:
            sqlite3.Error: If stats query fails
        """
        with self._lock:
            if self._conn is None:
                return {
                    "total_messages": 0,
                    "total_results": 0,
                    "results_by_status": {},
                    "messages_by_type": {},
                }
            cursor = self._conn.cursor()

            # Total counts
            cursor.execute("SELECT COUNT(*) FROM messages")
            total_messages = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM results")
            total_results = cursor.fetchone()[0]

            # Results by status
            cursor.execute("SELECT status, COUNT(*) FROM results GROUP BY status")
            results_by_status = {row[0]: row[1] for row in cursor.fetchall()}

            # Messages by type
            cursor.execute("SELECT type, COUNT(*) FROM messages GROUP BY type")
            messages_by_type = {row[0]: row[1] for row in cursor.fetchall()}

            return {
                "total_messages": total_messages,
                "total_results": total_results,
                "results_by_status": results_by_status,
                "messages_by_type": messages_by_type,
            }
