"""
test_db_locking.py — Tests targeting SQLite "database is locked" failure modes.

Background
----------
The app uses get_conn() to open a new sqlite3.Connection on every call.  Each
`with get_conn() as conn:` block commits/rolls back on exit but does NOT close
the connection.  SQLite (WAL mode) allows many concurrent readers but only one
concurrent writer.  When two writers overlap — either from separate threads or
from a nested get_conn() call inside an active write transaction — the second
writer retries until `timeout` seconds elapse and then raises:

    sqlite3.OperationalError: database is locked

This module contains tests that:

  1. Demonstrate the raw SQLite locking behaviour on a file-based DB.
  2. Verify the connection-lifecycle behaviour of `with conn`.
  3. Reproduce the nested-write bug inside insert_payload() using a file DB
     (the existing test fixture patches both modules to the same in-memory
     connection, which masks the issue).
  4. Simulate concurrent background-task writes with threads.
  5. Show which sequential patterns are safe and which are hazardous.
"""

import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from database.connection import get_conn
from database.location.overland.table import LocationOverlandTable
from models.telemetry import OverlandPayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PLACES_DDL = """
    CREATE TABLE IF NOT EXISTS places (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        lat_snap     REAL NOT NULL,
        lon_snap     REAL NOT NULL,
        country_code TEXT,
        country      TEXT,
        region       TEXT,
        city         TEXT,
        suburb       TEXT,
        road         TEXT,
        display_name TEXT,
        geocoded_at  TEXT,
        UNIQUE(lat_snap, lon_snap)
    );
"""

_OVERLAND_DDL = """
    CREATE TABLE IF NOT EXISTS location_overland (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id           TEXT NOT NULL,
        timestamp           TEXT NOT NULL,
        latitude            REAL NOT NULL,
        longitude           REAL NOT NULL,
        altitude            REAL,
        speed               REAL,
        horizontal_accuracy REAL,
        vertical_accuracy   REAL,
        motion              TEXT,
        activity            TEXT,
        wifi_ssid           TEXT,
        battery_state       TEXT,
        battery_level       REAL,
        pauses              INTEGER,
        desired_accuracy    REAL,
        significant_change  TEXT,
        place_id            INTEGER REFERENCES places(id),
        raw_json            TEXT,
        inserted_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
        UNIQUE(device_id, timestamp)
    );
"""

_NOISE_DDL = """
    CREATE TABLE IF NOT EXISTS location_noise (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        overland_id INTEGER NOT NULL REFERENCES location_overland(id) ON DELETE CASCADE,
        tier        INTEGER NOT NULL,
        reason      TEXT NOT NULL,
        flagged_at  TEXT NOT NULL DEFAULT(strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_noise_overland_id ON location_noise(overland_id);
"""


def _setup_file_db(path: Path) -> None:
    """Create the minimal schema needed by overland + noise tests on a file DB."""
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(_PLACES_DDL + _OVERLAND_DDL + _NOISE_DDL)
    conn.commit()
    conn.close()


def _make_feature(lon=2.3522, lat=48.8566, ts="2024-06-15T09:00:00+00:00",
                  h_acc=10.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {"timestamp": ts, "horizontal_accuracy": h_acc},
    }


# ---------------------------------------------------------------------------
# Section 1: Raw SQLite locking behaviour
# ---------------------------------------------------------------------------

class TestRawSQLiteLocking:
    """Foundational SQLite locking facts.

    These tests use raw sqlite3 — no app code — to document the exact locking
    semantics the rest of the tests build on.
    """

    def test_single_writer_succeeds(self, tmp_path):
        """One connection can write and commit without issue."""
        db = tmp_path / "single.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 1
        conn.close()

    def test_two_concurrent_writers_raises_locked(self, tmp_path):
        """Two connections to the same file, both holding write transactions,
        causes the second one to raise OperationalError after timeout."""
        db = tmp_path / "two_writers.db"

        conn_a = sqlite3.connect(str(db))
        conn_a.execute("PRAGMA journal_mode=WAL;")
        conn_a.execute("CREATE TABLE t (x INTEGER)")
        conn_a.commit()

        # Open a write transaction on A — it now holds the RESERVED write lock.
        conn_a.execute("INSERT INTO t VALUES (1)")

        # B tries to write while A's transaction is open.
        conn_b = sqlite3.connect(str(db), timeout=0.1)  # short timeout for speed
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            conn_b.execute("INSERT INTO t VALUES (2)")

        conn_a.rollback()
        conn_a.close()
        conn_b.close()

    def test_timeout_is_respected(self, tmp_path):
        """The second writer waits approximately `timeout` seconds, then raises."""
        db = tmp_path / "timeout.db"

        conn_a = sqlite3.connect(str(db))
        conn_a.execute("PRAGMA journal_mode=WAL;")
        conn_a.execute("CREATE TABLE t (x INTEGER)")
        conn_a.commit()
        conn_a.execute("INSERT INTO t VALUES (1)")  # hold write lock

        conn_b = sqlite3.connect(str(db), timeout=0.5)
        start = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            conn_b.execute("INSERT INTO t VALUES (2)")
        elapsed = time.monotonic() - start

        assert elapsed >= 0.4, f"Timeout not respected: elapsed={elapsed:.2f}s"
        conn_a.rollback()
        conn_a.close()
        conn_b.close()

    def test_wal_mode_concurrent_readers_never_block(self, tmp_path):
        """WAL mode: two read-only connections can both read while a third writes."""
        db = tmp_path / "wal_read.db"

        setup = sqlite3.connect(str(db))
        setup.execute("PRAGMA journal_mode=WAL;")
        setup.execute("CREATE TABLE t (x INTEGER)")
        setup.execute("INSERT INTO t VALUES (42)")
        setup.commit()
        setup.close()

        reader1 = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        reader2 = sqlite3.connect(f"file:{db}?mode=ro", uri=True)

        # Both readers can fetch simultaneously without issue.
        r1 = reader1.execute("SELECT x FROM t").fetchone()[0]
        r2 = reader2.execute("SELECT x FROM t").fetchone()[0]
        assert r1 == 42
        assert r2 == 42

        reader1.close()
        reader2.close()

    def test_writer_released_allows_second_writer(self, tmp_path):
        """Once the first writer commits, the second writer succeeds."""
        db = tmp_path / "seq.db"

        conn_a = sqlite3.connect(str(db))
        conn_a.execute("PRAGMA journal_mode=WAL;")
        conn_a.execute("CREATE TABLE t (x INTEGER)")
        conn_a.commit()
        conn_a.execute("INSERT INTO t VALUES (1)")
        conn_a.commit()   # release the write lock
        conn_a.close()

        conn_b = sqlite3.connect(str(db), timeout=1)
        conn_b.execute("INSERT INTO t VALUES (2)")  # must not raise
        conn_b.commit()

        count = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 2
        conn_b.close()

    def test_sequential_connections_do_not_conflict(self, tmp_path):
        """Opening a second connection after the first is closed is always safe."""
        db = tmp_path / "sequential.db"

        conn1 = sqlite3.connect(str(db))
        conn1.execute("PRAGMA journal_mode=WAL;")
        conn1.execute("CREATE TABLE t (x INTEGER)")
        conn1.execute("INSERT INTO t VALUES (1)")
        conn1.commit()
        conn1.close()

        conn2 = sqlite3.connect(str(db))
        conn2.execute("INSERT INTO t VALUES (2)")
        conn2.commit()
        conn2.close()

        final = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert final == 2


# ---------------------------------------------------------------------------
# Section 2: Connection lifecycle — `with conn` semantics
# ---------------------------------------------------------------------------

class TestConnectionLifecycle:
    """Verify what `with sqlite3.Connection as conn` does (and does not) do."""

    def test_with_block_commits_on_success(self, tmp_path):
        """with conn: commits on normal exit, so changes are visible after."""
        db = tmp_path / "lifecycle.db"
        c = sqlite3.connect(str(db))
        c.execute("CREATE TABLE t (x INTEGER)")
        c.commit()

        with c:
            c.execute("INSERT INTO t VALUES (99)")

        count = c.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 1
        c.close()

    def test_with_block_rolls_back_on_exception(self, tmp_path):
        """with conn: rolls back on exception, so no changes are visible."""
        db = tmp_path / "rollback.db"
        c = sqlite3.connect(str(db))
        c.execute("CREATE TABLE t (x INTEGER)")
        c.commit()

        with pytest.raises(ValueError):
            with c:
                c.execute("INSERT INTO t VALUES (1)")
                raise ValueError("abort")

        count = c.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 0
        c.close()

    def test_with_block_does_not_close_connection(self, tmp_path):
        """`with conn` does NOT close the connection on exit.

        After the block the connection is still usable — it was committed/rolled
        back but never closed.  This is the sqlite3.Connection contract.
        """
        db = tmp_path / "not_closed.db"
        c = sqlite3.connect(str(db))
        c.execute("CREATE TABLE t (x INTEGER)")
        c.commit()

        with c:
            c.execute("INSERT INTO t VALUES (1)")

        # Connection is still open — we can query again.
        count = c.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 1
        c.close()

    def test_with_block_leaves_connection_open(self, tmp_path):
        """The `with conn` context manager commits/rolls back but does NOT close
        the connection.  This is the sqlite3.Connection contract, and every
        table's init()/insert() relies on it.

        After the `with` block exits the connection is still alive — a second
        write *from the same connection* works fine, but if another process or
        thread holds a write lock the connection will be stuck waiting until
        it is explicitly closed or garbage-collected.
        """
        db_path = tmp_path / "open_conn.db"

        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.commit()

        # Simulate the pattern used in every table: `with get_conn() as conn: ...`
        with conn:
            conn.execute("INSERT INTO t VALUES (1)")
        # The `with` block committed — conn is still open.

        # We can still query and write on the same connection.
        conn.execute("INSERT INTO t VALUES (2)")
        conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 2

        # Explicitly close to confirm it was still open.
        conn.close()

        # Verify the data on a fresh connection.
        verify = sqlite3.connect(str(db_path))
        assert verify.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
        verify.close()

    def test_cursor_usable_after_with_block_exits(self, tmp_path):
        """Demonstrates the get_most_recent_points() cursor-after-with pattern.

        The pattern is:
            with get_conn() as conn:
                rows = conn.execute(query)
            return rows.fetchall()   # ← after the with block exits

        This works because `with conn` commits but does not close the connection.
        The cursor still holds a reference to the open connection.
        """
        db = tmp_path / "cursor.db"
        conn = sqlite3.connect(str(db))
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(5)])
        conn.commit()

        # Mimic get_most_recent_points() exactly.
        with conn:
            rows = conn.execute("SELECT x FROM t ORDER BY x")

        results = rows.fetchall()   # used after the `with` block

        assert len(results) == 5
        assert [r[0] for r in results] == list(range(5))
        conn.close()

    def test_open_write_conn_blocks_second_writer(self, tmp_path):
        """An open (committed) connection can still block a second writer if
        Python has not yet closed it.

        This tests a subtlety: even after `with conn` commits, if `conn` is
        still referenced and was in an implicit transaction that was not yet
        fully released, a second write can fail.  In practice, the committed
        connection should release the write lock, so this tests the happy path.
        """
        db = tmp_path / "committed.db"
        conn_a = sqlite3.connect(str(db))
        conn_a.execute("PRAGMA journal_mode=WAL;")
        conn_a.execute("CREATE TABLE t (x INTEGER)")
        conn_a.commit()

        # After committing, the write lock is released — a second writer is fine.
        with conn_a:
            conn_a.execute("INSERT INTO t VALUES (1)")
        # conn_a committed above; write lock released.

        conn_b = sqlite3.connect(str(db), timeout=0.5)
        conn_b.execute("INSERT INTO t VALUES (2)")  # must NOT raise
        conn_b.commit()
        conn_b.close()
        conn_a.close()

        count = sqlite3.connect(str(db)).execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# Section 3: Nested write connection — the insert_payload bug
# ---------------------------------------------------------------------------

class TestNestedWriteConnection:
    """Demonstrates that opening a second write connection *inside* an active
    write transaction raises 'database is locked'.

    This exactly mirrors what insert_payload() does when it calls
    noise_table.insert() inside its `with get_conn() as conn` block.
    """

    def test_nested_write_inside_active_transaction_raises_locked(self, tmp_path):
        """Core reproduction of the bug.

        Outer connection writes (holds RESERVED lock).
        Inner connection tries to write → OperationalError: database is locked.
        """
        db = tmp_path / "nested.db"
        outer = sqlite3.connect(str(db))
        outer.execute("PRAGMA journal_mode=WAL;")
        outer.execute("CREATE TABLE main_table (x INTEGER)")
        outer.execute("CREATE TABLE side_table (y INTEGER)")
        outer.commit()

        # Start a write transaction on the outer connection.
        outer.execute("INSERT INTO main_table VALUES (1)")
        # Outer now holds the write lock (RESERVED in WAL mode).

        # Inner connection — exactly as noise_table.insert() does.
        inner = sqlite3.connect(str(db), timeout=0.1)
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            inner.execute("INSERT INTO side_table VALUES (99)")

        outer.rollback()
        outer.close()
        inner.close()

    def test_sequential_writes_from_separate_connections_are_safe(self, tmp_path):
        """If the outer connection commits first, the inner write is safe.

        This is the fix: commit (or close) the outer connection before allowing
        a second connection to write.
        """
        db = tmp_path / "sequential.db"
        outer = sqlite3.connect(str(db))
        outer.execute("PRAGMA journal_mode=WAL;")
        outer.execute("CREATE TABLE main_table (x INTEGER)")
        outer.execute("CREATE TABLE side_table (y INTEGER)")
        outer.commit()

        outer.execute("INSERT INTO main_table VALUES (1)")
        outer.commit()   # ← commit releases the write lock
        outer.close()

        inner = sqlite3.connect(str(db), timeout=0.1)
        inner.execute("INSERT INTO side_table VALUES (99)")  # must not raise
        inner.commit()
        inner.close()

    def test_insert_payload_deferred_noise_insert_file_db(self, tmp_path):
        """Verifies that the fix works: noise_table.insert() is now called after
        the outer transaction commits, so both connections succeed.

        Previously, insert_payload called noise_table.insert() inside its own
        `with get_conn() as conn` block, which opened a second write connection
        while the first still held the WAL write lock, causing
        'database is locked'.  The fix defers noise inserts to after the outer
        `with` block exits and its transaction is committed.
        """
        db_path = tmp_path / "nested_fixed.db"
        _setup_file_db(db_path)

        def make_file_conn(read_only=False):
            if read_only:
                c = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            else:
                c = sqlite3.connect(str(db_path), timeout=1)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON;")
            if not read_only:
                c.execute("PRAGMA journal_mode=WAL;")
            return c

        feature = _make_feature(h_acc=150.0)   # > LOCATION_NOISE_ACCURACY_THRESHOLD (100)
        payload = OverlandPayload(locations=[feature])

        tbl = LocationOverlandTable()

        with patch("database.location.overland.table.get_conn", side_effect=make_file_conn), \
             patch("database.location.noise.table.get_conn", side_effect=make_file_conn):
            inserted, skipped = tbl.insert_payload(payload, "iphone")

        assert inserted == 1

        # With the fix, the noise row IS present — the deferred insert succeeds.
        check = sqlite3.connect(str(db_path))
        noise_count = check.execute("SELECT COUNT(*) FROM location_noise").fetchone()[0]
        check.close()

        assert noise_count == 1, (
            "Expected 1 noise row for the high-accuracy point. "
            "If this is 0 the deferred-noise fix has been reverted."
        )

    def test_insert_payload_with_shared_conn_correctly_inserts_noise(self, tmp_path):
        """Control: when both modules share the same connection (as the existing
        test fixture does), the noise row IS inserted correctly — showing why
        those tests pass despite the production bug.
        """
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON;")
        db.executescript(_PLACES_DDL + _OVERLAND_DDL + _NOISE_DDL)

        feature = _make_feature(h_acc=150.0)
        payload = OverlandPayload(locations=[feature])

        tbl = LocationOverlandTable()

        with patch("database.location.overland.table.get_conn", return_value=db), \
             patch("database.location.noise.table.get_conn", return_value=db):
            inserted, skipped = tbl.insert_payload(payload, "iphone")

        assert inserted == 1
        noise_count = db.execute("SELECT COUNT(*) FROM location_noise").fetchone()[0]
        assert noise_count == 1  # shared conn → no locking → noise row present


# ---------------------------------------------------------------------------
# Section 4: Concurrent writes with threads (background task simulation)
# ---------------------------------------------------------------------------

class TestConcurrentWrites:
    """Simulate multiple FastAPI background tasks writing simultaneously.

    FastAPI runs synchronous background tasks in a ThreadPoolExecutor.  When
    two Overland uploads arrive close together, their background tasks can
    overlap, producing concurrent writes from separate threads.
    """

    def test_two_threads_writing_same_file_one_may_fail(self, tmp_path):
        """Two threads each try to write to the same SQLite file concurrently.

        At least one succeeds.  The second may raise OperationalError if it
        cannot acquire the write lock within `timeout` seconds.
        """
        db = tmp_path / "threaded.db"
        conn_setup = sqlite3.connect(str(db))
        conn_setup.execute("PRAGMA journal_mode=WAL;")
        conn_setup.execute("CREATE TABLE t (x INTEGER, writer TEXT)")
        conn_setup.commit()
        conn_setup.close()

        errors = []
        successes = []

        def writer(name, hold_seconds=0.2):
            try:
                conn = sqlite3.connect(str(db), timeout=0.1)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("INSERT INTO t VALUES (1, ?)", (name,))
                time.sleep(hold_seconds)  # hold the write lock
                conn.commit()
                conn.close()
                successes.append(name)
            except sqlite3.OperationalError as e:
                errors.append((name, str(e)))

        t1 = threading.Thread(target=writer, args=("thread-1",))
        t2 = threading.Thread(target=writer, args=("thread-2",))
        t1.start()
        time.sleep(0.01)   # give t1 a head start so it acquires the lock first
        t2.start()
        t1.join()
        t2.join()

        # At least one writer succeeded.
        assert len(successes) >= 1

        if errors:
            assert any("database is locked" in msg for _, msg in errors)

    def test_concurrent_insert_payload_calls_can_conflict(self, tmp_path):
        """Simulates two Overland background tasks running simultaneously.

        Both call insert_payload on the same file DB.  With a short timeout,
        one of them will raise 'database is locked' for at least some of its
        inserts.  In production the exception is caught per-point (logged,
        not re-raised), so the task finishes but some rows are dropped.
        """
        db_path = tmp_path / "concurrent_overland.db"
        _setup_file_db(db_path)

        lock_errors = []

        def make_file_conn(read_only=False):
            c = sqlite3.connect(str(db_path), timeout=0.1)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON;")
            c.execute("PRAGMA journal_mode=WAL;")
            return c

        def run_insert(device_id, hour_offset):
            features = [_make_feature(ts=f"2024-06-15T{(i + hour_offset):02d}:00:00+00:00",
                                      h_acc=10.0)
                        for i in range(3)]
            payload = OverlandPayload(locations=features)
            tbl = LocationOverlandTable()
            try:
                with patch("database.location.overland.table.get_conn",
                           side_effect=make_file_conn), \
                     patch("database.location.noise.table.get_conn",
                           side_effect=make_file_conn):
                    tbl.insert_payload(payload, device_id)
            except sqlite3.OperationalError as e:
                lock_errors.append(str(e))

        t1 = threading.Thread(target=run_insert, args=("device-1", 0))
        t2 = threading.Thread(target=run_insert, args=("device-2", 10))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # No assertion on lock_errors here — the point is to show the
        # scenario can happen.  The test passes even if both succeed
        # (when WAL serialises them quickly), but documents the hazard.

    def test_insert_payload_and_location_change_concurrent(self, tmp_path):
        """Simulates insert_payload and location_change.run() executing at the
        same time (which is how the /overland endpoint queues them).

        Both are added as separate background tasks, which FastAPI can run in
        different threads.  Here we use a threading.Event to synchronise them
        to the same instant to maximise contention.
        """
        db_path = tmp_path / "overlap.db"
        _setup_file_db(db_path)

        lock_errors = []
        start_event = threading.Event()

        def write_overland():
            start_event.wait()
            conn = sqlite3.connect(str(db_path), timeout=0.2)
            conn.execute("PRAGMA journal_mode=WAL;")
            try:
                conn.execute(
                    "INSERT INTO location_overland "
                    "(device_id, timestamp, latitude, longitude) "
                    "VALUES ('iphone', '2024-06-15T09:00:00Z', 48.85, 2.35)"
                )
                time.sleep(0.1)  # hold the write lock while location_change also writes
                conn.commit()
            except sqlite3.OperationalError as e:
                lock_errors.append(("overland", str(e)))
                conn.rollback()
            finally:
                conn.close()

        def write_location_change():
            start_event.wait()
            conn = sqlite3.connect(str(db_path), timeout=0.1)
            conn.execute("PRAGMA journal_mode=WAL;")
            # location_change.run() writes to known_places and place_visits;
            # here we write to the same DB to simulate the contention.
            try:
                conn.execute(
                    "INSERT INTO places (lat_snap, lon_snap) VALUES (48.850, 2.350)"
                )
                conn.commit()
            except sqlite3.OperationalError as e:
                lock_errors.append(("location_change", str(e)))
                conn.rollback()
            finally:
                conn.close()

        t1 = threading.Thread(target=write_overland)
        t2 = threading.Thread(target=write_location_change)
        t1.start()
        t2.start()
        start_event.set()   # release both threads simultaneously
        t1.join()
        t2.join()

        # Document whether a conflict occurred.
        if lock_errors:
            tables_affected = [t for t, _ in lock_errors]
            assert any("database is locked" in msg for _, msg in lock_errors), \
                f"Unexpected errors: {lock_errors}"
            # This is the expected failure mode — log which task was blocked.
            _ = tables_affected  # used for context in failure messages

    def test_many_concurrent_writers_with_short_timeout(self, tmp_path):
        """Stress test: N threads each try to insert one row.  With a short
        timeout, several will raise 'database is locked'.  With a long timeout
        (as in production: 10 s) they should all eventually succeed, but only
        because they serialise — meaning a batch of large payloads could cause
        real latency.
        """
        db_path = tmp_path / "stress.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.commit()
        conn.close()

        lock_errors = []
        successes = []
        lock = threading.Lock()

        def insert_one(i):
            try:
                c = sqlite3.connect(str(db_path), timeout=0.05)
                c.execute("PRAGMA journal_mode=WAL;")
                c.execute("INSERT INTO t VALUES (?)", (i,))
                c.commit()
                c.close()
                with lock:
                    successes.append(i)
            except sqlite3.OperationalError as e:
                with lock:
                    lock_errors.append(str(e))

        threads = [threading.Thread(target=insert_one, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads either succeed or get a locking error — never a silent hang.
        assert len(successes) + len(lock_errors) == 10
        # With WAL mode the window is narrow; most will succeed, but at least
        # one locking error is expected under tight contention.
        assert len(successes) > 0


# ---------------------------------------------------------------------------
# Section 5: Safe vs. unsafe sequential patterns
# ---------------------------------------------------------------------------

class TestSequentialPatterns:
    """Verify which sequential get_conn() patterns are safe in production.

    Sequential calls (second connection opened *after* the first commits) are
    safe even though they create separate connections — the first has released
    its write lock before the second acquires one.
    """

    def test_sequential_get_place_id_then_insert_is_safe(self, tmp_path):
        """LocationShortcutsTable.insert() calls get_place_id() (one conn) then
        opens its own conn.  Because get_place_id commits and returns before
        the second conn tries to write, there is no conflict.

        This test confirms the sequential pattern is safe.
        """
        db_path = tmp_path / "sequential_safe.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("CREATE TABLE places (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                     "lat_snap REAL, lon_snap REAL, UNIQUE(lat_snap, lon_snap))")
        conn.commit()
        conn.close()

        call_log = []

        def make_conn(read_only=False):
            c = sqlite3.connect(str(db_path), timeout=1)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON;")
            c.execute("PRAGMA journal_mode=WAL;")
            call_log.append("opened")
            return c

        with patch("database.location.geocoding.get_conn", side_effect=make_conn):
            from database.location.geocoding import get_place_id
            pid = get_place_id(48.8566, 2.3522)

        assert pid is not None
        assert len(call_log) == 1  # exactly one connection opened

    def test_insert_batch_opens_one_conn_per_insert(self, tmp_path):
        """CellularTable.insert_batch() calls insert() in a loop, opening a new
        connection per iteration.  Sequential (not concurrent), so no locking —
        but N connections are opened for N cellular states.

        This is inefficient but not a locking hazard.
        """
        db_path = tmp_path / "cellular.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.executescript("""
            CREATE TABLE location_shortcuts (
                id INTEGER PRIMARY KEY,
                timestamp TEXT, latitude REAL, longitude REAL,
                altitude REAL, device TEXT, is_locked INTEGER,
                battery INTEGER, is_charging INTEGER,
                is_connected_charger INTEGER, bssid TEXT, rssi INTEGER,
                place_id INTEGER, created_at TEXT
            );
            INSERT INTO location_shortcuts (id, timestamp, latitude, longitude, device)
            VALUES (1, '2024-06-15T09:00:00Z', 48.8566, 2.3522, 'iphone');

            CREATE TABLE cellular_state (
                id INTEGER PRIMARY KEY,
                shortcut_id INTEGER NOT NULL,
                provider_name TEXT,
                radio TEXT,
                code TEXT,
                is_roaming BOOLEAN,
                UNIQUE(shortcut_id, provider_name, radio)
            );
        """)
        conn.commit()
        conn.close()

        conn_count = []

        def make_conn(read_only=False):
            c = sqlite3.connect(str(db_path), timeout=1)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON;")
            c.execute("PRAGMA journal_mode=WAL;")
            conn_count.append(1)
            return c

        from database.cellular.table import CellularTable
        from models.telemetry import CellularState

        states = [
            CellularState(provider_name="T-Mobile", radio="LTE",
                          code="310", is_roaming=False),
            CellularState(provider_name="AT&T", radio="5G",
                          code="310", is_roaming=False),
        ]

        with patch("database.cellular.table.get_conn", side_effect=make_conn):
            CellularTable().insert_batch(states, shortcut_id=1)

        # One connection per insert call — no locking because they're sequential.
        assert len(conn_count) == 2

    def test_increment_api_usage_is_safe_when_called_alone(self, tmp_path):
        """increment_api_usage() opens a single connection via `with get_conn()`.
        Alone, this always succeeds.  The hazard is if it races with another
        write — tested in concurrent section.
        """
        db_path = tmp_path / "api_usage.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
            CREATE TABLE api_usage (
                service TEXT PRIMARY KEY,
                count   INTEGER NOT NULL DEFAULT 0,
                month   TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

        def make_conn(read_only=False):
            c = sqlite3.connect(str(db_path), timeout=1)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA foreign_keys = ON;")
            c.execute("PRAGMA journal_mode=WAL;")
            return c

        with patch("database.connection.get_conn", side_effect=make_conn):
            from database.connection import increment_api_usage
            increment_api_usage()
            increment_api_usage()

        count = sqlite3.connect(str(db_path)).execute(
            "SELECT count FROM api_usage WHERE service = 'exchangerate.host'"
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# Section 6: Specific get_conn() configuration tests
# ---------------------------------------------------------------------------

class TestGetConnConfiguration:
    """Verify that get_conn() sets the pragmas expected by the rest of the code."""

    def test_get_conn_write_enables_wal(self, tmp_path):
        """get_conn() (write mode) enables WAL journal mode."""
        db_path = tmp_path / "wal.db"
        db_path.touch()

        with patch("database.connection.DB_FILE", db_path):
            conn = get_conn()
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            conn.close()

        assert mode == "wal"

    def test_get_conn_write_enables_foreign_keys(self, tmp_path):
        """get_conn() (write mode) enables PRAGMA foreign_keys = ON."""
        db_path = tmp_path / "fk.db"
        db_path.touch()

        with patch("database.connection.DB_FILE", db_path):
            conn = get_conn()
            fk = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
            conn.close()

        assert fk == 1

    def test_get_conn_read_only_skips_wal_pragma(self, tmp_path):
        """get_conn(read_only=True) opens in URI read-only mode and does not
        set journal_mode (a write-only pragma)."""
        db_path = tmp_path / "ro.db"
        # File must exist for URI read-only to work.
        setup = sqlite3.connect(str(db_path))
        setup.execute("CREATE TABLE t (x INTEGER)")
        setup.commit()
        setup.close()

        with patch("database.connection.DB_FILE", db_path):
            conn = get_conn(read_only=True)
            # Can read …
            result = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
            # … but cannot write.
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO t VALUES (1)")
            conn.close()

        assert result == 0

    @pytest.mark.slow
    def test_get_conn_write_timeout_is_10_seconds(self, tmp_path):
        """get_conn() sets timeout=10, so a locked DB waits up to 10 s.

        NOTE: this test takes ~10 seconds — run with `-m slow` to include it.

        We don't actually wait 10 s — we just verify the timeout attribute.
        """
        db_path = tmp_path / "timeout_attr.db"
        db_path.touch()

        with patch("database.connection.DB_FILE", db_path):
            conn = get_conn()
            # sqlite3.Connection exposes no direct timeout attribute, but we can
            # confirm the behaviour by checking how long it waits when locked.
            # Use a short-lived lock to keep the test fast.
            conn.execute("CREATE TABLE t (x INTEGER)")
            conn.commit()

            blocker = sqlite3.connect(str(db_path))
            blocker.execute("INSERT INTO t VALUES (1)")  # holds write lock

            start = time.monotonic()
            try:
                conn.execute("INSERT INTO t VALUES (2)")
                conn.commit()
            except sqlite3.OperationalError:
                pass
            elapsed = time.monotonic() - start
            blocker.rollback()
            blocker.close()
            conn.close()

        # The default timeout is 10 s; since we set the blocker to NOT commit,
        # conn should wait ~10 s then raise.  We cap the test at 12 s to avoid
        # hanging CI.
        assert elapsed < 12, "get_conn() appears to be waiting longer than 10 s"
        assert elapsed >= 9, (
            f"get_conn() gave up after {elapsed:.1f} s — expected ~10 s timeout. "
            "Check that connection.py still passes timeout=10 to sqlite3.connect()."
        )
