"""
test_backfill_upload_log_and_completeness.py — Tests for the one-off
database/migrations/backfill_upload_log_and_completeness.py script:
upload_log reconstruction, spend_complete/pi_complete recomputation, and
the dry-run (default, rolled back) vs --apply (committed) behavior of main().
"""

import sqlite3
import sys
from datetime import date

import pytest

from database.migrations.backfill_upload_log_and_completeness import (
    main,
    reconstruct_upload_log,
    recompute_pi_complete,
    recompute_spend_complete,
)

SCHEMA = """
CREATE TABLE transactions (
    id        TEXT NOT NULL,
    source    TEXT NOT NULL,
    bank      TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    amount    REAL NOT NULL,
    currency  TEXT NOT NULL
);

CREATE TABLE upload_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    period_start  TEXT NOT NULL,
    period_end    TEXT NOT NULL,
    row_count     INTEGER NOT NULL,
    inferred      INTEGER NOT NULL DEFAULT 0,
    uploaded_at   TEXT,
    UNIQUE(source, period_start, period_end)
);

CREATE TABLE daily_summary (
    date                         TEXT PRIMARY KEY,
    spend_complete               INTEGER DEFAULT 0,
    pi_complete                  INTEGER DEFAULT 0,
    watchdog_heartbeats_received INTEGER,
    avg_w_pi                     REAL
);
"""


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "travel.db"
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    return path


def _conn(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _add_transaction(conn, bank, timestamp, tx_id="tx1", source="acct", currency="GBP", amount=-1.0):
    conn.execute(
        "INSERT INTO transactions (id, source, bank, timestamp, amount, currency) VALUES (?, ?, ?, ?, ?, ?)",
        (tx_id, source, bank, timestamp, amount, currency),
    )


# ---------------------------------------------------------------------------
# reconstruct_upload_log
# ---------------------------------------------------------------------------

def test_reconstruct_inserts_row_for_month_with_transactions(db_path):
    conn = _conn(db_path)
    _add_transaction(conn, "Revolut", "2026-06-15T10:00:00Z")
    conn.commit()

    summary = reconstruct_upload_log(conn, date(2026, 6, 1), date(2026, 6, 30))
    conn.commit()

    rows = conn.execute("SELECT * FROM upload_log").fetchall()
    assert len(rows) == 1
    assert rows[0]["source"] == "revolut"
    assert rows[0]["period_start"] == "2026-06-01"
    assert rows[0]["period_end"] == "2026-06-30"
    assert rows[0]["row_count"] == 1
    assert rows[0]["inferred"] == 1
    assert rows[0]["uploaded_at"] is None
    assert len(summary["inserted"]) == 1


def test_reconstruct_does_not_insert_zero_transaction_months(db_path):
    conn = _conn(db_path)
    summary = reconstruct_upload_log(conn, date(2026, 6, 1), date(2026, 6, 30))
    conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM upload_log").fetchone()[0] == 0
    ambiguous_sources = {s for s, _ in summary["ambiguous"]}
    assert ambiguous_sources == {"revolut", "wise"}


def test_reconstruct_separates_revolut_and_wise_by_bank_not_source(db_path):
    """transactions.source for Wise is a per-account id (e.g. '137103728_USD'),
    not the literal 'wise' — reconstruction must key off `bank`, not `source`."""
    conn = _conn(db_path)
    _add_transaction(conn, "Wise", "2026-06-15T10:00:00Z", source="137103728_USD")
    conn.commit()

    reconstruct_upload_log(conn, date(2026, 6, 1), date(2026, 6, 30))
    conn.commit()

    rows = conn.execute("SELECT * FROM upload_log").fetchall()
    assert len(rows) == 1
    assert rows[0]["source"] == "wise"


def test_reconstruct_skips_months_with_existing_upload_log_row(db_path):
    conn = _conn(db_path)
    _add_transaction(conn, "Revolut", "2026-06-15T10:00:00Z")
    conn.execute("""
        INSERT INTO upload_log (source, period_start, period_end, row_count, inferred, uploaded_at)
        VALUES ('revolut', '2026-06-01', '2026-06-30', 999, 0, '2026-06-16T00:00:00Z')
    """)
    conn.commit()

    reconstruct_upload_log(conn, date(2026, 6, 1), date(2026, 6, 30))
    conn.commit()

    rows = conn.execute("SELECT * FROM upload_log").fetchall()
    assert len(rows) == 1
    assert rows[0]["row_count"] == 999  # untouched — pre-existing row wins


# ---------------------------------------------------------------------------
# recompute_spend_complete / recompute_pi_complete
# ---------------------------------------------------------------------------

def test_recompute_spend_complete_flips_0_to_1_when_both_sources_covered(db_path):
    conn = _conn(db_path)
    conn.execute("INSERT INTO daily_summary (date, spend_complete) VALUES ('2026-06-15', 0)")
    conn.execute("""
        INSERT INTO upload_log (source, period_start, period_end, row_count, inferred)
        VALUES ('revolut', '2026-06-01', '2026-06-30', 5, 0),
               ('wise',    '2026-06-01', '2026-06-30', 3, 0)
    """)
    conn.commit()

    flips = recompute_spend_complete(conn)
    conn.commit()

    assert flips == {"0_to_1": 1, "1_to_0": 0, "unchanged": 0}
    row = conn.execute("SELECT spend_complete FROM daily_summary WHERE date = '2026-06-15'").fetchone()
    assert row["spend_complete"] == 1


def test_recompute_spend_complete_flips_1_to_0_when_incorrectly_marked(db_path):
    """Simulates the old bug: a date force-marked complete without upload_log coverage."""
    conn = _conn(db_path)
    conn.execute("INSERT INTO daily_summary (date, spend_complete) VALUES ('2026-06-15', 1)")
    conn.commit()

    flips = recompute_spend_complete(conn)
    conn.commit()

    assert flips == {"0_to_1": 0, "1_to_0": 1, "unchanged": 0}
    row = conn.execute("SELECT spend_complete FROM daily_summary WHERE date = '2026-06-15'").fetchone()
    assert row["spend_complete"] == 0


def test_recompute_pi_complete_flips_0_to_1_when_data_present(db_path):
    conn = _conn(db_path)
    conn.execute("""
        INSERT INTO daily_summary (date, pi_complete, watchdog_heartbeats_received, avg_w_pi)
        VALUES ('2026-06-15', 0, 12, NULL)
    """)
    conn.commit()

    flips = recompute_pi_complete(conn)
    conn.commit()

    assert flips == {"0_to_1": 1, "1_to_0": 0, "unchanged": 0}
    row = conn.execute("SELECT pi_complete FROM daily_summary WHERE date = '2026-06-15'").fetchone()
    assert row["pi_complete"] == 1


def test_recompute_pi_complete_flips_1_to_0_when_data_absent_and_recent(db_path):
    """Simulates the old bug: closed_after(2) marked it complete before data ever arrived."""
    from datetime import timedelta
    recent_date = (date.today() - timedelta(days=1)).isoformat()
    conn = _conn(db_path)
    conn.execute("""
        INSERT INTO daily_summary (date, pi_complete, watchdog_heartbeats_received, avg_w_pi)
        VALUES (?, 1, 0, NULL)
    """, (recent_date,))
    conn.commit()

    flips = recompute_pi_complete(conn)
    conn.commit()

    assert flips == {"0_to_1": 0, "1_to_0": 1, "unchanged": 0}


# ---------------------------------------------------------------------------
# main() — dry-run vs --apply
# ---------------------------------------------------------------------------

def _run_main(monkeypatch, db_path, extra_args=None):
    argv = [
        "backfill_upload_log_and_completeness.py",
        "--db", str(db_path),
        "--start-date", "2026-06-01",
        "--end-date", "2026-06-30",
        *(extra_args or []),
    ]
    monkeypatch.setattr(sys, "argv", argv)
    main()


def test_dry_run_writes_nothing(db_path, monkeypatch, capsys):
    conn = _conn(db_path)
    _add_transaction(conn, "Revolut", "2026-06-15T10:00:00Z")
    conn.commit()
    conn.close()

    _run_main(monkeypatch, db_path)

    out = capsys.readouterr().out
    assert "DRY-RUN" in out
    assert "upload_log +=" in out

    conn = _conn(db_path)
    assert conn.execute("SELECT COUNT(*) FROM upload_log").fetchone()[0] == 0


def test_apply_writes_changes(db_path, monkeypatch, capsys):
    conn = _conn(db_path)
    _add_transaction(conn, "Revolut", "2026-06-15T10:00:00Z")
    conn.execute("INSERT INTO daily_summary (date, spend_complete) VALUES ('2026-06-15', 0)")
    conn.commit()
    conn.close()

    _run_main(monkeypatch, db_path, extra_args=["--apply"])

    out = capsys.readouterr().out
    assert "APPLY" in out
    assert "Changes committed." in out

    conn = _conn(db_path)
    rows = conn.execute("SELECT * FROM upload_log WHERE source = 'revolut'").fetchall()
    assert len(rows) == 1
    assert rows[0]["inferred"] == 1


def test_dry_run_then_apply_produce_same_planned_result(db_path, monkeypatch, capsys):
    """Dry-run output should reflect exactly what --apply would do, since both
    run inside a transaction and only differ in commit vs rollback."""
    conn = _conn(db_path)
    _add_transaction(conn, "Revolut", "2026-06-15T10:00:00Z")
    _add_transaction(conn, "Wise", "2026-06-16T10:00:00Z", source="acct2")
    conn.commit()
    conn.close()

    _run_main(monkeypatch, db_path)
    dry_out = capsys.readouterr().out

    _run_main(monkeypatch, db_path, extra_args=["--apply"])
    apply_out = capsys.readouterr().out

    dry_lines = {line for line in dry_out.splitlines() if "upload_log +=" in line}
    apply_lines = {line for line in apply_out.splitlines() if "upload_log +=" in line}
    assert dry_lines == apply_lines
    assert len(dry_lines) == 2
