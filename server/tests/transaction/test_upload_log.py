"""
test_upload_log.py — Tests for database/transaction/upload_log.py (UploadLogTable).
"""

from unittest.mock import patch

from conftest import db, upload_log_rows

from database.transaction.upload_log import UploadLogRecord, UploadLogTable


def insert(db, **kwargs):
    record = UploadLogRecord(
        source=kwargs.get("source", "revolut"),
        period_start=kwargs.get("period_start", "2026-03-01"),
        period_end=kwargs.get("period_end", "2026-03-31"),
        row_count=kwargs.get("row_count", 10),
    )
    with patch("database.transaction.upload_log.get_conn", return_value=db):
        UploadLogTable().insert(record)


def test_insert_creates_row(db):
    insert(db, source="revolut", row_count=5)
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["source"] == "revolut"
    assert rows[0]["row_count"] == 5


def test_insert_sets_uploaded_at(db):
    insert(db)
    rows = upload_log_rows(db)
    assert rows[0]["uploaded_at"] is not None


def test_insert_sets_inferred_zero(db):
    """insert() always represents a genuine upload event, never a backfill guess."""
    insert(db)
    rows = upload_log_rows(db)
    assert rows[0]["inferred"] == 0


def test_insert_promotes_previously_inferred_row(db):
    """A row reconstructed by the historical backfill (inferred=1, uploaded_at=NULL)
    must be promoted to a real one if that period is later actually uploaded."""
    db.execute("""
        INSERT INTO upload_log (source, period_start, period_end, row_count, inferred, uploaded_at)
        VALUES ('revolut', '2026-03-01', '2026-03-31', 7, 1, NULL)
    """)
    insert(db, source="revolut", period_start="2026-03-01", period_end="2026-03-31", row_count=9)
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["inferred"] == 0
    assert rows[0]["row_count"] == 9
    assert rows[0]["uploaded_at"] is not None


def test_reupload_same_period_updates_row_count(db):
    insert(db, source="revolut", period_start="2026-03-01", period_end="2026-03-31", row_count=5)
    insert(db, source="revolut", period_start="2026-03-01", period_end="2026-03-31", row_count=9)
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["row_count"] == 9


def test_reupload_updates_uploaded_at(db):
    insert(db, source="revolut", row_count=5)
    first_uploaded_at = upload_log_rows(db)[0]["uploaded_at"]

    with patch("database.transaction.upload_log.get_conn", return_value=db):
        db.execute(
            "UPDATE upload_log SET uploaded_at = '2020-01-01T00:00:00Z' WHERE source = 'revolut'"
        )
    insert(db, source="revolut", row_count=6)
    second_uploaded_at = upload_log_rows(db)[0]["uploaded_at"]
    assert second_uploaded_at != "2020-01-01T00:00:00Z"


def test_zero_row_count_is_valid(db):
    insert(db, row_count=0)
    rows = upload_log_rows(db)
    assert len(rows) == 1
    assert rows[0]["row_count"] == 0


def test_different_sources_same_period_are_separate_rows(db):
    insert(db, source="revolut", period_start="2026-03-01", period_end="2026-03-31", row_count=5)
    insert(db, source="wise", period_start="2026-03-01", period_end="2026-03-31", row_count=8)
    rows = upload_log_rows(db)
    assert len(rows) == 2
    assert {r["source"] for r in rows} == {"revolut", "wise"}


def test_different_periods_same_source_are_separate_rows(db):
    insert(db, source="revolut", period_start="2026-03-01", period_end="2026-03-31", row_count=5)
    insert(db, source="revolut", period_start="2026-04-01", period_end="2026-04-30", row_count=8)
    rows = upload_log_rows(db)
    assert len(rows) == 2
