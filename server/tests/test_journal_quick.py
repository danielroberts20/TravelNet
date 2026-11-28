"""POST /upload/journal/quick and the enrichment behind it."""
import json
import sqlite3
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import require_upload_token
from database.journal.table import JournalTable
from upload.journal import router as journal_router_mod

NOW = datetime(2026, 11, 28, 10, 31, tzinfo=timezone.utc)   # 21:31 in Melbourne

SCHEMA = """
CREATE TABLE places (id INTEGER PRIMARY KEY, country_code TEXT, country TEXT, region TEXT,
    locality TEXT, city TEXT, timezone TEXT);
CREATE TABLE location_unified (timestamp TEXT, latitude REAL, longitude REAL, place_id INTEGER);
CREATE TABLE transition_timezone (transitioned_at TEXT, to_tz TEXT);
CREATE TABLE weather_hourly (timestamp TEXT, latitude REAL, longitude REAL,
    temperature_c REAL, weathercode INTEGER);
CREATE TABLE health_quantity (timestamp TEXT, metric TEXT, value REAL, source TEXT);
CREATE TABLE transactions (timestamp TEXT, amount_gbp REAL, is_internal INTEGER DEFAULT 0,
    is_interest INTEGER DEFAULT 0, state TEXT);
INSERT INTO transition_timezone VALUES ('2020-01-01T00:00:00Z', 'Europe/London');
"""


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "t.db"
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit(); conn.close()

    def _get_conn(*a, **k):
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys = ON;")
        return c

    # create journal_entries through the real table definition
    with patch("database.journal.table.get_conn", _get_conn):
        JournalTable().init()
    # Background work (daily_summary refresh + Trevor push) is covered in test_journal_sync.py.
    with patch.object(journal_router_mod, "get_conn", _get_conn), \
         patch.object(journal_router_mod, "JOURNAL_LATEST_FILE", tmp_path / "latest.json"), \
         patch.object(journal_router_mod, "_after_save") as after:
        _get_conn.after_save = after
        yield _get_conn


@pytest.fixture
def client(db):
    app = FastAPI()
    app.include_router(journal_router_mod.router, prefix="/upload/journal")
    app.dependency_overrides[require_upload_token] = lambda: None
    return TestClient(app)


def _melbourne_context(get_conn):
    c = get_conn()
    c.execute("INSERT INTO places VALUES (1,'AU','Australia','Victoria','St Kilda','Melbourne','Australia/Melbourne')")
    c.execute("INSERT INTO location_unified VALUES ('2026-11-28T10:30:00Z',-37.867,144.98,1)")
    c.execute("INSERT INTO weather_hourly VALUES ('2026-11-28T10:00:00Z',-37.9,145.0,19.5,3)")
    # local midnight Melbourne = 2026-11-27T13:00Z
    c.execute("INSERT INTO health_quantity VALUES ('2026-11-28T01:00:00Z','Step Count',4000,'Dan Watch')")
    c.execute("INSERT INTO health_quantity VALUES ('2026-11-28T01:00:00Z','Step Count',4000,'Dan iPhone')")  # dup
    c.execute("INSERT INTO health_quantity VALUES ('2026-11-27T05:00:00Z','Step Count',999,'Dan Watch')")   # yesterday
    c.execute("INSERT INTO transactions VALUES ('2026-11-28T03:00:00Z',-12.5,0,0,'COMPLETED')")
    c.execute("INSERT INTO transactions VALUES ('2026-11-28T03:00:00Z',50,0,0,'COMPLETED')")   # credit
    c.execute("INSERT INTO transactions VALUES ('2026-11-28T03:00:00Z',-9,1,0,'COMPLETED')")   # internal
    c.commit(); c.close()


def _post(client, **body):
    body.setdefault("timestamp", NOW.isoformat())
    return client.post("/upload/journal/quick", json=body)


def _rows(get_conn):
    c = get_conn()
    rows = c.execute("SELECT * FROM journal_entries").fetchall()
    c.close()
    return rows


def test_quick_entry_is_enriched_from_travelnet_data(client, db):
    _melbourne_context(db)
    r = _post(client, text="  Good day at the beach  ", mood=4, rating=8, energy=3, tags="beach, sun")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["provenance"] == "contemporaneous" and body["local_date"] == "2026-11-28"
    assert {"location", "weather", "steps_so_far", "spend_gbp_so_far"} <= set(body["enriched"])

    (row,) = _rows(db)
    assert row["text"] == "Good day at the beach"
    assert row["tz"] == "Australia/Melbourne" and row["utc_offset"] == "+1100"
    assert row["valence_self"] == 0.5 and row["mood_self"] == 4
    assert row["place_id"] == 1 and row["latitude"] == pytest.approx(-37.867)
    e = json.loads(row["enrichment_json"])
    assert e["steps_so_far"] == 4000          # Watch/iPhone duplicate and yesterday excluded
    assert e["spend_gbp_so_far"] == 12.5      # credit and internal excluded
    assert e["weather"]["temp_c"] == 19.5


def test_enrichment_missing_never_blocks_capture(client, db):
    r = _post(client, text="nothing known about where I am")
    assert r.status_code == 200
    (row,) = _rows(db)
    assert row["tz"] == "Europe/London" and row["place_id"] is None
    assert json.loads(row["enrichment_json"]) == {}


def test_resending_same_client_id_is_a_noop(client, db):
    assert _post(client, text="one", client_id="abc").json()["duplicate"] is False
    assert _post(client, text="one", client_id="abc").json()["duplicate"] is True
    assert len(_rows(db)) == 1


def test_retro_entry_is_tagged_and_not_enriched_with_current_context(client, db):
    _melbourne_context(db)
    r = _post(client, text="Fiji boat trip, remembering it now", covers_date="2026-09-23")
    assert r.json()["provenance"] == "retro"
    (row,) = _rows(db)
    assert row["source"] == "retro" and row["lag_days"] == 66
    assert row["covers_start"] == row["covers_end"] == "2026-09-23"
    assert row["place_id"] is None and json.loads(row["enrichment_json"]) == {}


def test_covers_date_in_future_rejected(client, db):
    assert _post(client, text="x", covers_date="2026-12-01").status_code == 400


@pytest.mark.parametrize("bad", [{"text": ""}, {"text": "x", "mood": 6}, {"text": "x", "rating": 11}])
def test_validation(client, bad):
    assert _post(client, **bad).status_code == 422


def test_naive_timestamp_uses_current_timezone(client, db):
    _post(client, text="late night", timestamp="2026-11-28T23:30:00")   # London in this DB
    (row,) = _rows(db)
    assert row["ts_utc"] == "2026-11-28T23:30:00Z" and row["local_date"] == "2026-11-28"


def test_contemporaneous_entry_updates_journal_latest(client, db, tmp_path):
    _melbourne_context(db)
    _post(client, text="x")
    latest = json.loads((tmp_path / "latest.json").read_text())
    assert latest["timestamp"] == "2026-11-28T21:31:00+11:00"


def test_retro_entry_does_not_touch_journal_latest(client, db, tmp_path):
    _post(client, text="x", covers_date="2026-11-01")
    assert not (tmp_path / "latest.json").exists()


def test_blank_optional_fields_from_shortcuts_are_treated_as_unset(client, db):
    r = client.post("/upload/journal/quick", json={
        "text": "skipped everything", "timestamp": "", "covers_date": "", "mood": "",
        "rating": "0", "energy": 0, "tags": "  ", "client_id": "",
    })
    assert r.status_code == 200, r.text
    (row,) = _rows(db)
    assert row["mood_self"] is None and row["rating"] is None and row["energy"] is None
    assert row["tags"] is None and row["provenance"] == "contemporaneous"


def test_numbers_sent_as_strings_are_accepted(client, db):
    assert _post(client, text="x", mood="4", rating="7", energy="3").status_code == 200
    (row,) = _rows(db)
    assert (row["mood_self"], row["rating"], row["energy"]) == (4, 7, 3)


def test_saved_entry_schedules_background_sync_for_its_days(client, db):
    _post(client, text="x", covers_date="2026-11-20")
    ((ids, days), _), = db.after_save.call_args_list
    assert len(ids) == 1 and days == ["2026-11-20"]


def test_duplicate_does_not_reschedule_sync(client, db):
    _post(client, text="x", client_id="a"); _post(client, text="x", client_id="a")
    assert db.after_save.call_count == 1


def test_full_entry_spanning_days_is_retro_with_folded_oneliners(client, db):
    r = _post(client, text="A long weekend.", full=True, covers_start="2026-11-25", covers_end="2026-11-28",
              best_moment="the beach", low_moment="None", who_with="kate", rating=7)
    assert r.status_code == 200, r.text
    (row,) = _rows(db)
    assert row["source"] == "full" and row["provenance"] == "retro"
    assert (row["covers_start"], row["covers_end"], row["lag_days"]) == ("2026-11-25", "2026-11-28", 0)
    assert row["text"] == "A long weekend.\n\nBest moment: the beach\nWho with: kate"
    ((_, days), _), = db.after_save.call_args_list
    assert days == ["2026-11-25", "2026-11-26", "2026-11-27", "2026-11-28"]


def test_range_validation(client, db):
    assert _post(client, text="x", covers_start="2026-11-20", covers_end="2026-11-10").status_code == 400
    assert _post(client, text="x", covers_start="2026-11-01", covers_end="2026-11-28").status_code == 400  # > 14 days
    assert _post(client, text="x", covers_end="2026-12-05").status_code == 400                              # future


def test_covers_end_alone_means_a_single_day(client, db):
    assert _post(client, text="x", covers_end="2026-11-26").status_code == 200
    (row,) = _rows(db)
    assert row["covers_start"] == row["covers_end"] == "2026-11-26"
