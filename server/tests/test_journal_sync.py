"""Mood resolution, Trevor push payloads, Apple import, and the journal daily-summary domain."""
import json
import sqlite3
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth import require_upload_token
from database.journal import sync as sync_mod
from database.journal.mood import find_healthkit_mood, resolve_mood
from database.journal.sync import build_trevor_entry, push_to_trevor, reconcile_all
from database.journal.table import JournalTable
from scheduled_tasks.daily_summary.journal import compute_journal_data
from upload.journal import router as router_mod

SCHEMA = """
CREATE TABLE places (id INTEGER PRIMARY KEY);
CREATE TABLE state_of_mind (id TEXT PRIMARY KEY, kind TEXT, start_ts TEXT, end_ts TEXT,
    valence REAL, classification TEXT);
CREATE TABLE mood_labels (som_id TEXT, label TEXT);
CREATE TABLE mood_associations (som_id TEXT, association TEXT);
"""


def _utc(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


@pytest.fixture
def get_conn(tmp_path):
    path = tmp_path / "t.db"
    c = sqlite3.connect(path); c.executescript(SCHEMA); c.commit(); c.close()

    def _get_conn(*a, **k):
        c = sqlite3.connect(path); c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys = ON;")
        return c

    with patch("database.journal.table.get_conn", _get_conn):
        JournalTable().init()
    return _get_conn


def _som(conn, id, ts, valence=0.5, cls="pleasant", labels=("content",), assoc=("travel",)):
    conn.execute("INSERT INTO state_of_mind VALUES (?,?,?,?,?,?)", (id, "k", ts, ts, valence, cls))
    conn.executemany("INSERT INTO mood_labels VALUES (?,?)", [(id, l) for l in labels])
    conn.executemany("INSERT INTO mood_associations VALUES (?,?)", [(id, a) for a in assoc])


def _entry(conn, id="e1", ts="2026-09-05T03:04:02Z", provenance="contemporaneous", valence_self=None,
           covers=("2026-09-04", "2026-09-04"), extra=None, source="quick"):
    conn.execute("""INSERT INTO journal_entries
        (id, ts_utc, local_date, source, provenance, text, covers_start, covers_end, valence_self,
         mood_self, enrichment_json, lag_days, latitude, longitude)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (id, ts, "2026-09-04", source, provenance, "text " + id, covers[0], covers[1], valence_self,
         None if valence_self is None else 4, json.dumps(extra or {}), None, 47.6, -122.3))
    conn.commit()
    return conn.execute("SELECT * FROM journal_entries WHERE id = ?", (id,)).fetchone()


# ---- mood ------------------------------------------------------------------

def test_healthkit_wins_over_self_tap(get_conn):
    c = get_conn(); _som(c, "a", "2026-09-05T03:04:11Z", 0.54, "pleasant", ("content", "passionate"))
    m = resolve_mood(c, _entry(c, valence_self=1.0))
    assert (m["valence"], m["valence_source"], m["valence_class"]) == (0.54, "healthkit", "pleasant")
    assert m["labels"] == ["content", "passionate"] and m["associations"] == ["travel"]


def test_self_tap_is_the_fallback_and_unknown_is_none(get_conn):
    c = get_conn()
    m = resolve_mood(c, _entry(c, valence_self=0.5))
    assert (m["valence"], m["valence_source"]) == (0.5, "self_tap")
    m = resolve_mood(c, _entry(c, id="e2", ts="2026-09-06T03:04:02Z"))
    assert (m["valence"], m["valence_source"]) == (None, None)


def test_window_is_forward_45_minutes_and_prefers_closest(get_conn):
    c = get_conn()
    _som(c, "before", "2026-09-05T03:03:00Z"); _som(c, "late", "2026-09-05T03:50:00Z")
    assert find_healthkit_mood(c, _utc("2026-09-05T03:04:02Z")) is None
    _som(c, "far", "2026-09-05T03:40:00Z", labels=("happy",))
    _som(c, "near", "2026-09-05T03:04:30Z", labels=("sad",))
    assert find_healthkit_mood(c, _utc("2026-09-05T03:04:02Z"), ["happy"])["som_id"] == "near"


def test_retro_entries_never_join_healthkit(get_conn):
    c = get_conn(); _som(c, "a", "2026-09-05T03:04:11Z")
    m = resolve_mood(c, _entry(c, provenance="retro", valence_self=-0.5))
    assert (m["valence"], m["valence_source"]) == (-0.5, "self_tap")


def test_apple_sidecar_labels_used_when_no_healthkit(get_conn):
    c = get_conn()
    m = resolve_mood(c, _entry(c, extra={"mood": {"labels": ["Content"], "associations": ["Hobbies"]}}))
    assert m["labels"] == ["Content"] and m["associations"] == ["Hobbies"]


# ---- payload / push --------------------------------------------------------

def test_trevor_entry_payload(get_conn):
    c = get_conn(); _som(c, "a", "2026-09-05T03:04:11Z")
    row = _entry(c, extra={"location": {"town": "Seattle", "country": "United States"}})
    p = build_trevor_entry(c, row)
    assert p["town"] == "Seattle" and p["country"] == "United States"
    assert p["covers_start"] == "2026-09-04" and p["valence_source"] == "healthkit"
    assert p["provenance"] == "contemporaneous" and p["latitude"] == 47.6
    json.dumps(p)   # must be serialisable


def test_push_sends_key_and_payload():
    resp = MagicMock(status_code=200)
    with patch.object(sync_mod.httpx, "post", return_value=resp) as post:
        assert push_to_trevor([{"id": "x"}], reconcile=True) is True
    (url,), kw = post.call_args
    assert url.endswith("/ingest/entries") and kw["json"] == {"entries": [{"id": "x"}], "reconcile": True}
    assert "x-api-key" in kw["headers"]


def test_push_failure_is_swallowed_and_empty_reconcile_refused():
    with patch.object(sync_mod.httpx, "post", side_effect=OSError("down")):
        assert push_to_trevor([{"id": "x"}]) is False
    with patch.object(sync_mod.httpx, "post") as post:
        assert push_to_trevor([], reconcile=True) is False
        post.assert_not_called()


def test_reconcile_all_sends_everything(get_conn):
    c = get_conn(); _entry(c, "e1"); _entry(c, "e2", ts="2026-09-06T03:04:02Z")
    with patch.object(sync_mod, "get_conn", get_conn), \
         patch.object(sync_mod, "push_to_trevor", return_value=True) as push:
        assert reconcile_all() == 2
    sent, = push.call_args.args
    assert [e["id"] for e in sent] == ["e1", "e2"] and push.call_args.kwargs["reconcile"] is True


# ---- Apple import endpoint -------------------------------------------------

@pytest.fixture
def client(get_conn):
    app = FastAPI()
    app.include_router(router_mod.router, prefix="/upload/journal")
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch.object(router_mod, "get_conn", get_conn), patch.object(router_mod, "_after_save") as after:
        client = TestClient(app); client.after = after; client.get_conn = get_conn
        yield client


def _imp(key="2026-09-05_2026-09-04-Fri_20_04_02_-0700", **kw):
    return {"import_key": key, "ts_utc": "2026-09-05T03:04:02Z", "local_date": "2026-09-04",
            "utc_offset": "-0700", "text": "Hiked.\n\nBest moment: falls", "rating": 8, "energy": 4,
            "tags": "hike", "latitude": 47.6, "longitude": -122.3,
            "enrichment": {"location": {"town": "Seattle"}}, **kw}


def test_import_is_idempotent_and_marks_source(client):
    r = client.post("/upload/journal/import", json={"entries": [_imp()]})
    assert r.json() == {"status": "success", "received": 1, "inserted": 1, "skipped": 0}
    r = client.post("/upload/journal/import", json={"entries": [_imp()]})
    assert r.json()["inserted"] == 0 and r.json()["skipped"] == 1
    row = client.get_conn().execute("SELECT * FROM journal_entries").fetchone()
    assert row["id"].startswith("apple:") and row["source"] == "import"
    assert row["provenance"] == "contemporaneous" and row["covers_start"] == "2026-09-04"
    assert client.after.call_count == 1       # second (duplicate) call schedules nothing


def test_import_keeps_out_of_range_scales_in_enrichment(client):
    r = client.post("/upload/journal/import", json={"entries": [_imp(energy=8, rating=11)]})
    assert r.status_code == 200 and r.json()["inserted"] == 1
    row = client.get_conn().execute("SELECT * FROM journal_entries").fetchone()
    assert row["energy"] is None and row["rating"] is None
    assert json.loads(row["enrichment_json"])["energy_raw"] == 8
    assert json.loads(row["enrichment_json"])["rating_raw"] == 11


def test_journal_entries_is_never_prunable():
    from database.pruning import EXCLUDE_TABLES
    assert "journal_entries" in EXCLUDE_TABLES


def test_import_rejects_bad_dates(client):
    assert client.post("/upload/journal/import", json={"entries": [_imp(local_date="nope")]}).status_code == 400


# ---- daily summary journal domain -----------------------------------------

def test_journal_domain_counts_covering_entries(get_conn):
    c = get_conn()
    _entry(c, "live", covers=("2026-09-04", "2026-09-04"))
    _entry(c, "retro", provenance="retro", covers=("2026-09-01", "2026-09-04"))
    _entry(c, "draft", provenance="drafted_from_telemetry", covers=("2026-09-10", "2026-09-10"))
    assert compute_journal_data(c, {"date": "2026-09-04"}) == \
        {"journal_written": 1, "journal_entry_count": 2, "journal_retro": 0}
    assert compute_journal_data(c, {"date": "2026-09-02"}) == \
        {"journal_written": 1, "journal_entry_count": 1, "journal_retro": 1}
    assert compute_journal_data(c, {"date": "2026-09-10"}) == \
        {"journal_written": 0, "journal_entry_count": 0, "journal_retro": 0}
