"""Dashboard journal management: day cards, day detail, entry edit/delete/export."""
import json
import sqlite3
from datetime import date, datetime
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import config.general as cfg
from auth import require_upload_token
from database.journal import manage
from database.journal.table import JournalTable
from upload.journal import manage as manage_routes

SCHEMA = """
CREATE TABLE places (id INTEGER PRIMARY KEY, city TEXT, locality TEXT);
CREATE TABLE transition_timezone (transitioned_at TEXT, to_tz TEXT, to_offset TEXT);
INSERT INTO transition_timezone VALUES ('2026-09-11T00:00:00Z', 'Australia/Melbourne', '+11:00');
CREATE TABLE daily_summary (date TEXT UNIQUE, utc_start TEXT, utc_end TEXT, timezone TEXT, country TEXT, city TEXT, steps INTEGER, distance_km REAL,
    spend_gbp REAL, transaction_count INTEGER, photo_count INTEGER, temp_max_c REAL, temp_min_c REAL,
    weathercode INTEGER, sleep_hours REAL, was_in_transit INTEGER, distinct_places INTEGER,
    new_places_visited INTEGER, avg_valence REAL, mood_entries INTEGER, travel_phase TEXT, day_label TEXT);
CREATE TABLE location_unified (timestamp TEXT, latitude REAL, longitude REAL, place_id INTEGER);
CREATE TABLE known_places (id INTEGER PRIMARY KEY, label TEXT, latitude REAL, longitude REAL, place_id INTEGER);
CREATE TABLE place_visits (id INTEGER PRIMARY KEY, known_place_id INTEGER, arrived_at TEXT, departed_at TEXT,
    duration_mins INTEGER, notes TEXT, superseded_by INTEGER);
CREATE TABLE transactions (timestamp TEXT, merchant TEXT, description TEXT, amount REAL, currency TEXT, amount_gbp REAL, category TEXT,
    is_internal INTEGER DEFAULT 0, is_interest INTEGER DEFAULT 0, state TEXT);
CREATE TABLE photo_metadata (taken_at TEXT, latitude REAL, longitude REAL, is_screenshot INTEGER DEFAULT 0,
    is_screen_recording INTEGER DEFAULT 0);
CREATE TABLE flights (origin_city TEXT, destination_city TEXT, origin_iata TEXT, destination_iata TEXT,
    departed_at TEXT, arrived_at TEXT);
"""
TODAY = date(2026, 11, 28)
START = date(2026, 11, 20)


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


@pytest.fixture
def conn(get_conn):
    c = get_conn(); yield c; c.close()


def entry(conn, id, written, start=None, end=None, provenance="contemporaneous", text="t", source="quick"):
    start = start or written; end = end or start
    conn.execute("""INSERT INTO journal_entries (id, ts_utc, local_date, source, provenance, text, covers_start,
        covers_end, lag_days) VALUES (?,?,?,?,?,?,?,?,?)""",
        (id, f"{written}T10:00:00Z", written, source, provenance, text, start, end,
         (date.fromisoformat(written) - date.fromisoformat(end)).days if provenance == "retro" else None))
    conn.commit()


def summary(conn, day, **kw):
    cols = {"date": day, "country": "Australia", "city": "Melbourne", "steps": 5000, **kw}
    conn.execute(f"INSERT INTO daily_summary ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})", list(cols.values()))
    conn.commit()


# ---- day cards ---------------------------------------------------------------

def test_gaps_exclude_days_that_count_and_include_weak_coverage(conn):
    entry(conn, "live", "2026-11-22")                                              # counts
    entry(conn, "late", "2026-11-28", start="2026-11-23", provenance="retro")      # 5 days late: does NOT count
    entry(conn, "weekly", "2026-11-28", start="2026-11-25", end="2026-11-26", provenance="retro")  # range: counts
    days = {d["date"]: d for d in manage.list_days(conn, "gaps", TODAY, START, 3)}
    assert "2026-11-22" not in days and "2026-11-25" not in days and "2026-11-26" not in days
    assert "2026-11-23" in days                                                    # weak retro entry still shows
    assert days["2026-11-23"]["entries"][0]["id"] == "late" and days["2026-11-23"]["counted"] is False
    assert "2026-11-28" not in days                                                # today is not over
    assert list(days)[0] == "2026-11-27"                                           # newest first


def test_all_mode_includes_covered_days_with_their_entries(conn):
    entry(conn, "live", "2026-11-22", text="A good day")
    days = {d["date"]: d for d in manage.list_days(conn, "all", TODAY, START, 3)}
    assert len(days) == 8                                                          # 20th..27th
    assert days["2026-11-22"]["counted"] is True and days["2026-11-22"]["entries"][0]["snippet"] == "A good day"


def test_cards_carry_summary_fields_and_tolerate_missing_summary(conn):
    summary(conn, "2026-11-27", steps=12345, spend_gbp=42.5, photo_count=3)
    days = {d["date"]: d for d in manage.list_days(conn, "gaps", TODAY, START, 3)}
    assert (days["2026-11-27"]["steps"], days["2026-11-27"]["spend_gbp"]) == (12345, 42.5)
    assert days["2026-11-26"] == {"date": "2026-11-26", "counted": False, "entries": [], "spend": []}


def test_card_spend_is_per_currency_local_first(conn):
    # windows come from daily_summary: Melbourne 2026-11-27 = 2026-11-26T13:00Z .. 2026-11-27T13:00Z
    summary(conn, "2026-11-27", utc_start="2026-11-26T13:00:00Z", utc_end="2026-11-27T13:00:00Z")
    summary(conn, "2026-11-26", utc_start="2026-11-25T13:00:00Z", utc_end="2026-11-26T13:00:00Z")
    rows = [("2026-11-27T01:00:00Z", -20, "AUD", -10.0), ("2026-11-27T02:00:00Z", -30, "AUD", -15.0),
            ("2026-11-27T03:00:00Z", -5, "USD", -4.0), ("2026-11-27T04:00:00Z", -3, "GBP", -3.0)]
    for ts, amt, cur, gbp in rows:
        conn.execute("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,0,0,'COMPLETED')", (ts, "M", "x", amt, cur, gbp, "c"))
    conn.execute("INSERT INTO transactions VALUES ('2026-11-27T05:00:00Z','M','x',-8,'EUR',NULL,'c',0,0,'COMPLETED')")  # no GBP yet
    conn.commit()
    days = {d["date"]: d for d in manage.list_days(conn, "gaps", TODAY, START, 3)}
    assert days["2026-11-27"]["spend"] == [
        {"currency": "AUD", "local": 50.0, "gbp": 25.0, "count": 2},     # most-used currency first
        {"currency": "GBP", "local": 3.0, "gbp": 3.0, "count": 1},
        {"currency": "USD", "local": 5.0, "gbp": 4.0, "count": 1}]
    assert days["2026-11-26"]["spend"] == []


def test_trip_starting_today_has_no_days(conn):
    assert manage.list_days(conn, "gaps", TODAY, TODAY, 3) == []


# ---- day detail --------------------------------------------------------------

def test_day_detail_uses_the_local_day_window(conn):
    # Melbourne 2026-11-27 = 2026-11-26T13:00Z .. 2026-11-27T13:00Z
    summary(conn, "2026-11-27")
    for i in range(1000):
        conn.execute("INSERT INTO location_unified VALUES (?,?,?,?)",
                     (f"2026-11-27T{i // 60 % 12:02d}:{i % 60:02d}:00Z", -37.8 + i * 1e-5, 144.9, 1))
    conn.execute("INSERT INTO location_unified VALUES ('2026-11-27T20:00:00Z', 0, 0, 1)")        # next local day
    conn.execute("INSERT INTO known_places VALUES (1, 'Home', -37.8, 144.9, 1)")
    conn.execute("INSERT INTO places VALUES (1, 'Melbourne', 'St Kilda')")
    conn.execute("INSERT INTO place_visits VALUES (1, 1, '2026-11-27T01:00:00Z', '2026-11-27T03:00:00Z', 120, NULL, NULL)")
    conn.execute("INSERT INTO place_visits VALUES (2, 1, '2026-11-27T05:00:00Z', NULL, NULL, NULL, 1)")  # superseded
    conn.execute("INSERT INTO transactions VALUES ('2026-11-27T02:00:00Z','Cafe','x',-9,'AUD',-4.5,'food',0,0,'COMPLETED')")
    conn.execute("INSERT INTO transactions VALUES ('2026-11-27T02:00:00Z','Refund','x',18,'AUD',9,'food',0,0,'COMPLETED')")
    conn.execute("INSERT INTO photo_metadata VALUES ('2026-11-27T04:00:00Z', -37.8, 144.9, 0, 0)")
    conn.execute("INSERT INTO photo_metadata VALUES ('2026-11-27T04:00:00Z', -37.8, 144.9, 1, 0)")   # screenshot
    conn.execute("INSERT INTO flights VALUES ('Melbourne','Sydney','MEL','SYD','2026-11-27T05:00:00Z','2026-11-27T07:00:00Z')")
    conn.commit()
    entry(conn, "e", "2026-11-28", start="2026-11-26", end="2026-11-28", provenance="retro", text="Long weekend")

    d = manage.day_detail(conn, "2026-11-27")
    assert d["utc_start"] == "2026-11-26T13:00:00Z" and d["utc_end"] == "2026-11-27T13:00:00Z"
    assert 0 < len(d["track"]) <= manage.TRACK_MAX_POINTS + 1 and [0, 0] not in d["track"]
    assert [v["label"] for v in d["visits"]] == ["Home"] and d["visits"][0]["locality"] == "St Kilda"
    assert [t["merchant"] for t in d["transactions"]] == ["Cafe"]
    assert (d["transactions"][0]["amount"], d["transactions"][0]["currency"]) == (-9, "AUD")
    assert d["spend"] == [{"currency": "AUD", "local": 9.0, "gbp": 4.5, "count": 1}]
    assert len(d["photos"]) == 1 and d["flights"][0]["destination_iata"] == "SYD"
    assert d["entries"][0]["text"] == "Long weekend"
    assert d["summary"]["timezone"] == "Australia/Melbourne"


# ---- entries -----------------------------------------------------------------

def test_list_entries_search_filter_and_literal_wildcards(conn):
    entry(conn, "a", "2026-11-21", text="100% sunny at the beach")
    entry(conn, "b", "2026-11-22", text="plain day", provenance="retro", source="retro")
    assert manage.list_entries(conn, q="100%")["total"] == 1                       # % is literal
    assert manage.list_entries(conn, q="%")["total"] == 1
    assert manage.list_entries(conn, provenance="retro")["entries"][0]["id"] == "b"
    assert manage.list_entries(conn, date_from="2026-11-22")["total"] == 1
    assert [e["id"] for e in manage.list_entries(conn)["entries"]] == ["b", "a"]    # newest first
    assert manage.list_entries(conn, limit=1, offset=1)["entries"][0]["id"] == "a"


def test_update_validates_and_delete_removes(conn):
    entry(conn, "a", "2026-11-21", text="old")
    assert manage.update_entry(conn, "a", {"text": "  new text ", "rating": 7, "tags": " x, y "}) is True
    row = manage.get_entry(conn, "a")
    assert (row["text"], row["rating"], row["tags"]) == ("new text", 7, "x, y")
    assert manage.update_entry(conn, "a", {"rating": 0}) is True and manage.get_entry(conn, "a")["rating"] is None
    for bad in ({"text": "  "}, {"rating": 11}, {"energy": 6}, {}):
        with pytest.raises(ValueError):
            manage.update_entry(conn, "a", bad)
    assert manage.update_entry(conn, "missing", {"text": "x"}) is False
    assert manage.delete_entry(conn, "a") is True and manage.delete_entry(conn, "a") is False


def test_export_formats(conn):
    entry(conn, "a", "2026-11-21", text="hello")
    content, media, name = manage.export_entries(conn, "json")
    assert json.loads(content)[0]["text"] == "hello" and media == "application/json" and name.endswith(".json")
    md, media, name = manage.export_entries(conn, "md")
    assert "## 2026-11-21" in md and "hello" in md and name.endswith(".md")


# ---- routes ------------------------------------------------------------------

@pytest.fixture
def client(get_conn):
    app = FastAPI()
    app.include_router(manage_routes.router, prefix="/upload/journal")
    app.dependency_overrides[require_upload_token] = lambda: None
    with patch.object(manage_routes, "get_conn", get_conn), patch.object(manage_routes, "push_entry_ids") as push, \
         patch.object(manage_routes, "_after_delete") as after, patch.object(manage_routes, "reconcile_all") as rec, \
         patch.object(cfg, "TRAVEL_START_DATE", datetime(2026, 6, 11)):
        c = TestClient(app); c.get_conn, c.push, c.after, c.rec = get_conn, push, after, rec
        yield c


def test_routes_validate_input(client):
    assert client.get("/upload/journal/days?mode=nope").status_code == 422
    assert client.get("/upload/journal/days/not-a-date").status_code == 400
    assert client.get("/upload/journal/entries?date_from=soon").status_code == 400
    assert client.get("/upload/journal/export?format=pdf").status_code == 422


def test_days_route_returns_cards(client):
    r = client.get("/upload/journal/days?mode=gaps")
    assert r.status_code == 200 and r.json()["count"] == len(r.json()["days"]) > 100


def test_patch_and_delete_routes_trigger_sync(client):
    c = client.get_conn()
    entry(c, "a", "2026-11-21", text="old"); c.close()
    assert client.patch("/upload/journal/entries/a", json={"text": "edited"}).status_code == 200
    client.push.assert_called_once_with(["a"])
    assert client.patch("/upload/journal/entries/a", json={"rating": 99}).status_code == 400
    assert client.patch("/upload/journal/entries/nope", json={"text": "x"}).status_code == 404
    assert client.delete("/upload/journal/entries/nope").status_code == 404
    assert client.delete("/upload/journal/entries/a").status_code == 200
    ((days,), _), = client.after.call_args_list
    assert days == ["2026-11-21"]


def test_sync_and_export_routes(client):
    r = client.post("/upload/journal/sync")
    assert r.json() == {"status": "accepted", "entries": 0}
    client.rec.assert_called_once()
    r = client.get("/upload/journal/export?format=md")
    assert r.status_code == 200 and "attachment" in r.headers["content-disposition"]
