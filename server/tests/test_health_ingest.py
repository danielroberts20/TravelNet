"""Tests for health_gaps/ingest.py and the recovery-aware report wording."""

import json
from datetime import datetime, timezone

from health_gaps.events import Event, Report
from health_gaps.ingest import backup_payload, make_ingest
from health_gaps.report import build_body, subject

DATA = {"metrics": [{"name": "heart_rate", "units": "count/min", "data": [{"date": "2026-11-24 09:49:21 +1100", "Avg": 55}]}]}


def test_backup_matches_the_rest_route_layout(tmp_path):
    p = backup_payload(DATA, tmp_path / "health", datetime(2026, 11, 24, 12, 0))
    assert p.name == "2026-11-24.jsonl"
    assert json.loads(p.read_text().splitlines()[0]) == {"data": DATA}


def test_backup_appends_one_line_per_payload(tmp_path):
    when = datetime(2026, 11, 24)
    backup_payload(DATA, tmp_path, when)
    backup_payload(DATA, tmp_path, when)
    assert len((tmp_path / "2026-11-24.jsonl").read_text().splitlines()) == 2


def test_ingest_backs_up_then_hands_to_the_handler(tmp_path):
    seen = []
    ingest = make_ingest(tmp_path, handler=lambda d: seen.append(("handled", (tmp_path.glob("*.jsonl").__next__().exists()))))
    ingest(DATA)
    assert seen == [("handled", True)]            # the backup already existed when the handler ran


def test_ingest_writes_backup_even_if_the_handler_raises(tmp_path):
    def boom(_):
        raise RuntimeError("db locked")
    ingest = make_ingest(tmp_path, handler=boom)
    try:
        ingest(DATA)
    except RuntimeError:
        pass
    assert list(tmp_path.glob("*.jsonl"))


def _ev(i=1):
    return Event(id=i, group_key="g", metrics={"Step Count": ["2026-11-20T00:00:00Z", "2026-11-21T00:00:00Z"]},
                 window_start=datetime(2026, 11, 20, tzinfo=timezone.utc), window_end=datetime(2026, 11, 21, tzinfo=timezone.utc),
                 status="confirmed", critical=False, immediate=True, notified_status=None, pushed_at=None, exported_at=None)


def test_report_wording_switches_with_auto_recovery():
    r = Report(newly_confirmed=[_ev()], awaiting_export=[_ev()])
    now = datetime(2026, 11, 24, tzinfo=timezone.utc)
    manual = build_body(r, now, False, 2.0)
    auto = build_body(r, now, False, 2.0, auto_recovery=True)
    assert "re-export from Health Auto Export" in manual and "After exporting" in manual
    assert "Pushcut notification will ask you to start the HAE server" in auto
    assert "re-export from Health Auto Export" not in auto and "Manual fallback" in auto
    assert "awaiting export" in subject(r, False) and "awaiting recovery" in subject(r, False, True)
