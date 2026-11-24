"""Tests for health_gaps/pull.py. A fake client stands in for the phone."""

from datetime import datetime, timedelta, timezone

import pytest

from health_gaps.events import Event
from health_gaps.hae_client import HAEAuthError, HAEProtocolError, HAEUnreachable
from health_gaps.pull import metric_window, pull_event

UTC = timezone.utc


def T(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


def event(metrics, start="2026-11-20 06:00", end="2026-11-20 18:00", eid=7):
    return Event(id=eid, group_key="g", metrics=metrics, window_start=T(start),
                 window_end=T(end) if end else None, status="confirmed", critical=False,
                 immediate=True, notified_status=None, pushed_at=None, exported_at=None)


NOW = T("2026-11-24 12:00")


def points(n):
    return {"metrics": [{"name": "x", "units": "u", "data": [{"date": "d"}] * n}]}


class FakeClient:
    """script maps (metric, call_index) -> payload | Exception; default is `default`."""
    def __init__(self, default=None, script=None):
        self.default = points(0) if default is None else default
        self.script = script or {}
        self.calls: list[tuple[str, datetime, datetime]] = []
        self.per_metric: dict[str, int] = {}

    def get_health_metrics(self, metric, start, end, **_):
        self.calls.append((metric, start, end))
        i = self.per_metric.get(metric, 0)
        self.per_metric[metric] = i + 1
        item = self.script.get((metric, i), self.default)
        if isinstance(item, Exception):
            raise item
        return item


def test_metric_window_uses_metric_span_padded():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 10:00:00Z"]})
    assert metric_window(ev, "heart_rate", NOW, 2) == (T("2026-11-20 06:00"), T("2026-11-20 12:00"))


def test_metric_window_open_gap_ends_now_and_pad_never_exceeds_now():
    ev = event({"heart_rate": ["2026-11-24 08:00:00Z", None]}, end=None)
    assert metric_window(ev, "heart_rate", NOW, 2)[1] == NOW


def test_metric_window_falls_back_to_event_window():
    ev = event({"heart_rate": None})
    assert metric_window(ev, "heart_rate", NOW, 0) == (T("2026-11-20 06:00"), T("2026-11-20 18:00"))


def test_complete_with_data_ingests_each_nonempty_chunk():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 20:00:00Z"]})   # 08-20 +-2h = 16h -> 3 chunks
    client = FakeClient(default=points(3))
    got = []
    res = pull_event(client, ev, NOW, got.append, pad_h=2, chunk_hours=6)
    mp = res.metrics[0]
    assert len(client.calls) == 3 and len(got) == 3
    assert mp.complete and mp.points == 9 and not mp.answered_empty and res.complete and not res.interrupted


def test_complete_empty_answer_is_answered_empty_and_ingests_nothing():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 10:00:00Z"]})
    got = []
    res = pull_event(FakeClient(), ev, NOW, got.append)
    assert got == [] and res.metrics[0].answered_empty and res.complete


def test_connection_drop_is_interrupted_never_answered_empty():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 20:00:00Z"],
                "Blood Oxygen Saturation": ["2026-11-20 08:00:00Z", "2026-11-20 20:00:00Z"]})
    client = FakeClient(script={("heart_rate", 1): HAEUnreachable("gone")})
    got = []
    res = pull_event(client, ev, NOW, got.append, chunk_hours=6)
    assert res.interrupted and not res.complete
    assert len(res.metrics) == 1                      # second metric never started
    hr = res.metrics[0]
    assert hr.chunks_done == 1 and not hr.complete and not hr.answered_empty
    assert got == []                                   # chunk 0 was empty: nothing to ingest


def test_partial_progress_is_ingested_before_a_drop():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 20:00:00Z"]})
    client = FakeClient(default=points(2), script={("heart_rate", 2): HAEUnreachable("gone")})
    got = []
    res = pull_event(client, ev, NOW, got.append, chunk_hours=6)
    assert res.interrupted and len(got) == 2 and res.metrics[0].points == 4
    assert not res.metrics[0].complete


def test_protocol_error_marks_only_that_metric_and_continues():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 10:00:00Z"],
                "Blood Oxygen Saturation": ["2026-11-20 08:00:00Z", "2026-11-20 10:00:00Z"]})
    client = FakeClient(default=points(1), script={("heart_rate", 0): HAEProtocolError("junk")})
    res = pull_event(client, ev, NOW, lambda p: None)
    hr, spo2 = res.metrics
    assert hr.error and not hr.complete and not hr.answered_empty
    assert spo2.complete and spo2.points >= 1
    assert not res.interrupted and not res.complete


def test_auth_error_propagates():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 10:00:00Z"]})
    with pytest.raises(HAEAuthError):
        pull_event(FakeClient(script={("heart_rate", 0): HAEAuthError("401")}), ev, NOW, lambda p: None)


def test_summarised_metric_uses_few_large_chunks():
    ev = event({"Step Count": ["2026-11-20 00:00:00Z", "2026-11-22 00:00:00Z"]})   # 52h with pad
    client = FakeClient()
    pull_event(client, ev, NOW, lambda p: None)
    assert len(client.calls) == 2                       # 48h chunks, not 9 x 6h


def test_chunks_requested_newest_first():
    ev = event({"heart_rate": ["2026-11-20 08:00:00Z", "2026-11-20 20:00:00Z"]})
    client = FakeClient()
    pull_event(client, ev, NOW, lambda p: None, chunk_hours=6)
    starts = [c[1] for c in client.calls]
    assert starts == sorted(starts, reverse=True)
