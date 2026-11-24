"""Tests for health_gaps/hae_client.py (HAE MCP client). No network: a fake session stands in."""

import json
from datetime import datetime, timedelta, timezone

import pytest
import requests

from health_gaps.hae_client import (
    HAEAuthError, HAEClient, HAEProtocolError, HAEUnreachable,
    chunk_range, count_points, format_hae_date, hae_metric_name,
)

UTC = timezone.utc


def T(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


class FakeResp:
    def __init__(self, status=200, body=None, headers=None, bad_json=False):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._body


class FakeSession:
    """Replays queued responses (or raises queued exceptions); records requests."""
    def __init__(self, *responses):
        self.queue = list(responses)
        self.calls: list[dict] = []
        self.closed = False

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


INIT = FakeResp(200, {"jsonrpc": "2.0", "id": 1, "result": {"serverInfo": {"name": "Health Auto Export"}}},
                headers={"Mcp-Session-Id": "SESS-1"})
INITIALIZED = FakeResp(202)


def tool_result(payload) -> FakeResp:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    return FakeResp(200, {"jsonrpc": "2.0", "id": 2, "result": {"content": [{"type": "text", "text": text}]}})


def client(*responses) -> tuple[HAEClient, FakeSession]:
    s = FakeSession(*responses)
    return HAEClient("100.1.2.3", 9000, "tok", session=s), s


GOOD = {"data": {"metrics": [{"name": "heart_rate", "units": "count/min",
                              "data": [{"date": "2026-11-24 09:49:21 +1100", "Avg": 55, "Min": 55, "Max": 55}]}]}}


# --- pure helpers ----------------------------------------------------------

def test_format_hae_date_is_utc_and_exact():
    aest = datetime(2026, 11, 24, 10, 0, tzinfo=timezone(timedelta(hours=11)))
    assert format_hae_date(aest) == "2026-11-23 23:00:00 +0000"


def test_format_hae_date_rejects_naive():
    with pytest.raises(ValueError):
        format_hae_date(datetime(2026, 1, 1))


def test_chunk_range_newest_first_and_contiguous():
    chunks = chunk_range(T("2026-11-20 00:00"), T("2026-11-20 14:00"), hours=6)
    assert chunks == [
        (T("2026-11-20 08:00"), T("2026-11-20 14:00")),
        (T("2026-11-20 02:00"), T("2026-11-20 08:00")),
        (T("2026-11-20 00:00"), T("2026-11-20 02:00")),
    ]


def test_chunk_range_short_and_empty():
    assert chunk_range(T("2026-11-20 00:00"), T("2026-11-20 01:00"), 6) == [(T("2026-11-20 00:00"), T("2026-11-20 01:00"))]
    assert chunk_range(T("2026-11-20 01:00"), T("2026-11-20 01:00"), 6) == []


def test_metric_names():
    assert hae_metric_name("heart_rate") == "heart_rate"          # virtual
    assert hae_metric_name("sleep") == "sleep_analysis"           # virtual
    assert hae_metric_name("Step Count") == "step_count"          # display -> snake
    assert hae_metric_name("Blood Oxygen Saturation") == "blood_oxygen_saturation"
    with pytest.raises(KeyError):
        hae_metric_name("Not A Metric")


def test_every_monitored_metric_has_an_hae_name():
    from health_gaps.config import METRIC_CONFIG
    for m in METRIC_CONFIG:
        hae_metric_name(m)


def test_count_points():
    assert count_points({"metrics": []}) == 0
    assert count_points({"metrics": [{"name": "x", "data": []}]}) == 0
    assert count_points({"metrics": [{"data": [1, 2]}, {"data": [3]}]}) == 3
    assert count_points({}) == 0


# --- handshake -------------------------------------------------------------

def test_connect_sends_auth_and_reuses_session_id():
    c, s = client(INIT, INITIALIZED, tool_result(GOOD))
    c.connect()
    c.get_health_metrics("heart_rate", T("2026-11-24 00:00"), T("2026-11-24 03:00"))
    assert s.calls[0]["url"] == "http://100.1.2.3:9000/mcp"
    assert all(call["headers"]["Authorization"] == "Bearer tok" for call in s.calls)
    assert "Mcp-Session-Id" not in s.calls[0]["headers"]
    assert s.calls[1]["headers"]["Mcp-Session-Id"] == "SESS-1"
    assert s.calls[2]["headers"]["Mcp-Session-Id"] == "SESS-1"
    assert s.calls[1]["json"]["method"] == "notifications/initialized" and "id" not in s.calls[1]["json"]


def test_connect_requires_server_info():
    c, _ = client(FakeResp(200, {"jsonrpc": "2.0", "id": 1, "result": {}}))
    with pytest.raises(HAEProtocolError):
        c.connect()


def test_context_manager_connects_and_closes():
    s = FakeSession(INIT, INITIALIZED)
    with HAEClient("h", 1, "t", session=s) as c:
        assert c._session_id == "SESS-1"
    assert s.closed and c._session_id is None


# --- get_health_metrics ----------------------------------------------------

def test_get_health_metrics_request_and_payload():
    c, s = client(INIT, INITIALIZED, tool_result(GOOD))
    c.connect()
    data = c.get_health_metrics("heart_rate", T("2026-11-24 00:00"), T("2026-11-24 03:00"))
    assert data == GOOD["data"] and count_points(data) == 1
    call = s.calls[2]["json"]
    assert call["method"] == "tools/call" and call["params"]["name"] == "get_health_metrics"
    assert call["params"]["arguments"] == {
        "start": "2026-11-24 00:00:00 +0000", "end": "2026-11-24 03:00:00 +0000",
        "metrics": "heart_rate", "aggregate": False, "interval": "minutes",
    }


def test_empty_answer_is_a_normal_return():
    empty = {"data": {"metrics": [{"name": "step_count", "units": "count", "data": []}]}}
    c, _ = client(INIT, INITIALIZED, tool_result(empty))
    c.connect()
    assert count_points(c.get_health_metrics("Step Count", T("2026-11-24 00:00"), T("2026-11-24 03:00"))) == 0


@pytest.mark.parametrize("bad", ["not json at all", json.dumps({"nope": 1}), json.dumps({"data": []})])
def test_malformed_tool_text_is_protocol_error(bad):
    c, _ = client(INIT, INITIALIZED, tool_result(bad))
    c.connect()
    with pytest.raises(HAEProtocolError):
        c.get_health_metrics("heart_rate", T("2026-11-24 00:00"), T("2026-11-24 03:00"))


def test_tool_is_error_flag_is_protocol_error():
    resp = FakeResp(200, {"jsonrpc": "2.0", "id": 2,
                          "result": {"isError": True, "content": [{"type": "text", "text": "bad dates"}]}})
    c, _ = client(INIT, INITIALIZED, resp)
    c.connect()
    with pytest.raises(HAEProtocolError):
        c.get_health_metrics("heart_rate", T("2026-11-24 00:00"), T("2026-11-24 03:00"))


# --- failure classification -------------------------------------------------

@pytest.mark.parametrize("exc", [requests.ConnectionError("refused"), requests.Timeout("slow"),
                                 requests.exceptions.ChunkedEncodingError("dropped")])
def test_network_failures_are_unreachable_not_empty(exc):
    c, _ = client(exc)
    with pytest.raises(HAEUnreachable):
        c.connect()


def test_drop_mid_run_is_unreachable():
    c, _ = client(INIT, INITIALIZED, requests.ConnectionError("gone"))
    c.connect()
    with pytest.raises(HAEUnreachable):
        c.get_health_metrics("heart_rate", T("2026-11-24 00:00"), T("2026-11-24 03:00"))


def test_401_is_auth_error():
    c, _ = client(FakeResp(401, {"error": "unauthorized"}))
    with pytest.raises(HAEAuthError):
        c.connect()


def test_5xx_is_unreachable_other_4xx_is_protocol():
    c, _ = client(FakeResp(503))
    with pytest.raises(HAEUnreachable):
        c.connect()
    c, _ = client(FakeResp(400))
    with pytest.raises(HAEProtocolError):
        c.connect()


def test_jsonrpc_error_and_non_json_body():
    c, _ = client(FakeResp(200, {"jsonrpc": "2.0", "id": 1, "error": {"code": -32600, "message": "bad"}}))
    with pytest.raises(HAEProtocolError):
        c.connect()
    c, _ = client(FakeResp(200, bad_json=True))
    with pytest.raises(HAEProtocolError):
        c.connect()


def test_token_never_appears_in_exception_text():
    c, _ = client(FakeResp(401))
    with pytest.raises(HAEAuthError) as ei:
        c.connect()
    assert "tok" not in str(ei.value)
    assert "tok" not in repr(c)


# --- query style ------------------------------------------------------------

def test_query_style_matches_rest_automations():
    from health_gaps.hae_client import query_style
    for m in ("Active Energy", "Apple Stand Time", "Resting Energy", "Step Count", "Walking + Running Distance"):
        s = query_style(m)
        assert s.aggregate is True and s.interval == "hours" and s.chunk_hours >= 24
    for m in ("heart_rate", "sleep", "Heart Rate Variability", "Apple Stand Hour", "Physical Effort"):
        s = query_style(m)
        assert s.aggregate is False and s.chunk_hours <= 6


def test_summarised_metric_is_requested_hourly_by_default():
    c, s = client(INIT, INITIALIZED, tool_result({"data": {"metrics": []}}))
    c.connect()
    c.get_health_metrics("Step Count", T("2026-11-24 00:00"), T("2026-11-24 03:00"))
    args = s.calls[2]["json"]["params"]["arguments"]
    assert args["aggregate"] is True and args["interval"] == "hours" and args["metrics"] == "step_count"


def test_chunk_range_align_to_start_keeps_the_grid_and_newest_first():
    start, end = T("2026-11-20 00:13"), T("2026-11-23 06:00")
    chunks = chunk_range(start, end, hours=48, align_to_start=True)
    assert chunks == [
        (T("2026-11-22 00:13"), T("2026-11-23 06:00")),
        (T("2026-11-20 00:13"), T("2026-11-22 00:13")),
    ]
    assert chunk_range(start, start, 48, align_to_start=True) == []


def test_resting_energy_uses_the_spelling_the_mcp_server_accepts():
    """HAE rejects "resting_energy" with 'No valid metrics selected'; it calls the metric basal_energy_burned."""
    assert hae_metric_name("Resting Energy") == "basal_energy_burned"


def test_resting_energy_request_sends_the_hae_spelling():
    c, s = client(INIT, INITIALIZED, tool_result({"data": {"metrics": []}}))
    c.connect()
    c.get_health_metrics("Resting Energy", T("2026-11-24 00:00"), T("2026-11-24 03:00"))
    args = s.calls[2]["json"]["params"]["arguments"]
    assert args["metrics"] == "basal_energy_burned" and args["aggregate"] is True      # still summarised hourly
