"""
health_gaps/hae_client.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Minimal client for Health Auto Export's MCP server (HTTP, bearer token).

The server runs on the phone and is only reachable while HAE is foregrounded
and the phone is unlocked, so it can vanish mid-run. The error types separate
the cases the recovery logic must never confuse:

    HAEUnreachable   the phone did not answer (down, timed out, dropped). Says
                     nothing about whether the data exists.
    HAEAuthError     the token was rejected.
    HAEProtocolError the server answered but not in a way we understand.

A call that returns normally means the phone *answered*; whether the answer
contained data is for the caller to decide (see ``count_points``).

Free of Prefect and of the settings object: callers pass host/port/token.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

from health_gaps.config import HEART_RATE, SLEEP
from upload.health.constants import SNAKE_TO_DISPLAY

PROTOCOL_VERSION = "2025-03-26"
DEFAULT_TIMEOUT_S = 30.0
DEFAULT_CHUNK_HOURS = 6.0

# The REST automations summarise these (hourly) and send everything else raw. A recovery
# pull must ask for each metric the same way, or recovered rows won't match live ones.
SUMMARISED_METRICS = frozenset({
    "Active Energy", "Apple Stand Time", "Resting Energy", "Step Count", "Walking + Running Distance",
})
SUMMARY_INTERVAL = "hours"
SUMMARISED_CHUNK_HOURS = 48.0   # hourly points are few, so far larger windows are fine


@dataclass(frozen=True)
class QueryStyle:
    aggregate: bool
    interval: str
    chunk_hours: float


def query_style(gap_metric: str) -> QueryStyle:
    if gap_metric in SUMMARISED_METRICS:
        return QueryStyle(True, SUMMARY_INTERVAL, SUMMARISED_CHUNK_HOURS)
    return QueryStyle(False, "minutes", DEFAULT_CHUNK_HOURS)


_DISPLAY_TO_SNAKE ={display: snake for snake, display in SNAKE_TO_DISPLAY.items()}
_VIRTUAL_TO_SNAKE = {HEART_RATE: "heart_rate", SLEEP: "sleep_analysis"}


class HAEError(Exception):
    pass


class HAEUnreachable(HAEError):
    pass


class HAEAuthError(HAEError):
    pass


class HAEProtocolError(HAEError):
    pass


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def hae_metric_name(gap_metric: str) -> str:
    """HAE's snake_case name for a metric as named in health_gaps (display or virtual)."""
    if gap_metric in _VIRTUAL_TO_SNAKE:
        return _VIRTUAL_TO_SNAKE[gap_metric]
    try:
        return _DISPLAY_TO_SNAKE[gap_metric]
    except KeyError:
        raise KeyError(f"no Health Auto Export name known for metric {gap_metric!r}") from None


def format_hae_date(dt: datetime) -> str:
    """HAE requires exactly 'yyyy-MM-dd HH:mm:ss Z'. Always sent in UTC."""
    if dt.tzinfo is None:
        raise ValueError("naive datetime; HAE dates must carry a timezone")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S +0000")


def chunk_range(start: datetime, end: datetime, hours: float = DEFAULT_CHUNK_HOURS) -> list[tuple[datetime, datetime]]:
    """Split [start, end] into consecutive windows of at most ``hours``, newest first.

    Newest first so that if the phone drops partway the most recent data, which is
    the least likely to have been recovered some other way, is already in.
    """
    if end <= start:
        return []
    step = timedelta(hours=hours)
    chunks: list[tuple[datetime, datetime]] = []
    cursor = end
    while cursor > start:
        lo = max(start, cursor - step)
        chunks.append((lo, cursor))
        cursor = lo
    return chunks


def count_points(payload: dict) -> int:
    """Number of data points in a get_health_metrics payload ({'metrics': [...]})."""
    return sum(len(m.get("data") or []) for m in payload.get("metrics") or [])


# ---------------------------------------------------------------------------
# client
# ---------------------------------------------------------------------------

@dataclass
class HAEClient:
    host: str
    port: int
    token: str = field(repr=False)   # keep out of logs and tracebacks
    timeout_s: float = DEFAULT_TIMEOUT_S
    session: Any = field(default_factory=requests.Session, repr=False)
    _session_id: Optional[str] = field(default=None, init=False, repr=False)
    _next_id: int = field(default=0, init=False, repr=False)

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/mcp"

    # -- lifecycle ---------------------------------------------------------

    def __enter__(self) -> "HAEClient":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        close = getattr(self.session, "close", None)
        if close:
            close()
        self._session_id = None

    def connect(self) -> None:
        """MCP handshake. Raises HAEUnreachable / HAEAuthError / HAEProtocolError."""
        self._session_id = None
        result = self._rpc("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "travelnet", "version": "1"},
        })
        if "serverInfo" not in result:
            raise HAEProtocolError("initialize response has no serverInfo")
        self._rpc("notifications/initialized", None, notification=True)

    # -- API ---------------------------------------------------------------

    def get_health_metrics(
        self,
        gap_metric: str,
        start: datetime,
        end: datetime,
        *,
        aggregate: Optional[bool] = None,
        interval: Optional[str] = None,
    ) -> dict:
        """Query one metric over [start, end]. Returns the payload's ``data`` object,
        i.e. ``{"metrics": [{"name", "units", "data": [...]}]}`` (the shape
        ``handle_health_upload`` takes). The phone having no data is a normal return
        with zero points (``{"metrics": []}``), not an error.

        aggregate/interval default to the metric's query_style() so the result matches
        what the REST automations would have sent."""
        style = query_style(gap_metric)
        text = self._call_tool("get_health_metrics", {
            "start": format_hae_date(start),
            "end": format_hae_date(end),
            "metrics": hae_metric_name(gap_metric),
            "aggregate": style.aggregate if aggregate is None else aggregate,
            "interval": style.interval if interval is None else interval,
        })
        try:
            body = json.loads(text)
        except ValueError as e:
            raise HAEProtocolError(f"tool result is not JSON: {text[:120]!r}") from e
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict):
            raise HAEProtocolError(f"tool result has no 'data' object: {text[:120]!r}")
        return data

    # -- plumbing ----------------------------------------------------------

    def _call_tool(self, name: str, arguments: dict) -> str:
        result = self._rpc("tools/call", {"name": name, "arguments": arguments})
        content = result.get("content")
        if not content or not isinstance(content, list) or "text" not in content[0]:
            raise HAEProtocolError(f"tool {name} returned no text content")
        if result.get("isError"):
            raise HAEProtocolError(f"tool {name} reported an error: {content[0]['text'][:200]}")
        return content[0]["text"]

    def _rpc(self, method: str, params: Optional[dict], *, notification: bool = False) -> dict:
        msg: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notification:
            self._next_id += 1
            msg["id"] = self._next_id

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id

        try:
            resp = self.session.post(self.url, headers=headers, json=msg, timeout=self.timeout_s)
        except requests.RequestException as e:   # connection refused, timeout, reset mid-body
            raise HAEUnreachable(f"{method}: {type(e).__name__}") from e

        if resp.status_code in (401, 403):
            raise HAEAuthError(f"{method}: HTTP {resp.status_code}")
        if resp.status_code >= 500:
            raise HAEUnreachable(f"{method}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            raise HAEProtocolError(f"{method}: HTTP {resp.status_code}")

        sid = resp.headers.get("Mcp-Session-Id")
        if sid:
            self._session_id = sid
        if notification:
            return {}

        try:
            body = resp.json()
        except ValueError as e:
            raise HAEProtocolError(f"{method}: response is not JSON") from e
        if not isinstance(body, dict):
            raise HAEProtocolError(f"{method}: unexpected response shape")
        if "error" in body:
            raise HAEProtocolError(f"{method}: {str(body['error'])[:200]}")
        result = body.get("result")
        if not isinstance(result, dict):
            raise HAEProtocolError(f"{method}: no result in response")
        return result
