"""
health_gaps/pull.py
~~~~~~~~~~~~~~~~~~~
Pull the data for a confirmed gap event from the phone's HAE server and hand it
to the normal health ingest.

The one rule that matters: **only a complete answer from the phone may count as
"the data is not there".** A dropped connection, an abort or an unprocessed
chunk leaves a metric ``complete=False`` and the caller must not mark anything
unrecoverable on that basis.

Free of Prefect and the DB: the client and the ingest callable are injected.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

from health_gaps.detect import parse
from health_gaps.events import Event
from health_gaps.hae_client import (
    HAEAuthError, HAEClient, HAEProtocolError, HAEUnreachable,
    chunk_range, count_points, query_style,
)

logger = logging.getLogger(__name__)


@dataclass
class MetricPull:
    metric: str
    window: tuple[datetime, datetime]
    chunks_total: int
    chunks_done: int = 0
    points: int = 0
    error: Optional[str] = None      # protocol error on this metric (not a connection loss)

    @property
    def complete(self) -> bool:
        """The phone answered every chunk for this metric."""
        return self.error is None and self.chunks_done == self.chunks_total

    @property
    def answered_empty(self) -> bool:
        """Complete answer, zero points: the phone genuinely has nothing for this window."""
        return self.complete and self.points == 0


@dataclass
class EventPull:
    event_id: int
    metrics: list[MetricPull] = field(default_factory=list)
    interrupted: bool = False        # the phone went away part-way through

    @property
    def points(self) -> int:
        return sum(m.points for m in self.metrics)

    @property
    def complete(self) -> bool:
        return not self.interrupted and all(m.complete for m in self.metrics)


def metric_window(event: Event, metric: str, now: datetime, pad_h: float) -> tuple[datetime, datetime]:
    """The window to ask the phone for.

    Raw metrics: the metric's own gap window, padded generously. Rows that already exist are
    ignored on insert, so padding only costs bandwidth and protects the gap's edges.

    Summarised metrics (hourly buckets anchored to the query start): padding would produce
    buckets that straddle rows already stored, double-counting the overlap. Instead the query
    starts one hour after the last stored row (so the new buckets continue its hourly grid)
    and ends at the first stored row after the gap, with no padding.
    """
    span = event.metrics.get(metric)
    if span:
        start = parse(span[0])
        end = parse(span[1]) if span[1] else now
    else:
        start, end = event.window_start, event.window_end or now
    if query_style(metric).aggregate:
        return start + timedelta(hours=1), min(end, now)
    pad = timedelta(hours=pad_h)
    return start - pad, min(end + pad, now)


def _metric_chunks(event: Event, metric: str, now: datetime, pad_h: float, chunk_hours: Optional[float]):
    lo, hi = metric_window(event, metric, now, pad_h)
    style = query_style(metric)
    return (lo, hi), chunk_range(lo, hi, chunk_hours or style.chunk_hours, align_to_start=style.aggregate)


def estimate_requests(event: Event, now: datetime, pad_h: float, chunk_hours: Optional[float] = None) -> int:
    """How many HAE requests pulling ``event`` takes (one per metric per chunk)."""
    return sum(len(_metric_chunks(event, m, now, pad_h, chunk_hours)[1]) for m in event.metrics)


def pull_event(
    client: HAEClient,
    event: Event,
    now: datetime,
    ingest: Callable[[dict], None],
    *,
    pad_h: float = 2.0,
    chunk_hours: Optional[float] = None,
) -> EventPull:
    """Pull every metric of ``event`` and ingest what comes back.

    ``ingest`` receives the payload's ``data`` object (``{"metrics": [...]}``) once per
    non-empty chunk. Raises HAEAuthError (the token is wrong; nothing will work); a lost
    connection stops the pull and returns it ``interrupted``.
    """
    result = EventPull(event.id)
    for metric in event.metrics:
        (lo, hi), chunks = _metric_chunks(event, metric, now, pad_h, chunk_hours)
        mp = MetricPull(metric, (lo, hi), len(chunks))
        result.metrics.append(mp)

        for c_lo, c_hi in chunks:
            try:
                payload = client.get_health_metrics(metric, c_lo, c_hi)
            except HAEUnreachable as e:
                logger.warning("Phone went away during %s (event %s): %s", metric, event.id, e)
                result.interrupted = True
                return result
            except HAEAuthError:
                raise
            except HAEProtocolError as e:
                logger.warning("Unusable answer for %s (event %s): %s", metric, event.id, e)
                mp.error = str(e)
                break
            n = count_points(payload)
            if n:
                ingest(payload)
            mp.chunks_done += 1
            mp.points += n
        logger.info("Event %s %s: %d points over %d/%d chunks%s", event.id, metric, mp.points,
                    mp.chunks_done, mp.chunks_total, "" if mp.complete else " (INCOMPLETE)")
    return result
