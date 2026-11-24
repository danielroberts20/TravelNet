"""
health_gaps/report.py
~~~~~~~~~~~~~~~~~~~~~
Plain-text email/Pushcut formatting for health gap events.
"""

from __future__ import annotations

from datetime import datetime

from health_gaps.events import Event, Report

SUBJECT_PREFIX = "[TravelNet] Health gaps"


def should_email(report: Report, is_digest_day: bool) -> bool:
    """Email only when there is something new. Digest-only gaps wait for digest day."""
    if report.newly_confirmed:
        return True
    return is_digest_day and bool(report.held_for_digest)


def subject(report: Report, is_digest_day: bool) -> str:
    n_new = len(report.newly_confirmed) + (len(report.held_for_digest) if is_digest_day else 0)
    return (
        f"{SUBJECT_PREFIX} — {n_new} new, "
        f"{len(report.awaiting_export)} awaiting export, {len(report.ongoing)} ongoing"
    )


def _span(ev: Event) -> str:
    start = ev.window_start.strftime("%Y-%m-%d %H:%M")
    if ev.window_end is None:
        return f"{start} → ongoing UTC"
    return f"{start} → {ev.window_end.strftime('%Y-%m-%d %H:%M')} UTC ({ev.hours:.1f}h)"


def _fmt_event(ev: Event, now: datetime, pad_h: float, with_export: bool) -> str:
    kind = "outage" if ev.critical and len(ev.metrics) >= 3 else "gap"
    lines = [
        f"  #{ev.id}  {kind}{'  [CRITICAL]' if ev.critical else ''}",
        f"    when    : {_span(ev)}",
        f"    metrics : {', '.join(sorted(ev.metrics))}",
    ]
    if with_export:
        a, b = ev.export_range(now, pad_h)
        lines.append(f"    export  : HAE → {a:%Y-%m-%d %H:%M} to {b:%Y-%m-%d %H:%M} UTC")
    return "\n".join(lines)


def build_body(report: Report, now: datetime, is_digest_day: bool, pad_h: float) -> str:
    new = list(report.newly_confirmed) + (list(report.held_for_digest) if is_digest_day else [])
    new_ids = {e.id for e in new}
    lines = [f"Health gap check — {now:%Y-%m-%d}", ""]

    lines += ["NEW CONFIRMED GAPS  (data missing — re-export from Health Auto Export)", "-" * 50]
    lines += [_fmt_event(e, now, pad_h, True) + "\n" for e in new]

    others = [e for e in report.awaiting_export if e.id not in new_ids]
    if others:
        lines += ["STILL AWAITING MANUAL EXPORT", "-" * 50]
        lines += [_fmt_event(e, now, pad_h, True) + "\n" for e in others]

    if report.ongoing:
        lines += ["ONGOING / UNCONFIRMED  (no action yet)", "-" * 50]
        lines += [_fmt_event(e, now, pad_h, False) + "\n" for e in report.ongoing]

    lines += [
        "After exporting, check the gap off so the next daily run can verify it:",
        "  POST /metadata/health-gaps/<id>/exported   (still empty afterwards → marked genuinely missing)",
        "  POST /metadata/health-gaps/<id>/dismiss    (mark genuinely missing without exporting)",
    ]
    return "\n".join(lines)


def push_text(ev: Event) -> str:
    if len(ev.metrics) >= 3:
        return f"Health ingest outage #{ev.id}: {len(ev.metrics)} metrics, {_span(ev)}"
    return f"Health gap #{ev.id}: {', '.join(sorted(ev.metrics))}, {_span(ev)}"
