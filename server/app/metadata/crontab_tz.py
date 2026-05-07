"""
crontab_tz.py
~~~~~~~~~~~~~
Convert a crontab so jobs fire at the same wall-clock time in a different timezone.

The Pi runs on Europe/London (GMT in winter, BST in summer). When travelling,
cron jobs fire at inconvenient local times. This module converts the timing so
a job that was "6am Pi time" becomes "6am your time" — i.e., takes the existing
hour/minute values, interprets them as being in the target timezone, and converts
to whatever Pi local time that corresponds to.

Core API
--------
- resolve_timezone(tz_str)          → (label, utc_offset_minutes)
- convert_crontab(text, user_off)   → (new_text, list_of_changes)
- update_crontab_timezone(tz_str)   → summary dict (read → convert → write)

Timezone input formats accepted
--------------------------------
- IANA names:         "America/New_York", "Europe/London", "Asia/Kolkata"
- UTC offset strings: "+1000", "+05:30", "-0500", "+10"
- Common abbreviations: "EST", "JST", "AEST" — see ABBREV_TO_IANA
"""

import logging
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

# When set, read_crontab/write_crontab use this file directly instead of the
# `crontab` subprocess. Used inside Docker where the host crontab is
# volume-mounted (e.g. /var/spool/cron/crontabs/dan) and the container runs as
# root so the `crontab -u dan` command would fail (user not in container passwd).
CRONTAB_FILE: str | None = os.environ.get("CRONTAB_FILE")

# Backup written before each conversion; restored by reset_crontab_timezone().
# Only available when CRONTAB_FILE is set (Docker); subprocess mode has no persistent path.
def _backup_path() -> Path | None:
    return Path(CRONTAB_FILE).with_suffix('.bak') if CRONTAB_FILE else None

# Stores the UTC offset (minutes) of the last applied conversion as a plain integer.
# Used to skip re-conversion when the timezone hasn't changed (e.g. fortnightly shortcut
# posting the same timezone for months).
def _last_offset_path() -> Path | None:
    return Path(CRONTAB_FILE).with_suffix('.last_tz') if CRONTAB_FILE else None


def _read_last_offset() -> int | None:
    path = _last_offset_path()
    if path and path.exists():
        try:
            return int(path.read_text().strip())
        except ValueError:
            logger.debug("Malformed .last_tz file at %s — ignoring cached offset", path)
    return None


def _write_last_offset(offset_min: int) -> None:
    path = _last_offset_path()
    if path:
        path.write_text(str(offset_min))


# ---------------------------------------------------------------------------
# Abbreviation → IANA lookup
# Abbreviations are regionally ambiguous; entries here use the most common
# global meaning. Prefer IANA names for unambiguous input.
# ---------------------------------------------------------------------------
ABBREV_TO_IANA: dict[str, str] = {
    "GMT":  "Etc/GMT",
    "UTC":  "Etc/UTC",
    "BST":  "Europe/London",        # British Summer Time  UTC+1
    "WET":  "Europe/Lisbon",        # Western European Time UTC+0/+1
    "CET":  "Europe/Paris",         # Central European Time UTC+1
    "CEST": "Europe/Paris",         # Central European Summer UTC+2
    "EET":  "Europe/Helsinki",      # Eastern European UTC+2
    "EEST": "Europe/Helsinki",      # Eastern European Summer UTC+3
    "MSK":  "Europe/Moscow",        # Moscow UTC+3
    "EST":  "America/New_York",     # Eastern Standard UTC-5
    "EDT":  "America/New_York",     # Eastern Daylight UTC-4
    "CST":  "America/Chicago",      # Central Standard UTC-6
    "CDT":  "America/Chicago",      # Central Daylight UTC-5
    "MST":  "America/Denver",       # Mountain Standard UTC-7
    "MDT":  "America/Denver",       # Mountain Daylight UTC-6
    "PST":  "America/Los_Angeles",  # Pacific Standard UTC-8
    "PDT":  "America/Los_Angeles",  # Pacific Daylight UTC-7
    "AKST": "America/Anchorage",    # Alaska Standard UTC-9
    "AKDT": "America/Anchorage",    # Alaska Daylight UTC-8
    "HST":  "Pacific/Honolulu",     # Hawaii UTC-10
    "IST":  "Asia/Kolkata",         # India Standard UTC+5:30
    "PKT":  "Asia/Karachi",         # Pakistan UTC+5
    "ICT":  "Asia/Bangkok",         # Indochina UTC+7
    "SGT":  "Asia/Singapore",       # Singapore UTC+8
    "HKT":  "Asia/Hong_Kong",       # Hong Kong UTC+8
    "JST":  "Asia/Tokyo",           # Japan UTC+9
    "KST":  "Asia/Seoul",           # Korea UTC+9
    "AEST": "Australia/Sydney",     # Australian Eastern Standard UTC+10
    "AEDT": "Australia/Sydney",     # Australian Eastern Daylight UTC+11
    "ACST": "Australia/Darwin",     # Australian Central Standard UTC+9:30
    "AWST": "Australia/Perth",      # Australian Western UTC+8
    "NZST": "Pacific/Auckland",     # New Zealand Standard UTC+12
    "NZDT": "Pacific/Auckland",     # New Zealand Daylight UTC+13
}

PI_TIMEZONE = "Europe/London"


# ---------------------------------------------------------------------------
# Timezone resolution
# ---------------------------------------------------------------------------

def resolve_timezone(tz_str: str) -> tuple[str, int]:
    """Resolve a timezone string to a (human-readable label, UTC offset in minutes) tuple.

    Accepts IANA names, UTC offset strings, and common abbreviations.
    Raises ValueError if the string cannot be resolved.
    """
    # 1. UTC offset string: (+/-)HH, (+/-)HHMM, (+/-)HH:MM
    m = re.fullmatch(r'([+-])(\d{1,2})(?::?(\d{2}))?', tz_str)
    if m:
        sign = 1 if m.group(1) == '+' else -1
        offset_min = sign * (int(m.group(2)) * 60 + int(m.group(3) or 0))
        return tz_str, offset_min

    # 2. Common abbreviation → resolve to IANA name
    iana_name = ABBREV_TO_IANA.get(tz_str.upper(), tz_str)

    # 3. IANA name (also used for resolved abbreviations)
    try:
        zi = ZoneInfo(iana_name)
        now = datetime.now(zi)
        offset_min = int(now.utcoffset().total_seconds() / 60)
        h, mm = divmod(abs(offset_min), 60)
        sign_char = '+' if offset_min >= 0 else '-'
        label = f"{iana_name} (UTC{sign_char}{h:02d}:{mm:02d})"
        return label, offset_min
    except Exception:
        pass

    raise ValueError(
        f"Cannot resolve timezone {tz_str!r}. "
        "Use an IANA name (e.g. 'America/New_York'), "
        "a UTC offset (e.g. '+1000', '-05:30'), "
        "or a common abbreviation (e.g. 'EST', 'JST')."
    )


def get_pi_utc_offset_minutes() -> int:
    """Return the Pi's current UTC offset in minutes (accounts for BST/GMT automatically)."""
    zi = ZoneInfo(PI_TIMEZONE)
    return int(datetime.now(zi).utcoffset().total_seconds() / 60)


# ---------------------------------------------------------------------------
# Cron line parsing and conversion
# ---------------------------------------------------------------------------

def _convert_time_fields(
    minute_str: str,
    hour_str: str,
    dom_str: str,
    dow_str: str,
    offset_minutes: int,
) -> tuple[str, str, str, str, int, str | None]:
    """Apply an offset (in minutes) to a cron time spec.

    offset_minutes = pi_utc_offset - user_utc_offset.
    Returns (new_minute, new_hour, new_dom, new_dow, day_delta, warning).
    day_delta is -1, 0, or +1 for midnight crossings.
    """
    # Wildcard hour means "every hour" — nothing to convert
    if hour_str == '*':
        return minute_str, hour_str, dom_str, dow_str, 0, None

    # Only handle plain integers; leave complex expressions (ranges, steps, lists) alone
    for name, val in [("hour", hour_str), ("minute", minute_str)]:
        if val != '*' and not re.fullmatch(r'\d+', val):
            return (
                minute_str, hour_str, dom_str, dow_str, 0,
                f"Complex {name} field '{val}' — skipped, adjust manually",
            )

    hour = int(hour_str)
    is_minute_wildcard = minute_str == '*'
    minute = 0 if is_minute_wildcard else int(minute_str)

    total = hour * 60 + minute + offset_minutes
    day_delta = 0
    while total < 0:
        total += 1440
        day_delta -= 1
    while total >= 1440:
        total -= 1440
        day_delta += 1

    new_hour = total // 60
    new_minute = total % 60

    # Adjust day-of-week for midnight crossings
    new_dow = dow_str
    if day_delta != 0 and dow_str not in ('*', '*/1'):
        try:
            days = [int(d) for d in dow_str.split(',')]
            new_dow = ','.join(str((d + day_delta) % 7) for d in days)
        except ValueError:
            pass  # Complex DOW — leave unchanged

    warning = None
    if day_delta != 0 and dom_str != '*':
        warning = f"Day-of-month '{dom_str}' may be off by {day_delta:+d} — check manually"

    new_minute_str = '*' if is_minute_wildcard else str(new_minute)
    return new_minute_str, str(new_hour), dom_str, new_dow, day_delta, warning


def convert_crontab(
    crontab_text: str,
    user_utc_offset_minutes: int,
    pi_utc_offset_minutes: int | None = None,
) -> tuple[str, list[dict]]:
    """Convert all cron job times from user's timezone to Pi's timezone.

    :param crontab_text: raw output of ``crontab -l``.
    :param user_utc_offset_minutes: target timezone UTC offset in minutes.
    :param pi_utc_offset_minutes: Pi's current offset (auto-detected if None).
    :returns: (new_crontab_text, list of change records).
    """
    if pi_utc_offset_minutes is None:
        pi_utc_offset_minutes = get_pi_utc_offset_minutes()

    offset = pi_utc_offset_minutes - user_utc_offset_minutes
    new_lines = []
    changes = []

    for line in crontab_text.splitlines():
        stripped = line.strip()

        # Pass blanks, comments, variable assignments, and @specials through unchanged
        if (not stripped
                or stripped.startswith('#')
                or stripped.startswith('@')
                or re.match(r'^\w+=', stripped)):
            new_lines.append(line)
            continue

        parts = stripped.split(None, 5)
        if len(parts) < 6:
            new_lines.append(line)
            continue

        minute, hour, dom, month, dow, _command = parts
        new_min, new_hour, new_dom, new_dow, day_delta, warning = _convert_time_fields(
            minute, hour, dom, dow, offset
        )

        # Rebuild line preserving the original structure
        new_parts = parts.copy()
        new_parts[0] = new_min
        new_parts[1] = new_hour
        new_parts[4] = new_dow
        new_line = ' '.join(new_parts)

        if new_line != stripped or warning:
            changes.append({
                'original':  stripped,
                'converted': new_line,
                'changed':   new_line != stripped,
                'day_delta': day_delta,
                'warning':   warning,
            })

        new_lines.append(new_line)

    return '\n'.join(new_lines) + '\n', changes


# ---------------------------------------------------------------------------
# crontab I/O
# ---------------------------------------------------------------------------

def read_crontab() -> str:
    """Return the current user's crontab as a string (empty string if none set).

    If the CRONTAB_FILE env var is set (Docker usage), reads that file directly.
    Otherwise falls back to ``crontab -l`` (host/CLI usage).
    """
    if CRONTAB_FILE:
        path = Path(CRONTAB_FILE)
        return path.read_text() if path.exists() else ""
    result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
    if result.returncode != 0 and 'no crontab' not in result.stderr.lower():
        raise RuntimeError(f"crontab -l failed: {result.stderr.strip()}")
    return result.stdout


def write_crontab(crontab_text: str) -> None:
    """Replace the current user's crontab with the supplied text.

    If the CRONTAB_FILE env var is set (Docker usage), writes that file directly.
    Otherwise falls back to ``crontab -`` (host/CLI usage).
    """
    if CRONTAB_FILE:
        path = Path(CRONTAB_FILE)
        path.write_text(crontab_text)
        return
    result = subprocess.run(['crontab', '-'], input=crontab_text, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"crontab - failed: {result.stderr.strip()}")


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def update_crontab_timezone(tz_str: str) -> dict:
    """Resolve timezone, convert crontab, and apply the result.

    :param tz_str: any accepted timezone string (IANA, offset, abbreviation).
    :returns: summary dict with timezone_label, pi_offset_min, user_offset_min, changes.
    :raises ValueError: unrecognised timezone.
    :raises RuntimeError: crontab read/write error.
    """
    tz_label, user_offset_min = resolve_timezone(tz_str)
    pi_offset_min = get_pi_utc_offset_minutes()

    if _read_last_offset() == user_offset_min:
        return {
            'timezone_label':  tz_label,
            'pi_offset_min':   pi_offset_min,
            'user_offset_min': user_offset_min,
            'changes':         [],
            'skipped':         True,
        }

    original = read_crontab()
    backup = _backup_path()
    if backup is not None:
        backup.write_text(original)

    new_crontab, changes = convert_crontab(original, user_offset_min, pi_offset_min)
    write_crontab(new_crontab)
    _write_last_offset(user_offset_min)

    return {
        'timezone_label':  tz_label,
        'pi_offset_min':   pi_offset_min,
        'user_offset_min': user_offset_min,
        'changes':         changes,
        'skipped':         False,
    }


def reset_crontab_timezone() -> str:
    """Restore the crontab from the backup saved before the last conversion.

    :returns: the restored crontab text.
    :raises RuntimeError: no backup path available (subprocess mode) or backup not found.
    """
    backup = _backup_path()
    if backup is None:
        raise RuntimeError(
            "Reset is only available when CRONTAB_FILE is set (Docker mode). "
            "In subprocess mode there is no persistent backup path."
        )
    if not backup.exists():
        raise RuntimeError("No backup found — crontab has not been converted yet.")
    content = backup.read_text()
    write_crontab(content)
    return content
