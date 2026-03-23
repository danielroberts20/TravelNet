#!/usr/bin/env python3
"""
convert_crontab_tz.py — adjust crontab schedules for a different timezone.

Re-times all cron jobs so they run at the same wall-clock time in your current
timezone. The Pi runs on Europe/London (GMT/BST); if you're in EST and a job
is scheduled for 06:00, this converts it so it fires at 06:00 EST instead.

Usage:
    python convert_crontab_tz.py <timezone>

Examples:
    python convert_crontab_tz.py America/New_York
    python convert_crontab_tz.py EST
    python convert_crontab_tz.py +1000
    python convert_crontab_tz.py -05:30

Run from the repo root. Prints a diff and prompts before applying.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "server" / "app"))

from crontab_tz import (
    convert_crontab,
    get_pi_utc_offset_minutes,
    read_crontab,
    resolve_timezone,
    write_crontab,
    PI_TIMEZONE,
)


def _offset_label(offset_min: int, iana_name: str = "") -> str:
    h, m = divmod(abs(offset_min), 60)
    sign = '+' if offset_min >= 0 else '-'
    suffix = f" (UTC{sign}{h:02d}:{m:02d})"
    return (iana_name + suffix) if iana_name else f"UTC{sign}{h:02d}:{m:02d}"


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] in ('-h', '--help'):
        print(__doc__)
        sys.exit(0 if sys.argv[1:] == ['--help'] else 1)

    tz_str = sys.argv[1]

    try:
        tz_label, user_offset_min = resolve_timezone(tz_str)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    pi_offset_min = get_pi_utc_offset_minutes()

    print(f"Pi timezone:   {_offset_label(pi_offset_min, PI_TIMEZONE)}")
    print(f"Target:        {tz_label}")
    print()

    try:
        original = read_crontab()
    except RuntimeError as e:
        print(f"Error reading crontab: {e}", file=sys.stderr)
        sys.exit(1)

    new_crontab, changes = convert_crontab(original, user_offset_min, pi_offset_min)

    if not changes:
        print("No cron lines needed conversion.")
        sys.exit(0)

    print("Changes:")
    for c in changes:
        if c['changed']:
            print(f"  - {c['original']}")
            print(f"  + {c['converted']}")
            if c['day_delta']:
                print(f"    (day shift: {c['day_delta']:+d})")
        if c['warning']:
            print(f"  ⚠  {c['warning']}")
        if c['changed'] or c['warning']:
            print()

    answer = input("Apply changes? [y/N] ").strip().lower()
    if answer != 'y':
        print("Aborted.")
        sys.exit(0)

    try:
        write_crontab(new_crontab)
    except RuntimeError as e:
        print(f"Error writing crontab: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Crontab updated to {tz_label}.")


if __name__ == '__main__':
    main()
