"""
config/schedules.py
~~~~~~~~~~~~~~~~~~~~
Pure schedule data — deployment names, cron expressions, and descriptions.
No flow imports. Imported by both deployments.py (to build deployments) and
update_timezone.py (to update deployment schedules via the Prefect API).
Keeping this separate breaks the circular import that would arise if
update_timezone.py imported from deployments.py.
"""

# {deployment_name: (cron_expression_or_None, description)}
# None cron = manual-only deployment (no automatic schedule).
SCHEDULE_CONFIGS = {
    # FX rates
    "get-fx-daily":                     ("0 2 * * *",               "Daily FX rate retrieval for previous day"),
    "get-fx-up-to-date":                ("0 3 8,15,22,28 * *",      "Backfill any missing FX dates up to today"),
    "reset-api-usage":                  ("0 0 1 * *",               "Reset monthly API call counters on the 1st"),

    # Location & geocoding
    "geocode-places":                   ("30 4 * * *",              "Daily reverse geocoding of uncoded places"),
    "backfill-place":                   ("15 5 * * *",              "Daily backfill of place_id on health/transaction rows"),
    "identify-location-noise":          ("0 * * * *",               "Daily flagging of noisy location points"),
    "retroactive-location-scan":        ("15 3 */2 * *",            "Every other night: retroactive scan for missed location stays"),

    # Weekly location analysis (Sunday)
    "weekly-location-analysis":         ("45 4 * * 0",              "Weekly: geocode new places then detect timezone/country transitions and flights"),

    # Manual-only subflow re-runs
    "detect-country-transitions":       (None,                      "Manual: detect country crossings from location history"),
    "detect-timezone-transitions":      (None,                      "Manual: detect IANA timezone changes from location history"),
    "detect-flights":                   (None,                      "Manual: detect flight gaps from location history"),

    # Weather
    "get-weather":                      ("30 5 * * *",              "Retroactive weather fetch for previous days (configurable)."),

    # Transactions & finance
    "backfill-gbp":                     ("30 2 * * *",              "Daily backfill of NULL amount_gbp values using stored FX rates"),
    "send-transaction-reminder":        ("0 8 2 * *",               "Monthly push notification to upload transactions"),
    "categorise-transactions":          ("0 3 3,10,17,24 * *",               "Use LLM-based classification to add categories to transaction data"),

    # Health
    "check-health-gaps":                ("50 5 * * 1",              "Weekly check for missing or partial health metric days"),

    # Journal
    "check-journal-staleness":          ("0 */4 * * *",             "Every 4 hours: send push notification if journal is stale"),

    # Notifications & digests
    "send-warn-error-log":              ("0 6 * * *",               "Daily flush and email of WARNING/ERROR log digest"),
    "send-cron-digest":                 ("0 9 * * *",               "Daily safety-net flush of cron job digest"),

    # Backups
    "backup-db":                        ("0 1 * * *",               "Daily local DB snapshot with 10-day retention"),
    "backup-db-to-cloudflare":          ("0 3 2,9,16,23 * *",       "Weekly backup of database to Cloudflare"),

    # Stats
    "push-public-stats":                ("0 7 * * *",               "Daily push of public_stats.json to GitHub"),

    # Watchdog
    "check-watchdog":                   ("*/5 * * * *",             "Check for recent Watchdog heartbeat and send alert if stale"),

    # Power
    "poll-shelly":                      ("*/5 * * * *",             "Poll Shelly device for power readings"),

    # Daily summary
    "compute-daily-summary":            ("0 8 * * *",               "Compute health, location and Pi domains for the daily summary"),
    "daily-summary-transactions":       ("30 8 4 * *",              "Compute & backfill transaction domain for the daily summary"),
    "daily-summary-weather":            ("0 6 * * *",             "Compute & backfill weather domain for the daily summary"),
    "daily-summary-health":             (None,                      "Manual: Compute health domain for the daily summary"),
    "daily-summary-location":           (None,                      "Manual: Compute location domain for the daily summary"),
    "daily-summary-pi":                 (None,                      "Manual: Compute Pi domain for the daily summary"),

    # Sleep reminder
    "sleep-reminder-schedule":          ("0 12 * * *",              "Calculate and schedule sleep reminder for tonight"),
    "sleep-reminder-notify":            (None,                      "Manual: Send sleep reminder. Scheduled by the schedule flow")
}
