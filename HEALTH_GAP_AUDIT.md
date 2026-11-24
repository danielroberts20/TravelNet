# Health gap audit — 2026-06-10 → 2026-11-24

Full-history gap scan and phone recovery, run 2026-11-23/24 using the HAE MCP recovery flow.
Pre-2026-06-10 data was deliberately left out (deferred).

## Outcome

| | Events |
|---|---|
| Resolved (data recovered from the phone) | 42 |
| Genuinely missing (phone answered completely with nothing) | 42 |
| Still pending | 0 |

Every gap event now has a final verdict. Every "genuinely missing" verdict rests on a
complete pull: a dropped connection never marks anything missing (see *Safety* below).

Rows in the DB, before the backfill (`pre_backfill_*.db`) vs now:

| Table | Before | Now | Added |
|---|---|---|---|
| `health_quantity` | 72,726 | 77,516 | +4,790 |
| `health_heart_rate` | 27,929 | 28,263 | +334 |
| `health_sleep` | 3,205 | 3,269 | +64 |

The NULL-source duplicates were removed first (backup taken). The DB has 0 duplicates and `quick_check` is ok.

## Sessions

| Tier | What | Result |
|---|---|---|
| 1 (loud) | 47 events: watch vitals, activity totals, sleep | 12 recovered, 35 missing, over 2 sessions |
| 2 (walking) | 23 events | Walking metrics almost entirely recovered |
| 3 (sparse) | VO2 Max, Respiratory Rate, Wrist Temperature | 5 events: 4 recovered, 1 missing (VO2 Max) |

The MCP names for Respiratory Rate, Apple Sleeping Wrist Temperature and VO2 Max
(`respiratory_rate`, `apple_sleeping_wrist_temperature`, `vo2_max`) were confirmed against the phone.
Only Resting Energy needed an alias (`basal_energy_burned`).

## Per metric (metric-level windows; one event can cover several metrics)

| Metric | Recovered | Missing |
|---|---|---|
| Walking Step Length | 17 (681h) | 0 |
| Walking Double Support Percentage | 16 (635h) | 0 |
| Walking Speed | 15 (606h) | 0 |
| Walking Asymmetry Percentage | 22 (925h) | 3 (107h) |
| Respiratory Rate | 3 (331h) | 0 |
| Apple Sleeping Wrist Temperature | 1 (192h) | 0 |
| Step Count / Walking + Running Distance | 1 each (19h) | 0 |
| Flights Climbed | 5 (141h) | 8 (283h) |
| Physical Effort | 3 (129h) | 7 (172h) |
| Time in Daylight | 2 (63h) | 11 (383h) |
| Active Energy | 2 (42h) | 6 (147h) |
| Apple Exercise Time | 1 (22h) | 6 (194h) |
| Resting Energy | 1 (19h) | 3 (77h) |
| Blood Oxygen Saturation | 2 (45h) | 18 (432h) |
| Heart Rate Variability | 2 (46h) | 14 (333h) |
| Environmental Audio Exposure | 2 (41h) | 9 (220h) |
| Heart rate | 2 (39h) | 9 (219h) |
| Apple Stand Hour / Apple Stand Time | 0 | 5 each (about 140h) |
| Sleep | 2 (120h) | 2 (96h) |
| VO2 Max | 1 (70h) | 2 (238h) |

## Likely causes of the genuinely-missing gaps

These are inferences from the pattern. The phone had no data to give, so the cause cannot be
checked from the DB.

- **Watch not worn or charging.** Apple Stand Hour, Apple Stand Time, Active Energy, Exercise Time,
  Flights Climbed and Physical Effort go missing together (for example 2026-09-06 → 09-09 and
  2026-08-25 → 08-27). That is the Watch off the wrist. 37 of the 42 missing events involve a loud metric.
- **Sparse by nature.** Blood Oxygen Saturation, HRV and Environmental Audio Exposure are only
  measured intermittently, so a long quiet stretch is partly normal. Their thresholds probably flag
  some gaps that were never really missing.
- **Time in Daylight and VO2 Max.** They are only recorded outdoors or on workouts. Quiet days look
  like gaps. The VO2 Max threshold was already raised to 7 days (PR #62); 2 of its 3 gaps are still
  about a week or longer and are probably real.
- **Sleep, 2 events of exactly 48h** (2026-07-22 → 07-24 and 07-27 → 07-29). Both run midnight to
  midnight, so they look like nights with no Watch sleep tracking.
- **Walking metrics.** These were almost entirely recovered, so the phone had the data and the
  REST pushes were what missed it. Only 3 Walking Asymmetry gaps were genuinely missing.

## Safety (what the verdicts rely on)

- Only a complete answer can mark a gap genuinely missing. A dropped connection or an error
  leaves the gap pending and it is retried.
- An ongoing gap is never declared absent, and a verdict needs a pull made after the gap closed.
- Two drops occurred on 2026-11-24 at about 07:27 and 07:28 UTC (a timeout, then a refused
  connection). Neither caused a wrong verdict: the affected gaps were retried and finished in
  later sessions. The cause is unknown; later sessions ran 10 minutes with no drops.
- Two overlapping runs in the first loud session doubled requests but caused no data damage.
  A single-run lock now prevents that.

## Left over

- **Pre-2026-06-10 history** is not scanned.
- The three `pre_*.db` backups in the data dir (about 1.1 GB each) can be moved to `backups/db`
  or deleted.

## Operating the recovery flow

Settings in `server/.env`: `HEALTH_GAP_NOTIFICATION` (the Pushcut notification whose Shortcut
starts HAE's MCP server), `HAE_HOST` (the phone's Tailscale IP), `HAE_PORT` (9000) and `HAE_TOKEN`.
Tunables (`config_overrides.json`): `HEALTH_GAP_PULL_PAD_HOURS` (12), `HEALTH_GAP_RECOVERY_COOLDOWN_HOURS` (20),
`HEALTH_GAP_RECOVERY_WAIT_MINUTES` (30), `HEALTH_GAP_RECOVERY_MAX_REQUESTS` (600).

- **Daily:** the `Check Health Gaps` flow scans and triggers recovery. Loud gaps prompt (20h cooldown);
  walking-only gaps prompt on the Monday digest day; sparse-only gaps never prompt alone.
- **Manual session:** `docker exec server-prefect-worker-1 prefect deployment run 'Recover Health Gaps/Recover Health Gaps' --param force=true --watch`
  (`max_tier=1|2|3`, `max_requests`, `dry_run=true` are also available).
- **Full-history scan:** the `Backfill Health Gaps` flow is silent and dry by default; pass `write=true` to record events.
- **During a session:** keep the phone unlocked with HAE in the foreground until the "done" notification.
  HAE's server cannot be stopped remotely, so stop it yourself afterwards.
