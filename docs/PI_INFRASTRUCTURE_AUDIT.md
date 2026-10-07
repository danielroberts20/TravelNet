# Pi Infrastructure Audit — 2026-10-06

Scope: the production Pi (`pi-server`, Pi 4B, 4 cores, 3.7 GiB RAM, SD root, 500 GB SSD `/mnt/ssd`, 6 TB HDD `/mnt/linux` + `/mnt/personal`). Covers TravelNet (API, worker, dashboard, nginx, Prefect server), Trevor, Constellation, the health-check cron, the Pi-side hooks for the Watchdog (which runs on another Pi), and the host OS.

Method: read-only inspection (`docker inspect/stats`, cgroup v2 counters, `/proc`, systemd units, crontabs, compose files, Dockerfiles, Prefect DB queried read-only, logs). **Nothing was changed.** Observation window: 5 days 5 h of uptime (last reboot 2026-10-01 18:00, the scheduled monthly one).

---

## 1. Executive summary

The platform is in decent shape: no OOM kills anywhere, no throttling (`get_throttled=0x0`), 52 °C, 80 % CPU idle, SSD at 4 % and every long-running container has a restart policy. The problems are all about **memory pressure and one runaway data store**, not CPU or disk capacity.

| # | Finding | Severity |
|---|---|---|
| 1 | **Prefect's SQLite DB is 2.4 GB** (83 k flow runs, 848 k log rows). The Prefect server is the biggest process on the box (609 MB RSS, 1.33 GB cgroup peak, **no memory limit**) and caused ~80 % of all SSD writes (18.6 GB in 5 days). 88 % of runs come from two `*/5` flows. | High |
| 2 | **The prefect-worker repeatedly hits its 768 MB cap** (cgroup peak = exactly the limit, `memory.events max` = 92,535 hits). Measured: a manual noise-flow run spiked to the cap but memory stayed high after exit, i.e. mostly **page cache from full scans of the 942 MB DB** (no index on `horizontal_accuracy`), not a leak. Real anonymous-memory culprits are probably the long flows (Backfill Place, Weekly Location Analysis); still to be profiled. | Medium |
| 3 | **Swap is 66 % full** (1.3 GB in zram) with only 1.4 GB "available". TravelNet's own containers are the ones paged out (dashboard 83 MB swapped vs 19 MB resident; Trevor 102 MB vs 11 MB), while the interactive dev sessions (`user-1000.slice`) hold 730 MB resident **plus 754 MB in swap**, i.e. ~1.5 GB, and account for more than half of all swapped memory. | High |
| 4 | **Zero Docker healthchecks.** A hung (not crashed) container is never restarted, and the host health script only checks "is running". | Medium |
| 5 | **Containers depend on Tailscale for Prefect**: `PREFECT_API_URL=http://<pi-tailnet-host>:4200/api` in both `ingest` and `prefect-worker`. They share a Docker network with `prefect-server`, so this is a needless dependency, and a landmine given the Tailscale retirement in progress. | Medium |
| 6 | `database is locked`: ~100 in 7 days on both API and worker. All 15 failed power polls fall in one ~15-minute daily window (05:15–05:30 local), consistent with the long-running `Backfill Place` job holding the write lock; API ingest writes in that window are exposed to the same lock. See §3.2a. | Medium–High |
| ~~6b~~ | ~~FX backfill is failing~~ **RETRACTED (§23):** those "failures" were unit-test runs written into the production Prefect server by `pytest`. The real FX flows complete daily and `fx_rates` is complete through 2026-10-04 (the flow fetches *now − 2 days* by design). | n/a |
| 7 | Resource protection is inconsistent: only Constellation has CPU/PID caps and weights; Prefect server and the dev sessions have none. | Medium |
| 8 | Housekeeping: unused background services, stale Tailscale-era cron jobs and configuration clean-up. Hardening items are tracked separately. | Low–Medium |

If you do only three things: (a) prune + cap the Prefect DB and move the two `*/5` flows out of Prefect, (b) point containers at `http://prefect-server:4200/api`, (c) put a `MemoryHigh`/`CPUWeight` slice on interactive/dev sessions. Together these should take the Pi from "permanently swapping" to comfortable headroom.

---

## 2. Host

### Observed

| Item | Value | Verdict |
|---|---|---|
| RAM | 3.7 GiB; 2.3 used, 1.4 buff/cache, **0.15 free**, 1.4 available | Tight |
| Swap | zram 2 GiB (zstd), 1.3 GiB used → 317 MB RAM; `page-cluster=0`; swappiness 60 | Right design, but too full |
| CPU | load avg 1.0–2.0 on 4 cores, ~80 % idle; governor `ondemand`, 1.5 GHz max | Fine |
| Thermal | 51–53 °C, never throttled | Fine |
| Root (SD) | 29 GB, 33 % used, only ~1.3 GB written since boot | Good — hot data is off the SD |
| SSD | 458 GB, 4 % used, mq-deadline, ~23.5 GB written since boot. SMART: 98 % wear-leveling remaining, 0 reallocated/CRC errors, 16.4 TB written lifetime (~5 % of rated 300 TBW), 12,043 power-on hours | Healthy; see §3.1 |
| HDD | 6 TB WD60EDAZ (an SMR Red model), 3.4 GB of DB backups on the ext4 partition, NTFS (`fuseblk`) personal share on the same disk | Fine for sequential backups |
| Docker | data-root on SSD, json-file logs capped 10 MB × 3 | Good |
| journald | 70 MB persistent on SD | Fine |
| Kernel | no PSI (`/proc/pressure/*` absent) | Can't measure stall time |

### Who is using memory (RSS)

| Process | RSS | Notes |
|---|---|---|
| Prefect server | **609 MB** | No limit |
| Claude Code remote sessions (6 procs) | **~1.0 GB** | Interactive tooling on the production host |
| TravelNet API (uvicorn) | 133 MB | |
| dockerd + containerd + tailscaled | 83 + 38 + 80 MB | |
| Samba (smbd/nmbd/winbindd/rpcd) | ~100 MB | |
| Constellation (2 gunicorn workers + master) | ~135 MB | |
| Prefect worker | 35 MB idle (spawns a subprocess per flow run) | |
| cloudflared, NetworkManager, journald, WoL service | 24 / 13 / 16 / 12 MB | |

The two big avoidable items are the Prefect server's working set (driven by DB size) and interactive dev sessions. Everything the containers *need* fits comfortably.

### Recommendations

1. **Cap interactive/dev sessions** so TravelNet always wins (they run under `user-1000.slice`). Measured 2026-10-06: 105 tasks, **730 MB resident, 754 MB swapped, peak 1.5 GB resident**. Of the ~1.3 GB in zram, ~754 MB is this slice and ~630 MB is the containers (travelnet 217, prefect-server 116, worker 113, trevor 102, dashboard 83). The biggest single fix for swap is therefore closing idle Claude Code/VS Code sessions (several are 12+ hours old at ~75–300 MB each), with the slice limit as a backstop:
   ```ini
   # /etc/systemd/system/user-1000.slice.d/limits.conf
   [Slice]
   MemoryHigh=900M    # throttle/reclaim, don't kill the editor (peak seen: 1.5G)
   CPUWeight=50       # default 100, containers get 100+
   IOWeight=50
   ```
   `sudo systemctl daemon-reload`. Use `MemoryHigh` rather than `MemoryMax` so a runaway session is slowed, not OOM-killed mid-edit. `vscode-server` (5 GB on SSD) is not running now but is the same class of load when connected.
2. **Disable what a headless wired box doesn't use**: `bluetooth`, `ModemManager` (both enabled), likely `wpa_supplicant`/`dhcpcd-wlan0` (wlan0 is DOWN/unmanaged; `dhcpcd-wlan0` is the 4th-slowest unit at boot, 30 s), and `avahi-daemon` if you never use `.local` names. Saves ~20–40 MB and boot time. Keep wlan0 if it's your intended fallback.
3. **Samba**: ~100 MB resident; stop the NetBIOS name service and `winbind` if only a plain file share is needed.
4. **Enable PSI** to get real memory-stall numbers. Verified: kernel has `CONFIG_PSI=y` with `CONFIG_PSI_DEFAULT_DISABLED=y`, so appending `psi=1` to the single line in `/boot/firmware/cmdline.txt` and rebooting will enable `/proc/pressure/{cpu,memory,io}` (also per-cgroup `memory.pressure`).
5. **Swap threshold**: leave zram (zstd, `page-cluster=0` is correct for zram). Once the footprint shrinks, raising `vm.swappiness` to ~100 is reasonable for zram, but don't touch it before reducing demand.
6. **CPU**: `ondemand` is fine; `performance` would remove ramp latency at ~no cost at these temps. The board is Rev 1.2 and `arm_boost` has no effect (max stays 1.5 GHz); overclocking isn't worth it given you're not CPU-bound.
7. **Reboot cadence**: monthly reboot at 18:00 on the 1st is fine. The monthly `smartctl -t long /dev/sdb` at 04:00 on the 1st takes many hours on a 6 TB disk and competes with HDD backups; it will finish before 18:00 but is worth moving to a quiet weekend night if the disk ever feels slow.

---

## 3. TravelNet

### 3.1 Prefect server (`prefect-server`, run by `prefect-server.service`)

**Facts**

- Image `prefecthq/prefect:3-latest` (running 3.6.25): **floating tag**, so a reboot-time `docker run` can pull a different major behaviour at any time. (Image is cached locally so it only changes if pulled, but pin it.)
- Started via `docker run` in a systemd unit: `Restart=always` (good), but **no `--memory`, `--cpus`, `--pids-limit`, `--init`, healthcheck, or log-driver override** (inherits daemon default).
- DB: `/mnt/ssd/docker/services/prefect/data/prefect.db` = **2.4 GB**. Rows: `flow_run` 83,714; `task_run` 238,338; `flow_run_state` 409 k; `task_run_state` 719 k; **`log` 847,837**; `events` 61 k; `event_resources` 341 k. Oldest run is 2026-05-31, so nothing has ever been pruned.
- Last 24 h: ~660 flow runs: **Check Watchdog 288, Poll Shelly 288**, Identify Location Noise 24, rest ~60. About 6.5 k log rows/day.
- Container I/O: 18.6 GB written / 3.6 GB read in 5 days (SSD total was ~23.5 GB). Peak cgroup memory 1.33 GB (includes reclaimable page cache from the 2.4 GB DB, but RSS alone is 609 MB).

**Why it matters**: SQLite scans and WAL checkpoints over a 2.4 GB file inflate RSS and I/O for what is, functionally, a scheduler for ~30 deployments. SSD wear itself is not a concern (870 EVO is rated ~300 TBW; 4.5 GB/day is trivial), but the write amplification means every flow run is slower and the server is memory-hungry.

**Recommendations**

1. **Stop 576 of ~650 daily runs at the source.** `check-watchdog` (a single indexed SELECT) and `poll-shelly` don't need a Prefect run each (subprocess spawn, ~5 state rows, ~5 task rows, ~20 log rows). Move them to lightweight in-process scheduled tasks in the FastAPI app (or the existing cron), keeping the same logic and notifications. This alone removes ~88 % of Prefect DB growth.
2. **Retention.** Delete flow runs older than e.g. 14 days via the Prefect API in batches (cascades to states/logs/task runs), then stop the server and `VACUUM` the DB once (needs ~2.5 GB temp; SSD has 419 GB free). Put it in a weekly flow. **Verified built-in settings (Prefect 3.6.25 defaults):** `PREFECT_SERVER_SERVICES_DB_VACUUM_ENABLED={'events'}` (only *events* are cleaned today; flow runs/logs are not), `..._RETENTION_PERIOD=90 days`, `..._BATCH_SIZE=200`, `..._LOOP_SECONDS=3600`, `PREFECT_EVENTS_RETENTION_PERIOD=7 days`, `PREFECT_LOGGING_TO_API_ENABLED=True`, `PREFECT_SERVER_ANALYTICS_ENABLED=True`. So enabling the built-in service for old flow runs is possible (confirmed from the 3.6.25 source: valid members are `events` and `flow_runs`; `PREFECT_SERVER_SERVICES_DB_VACUUM_ENABLED=events,flow_runs` enables both, and `true` maps to both. Set the retention as plain seconds, e.g. `...RETENTION_PERIOD=1209600` for 14 days, to avoid env-var timedelta parsing surprises. Not yet confirmed whether `flow_runs` vacuum also deletes the associated log rows, which are the bulk of the DB; check log row counts after the first cycles), but at 200 runs/hour it clears only ~4,800/day; with ~75 k runs older than 14 days the backlog would take ~15 days. Use a one-off bulk delete for the backlog, then let the service (or a weekly flow) keep up; shorten retention from the 90-day default to ~14 days. Also set `PREFECT_SERVER_ANALYTICS_ENABLED=false`. SQLite won't shrink until a manual `VACUUM`.
3. **Reduce log volume**: `PREFECT_LOGGING_TO_API_ENABLED=false` on the worker would stop shipping logs to the server DB (the app already has its own file/DB logging via `config/logging.py`), at the cost of no logs in the Prefect UI. Optional; weigh against UI debugging value. Also `PREFECT_SERVER_ANALYTICS_ENABLED=false`.
4. **Limits** (after the prune): `--memory=768m --memory-swap=768m --cpus=1.5 --pids-limit=256 --init`, plus `--memory-reservation=384m` so it is protected from reclaim ahead of Constellation. If you set the cap *before* pruning, expect heavier swapping; do (2) first.
5. **Pin** the image: `prefecthq/prefect:3.6.25-python3.12` (or the digest).
6. **Add a healthcheck** (`/api/health`).
7. Move it into `docker-compose.yml` as a service. Today it's the one piece of TravelNet outside compose, so `build.sh`, `docker compose stop` (used by `graceful_reboot.sh`) and `depends_on` ignore it; the `travelnet` network only exists because compose creates it, which makes the unit's `--network travelnet` start-order dependent (it works today because of `Restart=always`).

### 3.2 Prefect worker (`server-prefect-worker-1`, `python -m deployments`)

**Facts**: 768 MB limit, swap allowed (memswap 1.5 GB). Now 140 MB / 35 MB RSS idle, **but peak = 805,318,656 bytes = the limit exactly, and `memory.events: max 92,535`**: the cgroup repeatedly hit the cap and was forced to reclaim/swap, which makes the offending flow slow but not dead (no `oom_kill`). 112 `database is locked` log lines in 7 days.

**Original hypothesis (partly refuted by measurement on 2026-10-06)**: I suspected `flag_location_noise.py::flag_tier2_noise` (hourly; `fetchall()` with a watermark tied to the last *flagged* noise row). A manual run showed otherwise:
- Over 7 days the flow ran 168 times, avg 2.3 s, max 10.1 s. Tier 2 returned in ~0.4 s with 0 rows, so the watermark is not causing a full re-scan today. **Not the primary cause.** (The hourly-vs-"Daily" schedule mismatch is still real but cheap.)
- During the manual run, cgroup memory went 266 MB → 790 MB → 805 MB (the cap) within ~4 s, during flow start / tier 1, then **stayed at ~694 MB after the process exited**. Memory that survives process exit is page cache, not RSS. Tier 1 filters on `horizontal_accuracy` with no index, so it full-scans the 942 MB `travel.db`, and those file pages are charged to the worker's cgroup. This is reclaimable and mostly benign, but it explains why `memory.peak` equals the limit and why `memory.events max` is so high.
- Baseline worker is ~135 MB. Each `*/5` run (Check Watchdog / Poll Shelly) adds ~100 MB per flow subprocess (345 MB with two concurrent), confirming the per-run import cost.
- The real anonymous-memory candidates are the long flows in the last 7 days: **Weekly Location Analysis (1,508 s)**, **Backfill Place (avg 911 s, max 1,177 s, daily)**, Detect Country Transitions (883 s), Detect Timezone Transitions (575 s), Get Weather (226 s avg). These have not yet been profiled for anon vs file memory; see the sampler in §9.

**Recommendations**

1. Add an index for tier 1 (`horizontal_accuracy`, ideally partial `WHERE horizontal_accuracy > 100`) so the hourly run stops scanning the whole DB file; make the schedule match intent (daily, or keep hourly now that it is cheap). Optionally persist "last examined timestamp" rather than "last flagged". Then profile the long flows (Backfill Place, Weekly Location Analysis, Detect Country/Timezone Transitions) for `fetchall()`-style loads.
2. Once memory is under control, keep 768 MB; set `mem_reservation: 192m`.
3. Prefect `serve()` runs each flow run as a subprocess, so every `*/5` run re-imports Prefect + the app; this is the main CPU churn source (see 3.1.1).
4. Add `pids_limit`, `cpus: 2.0` and a healthcheck (process-level, e.g. the Prefect runner's heartbeat or checking `pgrep -f deployments`).
5. `PREFECT_API_URL` should be `http://prefect-server:4200/api` (see §6).

### 3.2a Flow failures and stuck runs (7-day state breakdown)

| Flow | State | Count |
|---|---|---|
| Get Power Statistics (poll-shelly, 2,016 runs) | **Failed** | 15 |
| Backfill FX (get-fx-up-to-date) | **Failed** | 4 *(test-suite artifacts, see §23)* |
| Check Watchdog / Get Power Statistics / Identify Location Noise | **Submitting** | 2 / 2 / 1 |

- Everything else non-completed is a future `Scheduled` run (Prefect pre-creates them), not a failure.
- **All 15 power-poll failures are `OperationalError: database is locked`** (confirmed per run), and they occur in a tight daily window: runs scheduled at 18:15–18:30 UTC on Oct 3–6 and 19:15–19:30 UTC on Sep 30–Oct 2 (3 failures per day, 2 on some days). The window moves by exactly one hour between Oct 2 and Oct 3. That pattern matches a **daily job in the trip's local timezone** (schedules are built in `get_current_timezone()`) that holds the SQLite write lock for ~15 minutes. The one-hour shift matches a daylight-saving change in the local timezone: before and after it the failures fall at 05:20 local. **Still inferred, not verified:** the lock holder is `Backfill Place` (`15 5 * * *` local; it averages 911 s and peaks at 1,177 s). `Get Weather` (05:30 local, 226 s avg) follows immediately and extends the window. Because the lock lasts longer than the 30 s `busy_timeout`, task retries would not help; the writer must commit in small batches.
- Lost data from this: ~3 missed 5-minute power samples per day. **More important, any API ingest write (phone uploads via `BackgroundTasks`) landing in that window will hit the same lock**; the `travelnet` (API) container logged 98 `database is locked` lines in 7 days. Whether those lost uploads are retried by the clients is unknown and worth checking.
- `Submitting` is a transient state while the runner spawns a subprocess. The three listed were created on 2026-10-01 (15:00 and 16:55 UTC), i.e. before the monthly 18:00 reboot, and have no start time: they are **orphans from the reboot** that will never run (harmless, but they never clear on their own; the retention delete will remove them). It also shows the reboot script stops containers without waiting for in-flight flow runs.
- **RETRACTED, see §23: these Backfill FX runs were produced by running the unit tests, not by a broken FX key. The text below is kept only as a record of the mistake.**
- ~~**Backfill FX is an application/config failure, not infrastructure.**~~ Four runs within ~70 s on 2026-10-01 05:09–05:10 UTC (probably manual retries) failed with: `No API quota remaining, aborting`; `invalid_access_key: Invalid key` (twice); and `Date range exceeds 365 day API limit (516 days) — use get_fx_for_month to backfill manually`. So the FX API key is invalid or revoked, the monthly quota was exhausted, and the backfill gap is **516 days** (back to about mid-2025), i.e. historical FX rates (and therefore `amount_gbp` conversions) may be missing for most of the trip. Next scheduled run is Oct 8; it will fail the same way unless the key is fixed. Worth checking independently of the Pi work.

### 3.3 TravelNet API (`travelnet`)

- 1 GB limit, 123 MB RSS, 343 MB cgroup (cache included), peak 998 MB (limit nearly reached once: likely a backup/ML/analysis path or startup). Single uvicorn worker, which is the right choice for a single-writer SQLite DB.
- BackgroundTasks DB writes are non-blocking (per CLAUDE.md), good for ingest latency.
- `./app:/app` bind mount in production (dev convenience): image rebuilds don't affect running code, and a `git checkout` mid-flight changes live code. Acceptable for a personal system, but be aware.
- No healthcheck, no `cpus`/`pids_limit`, no `mem_reservation`.

**Recommendations**: `mem_limit: 1g` (keep), `mem_reservation: 384m`, `cpu_shares: 1024` (default is 1024 in compose, but Constellation is 256, so relative priority is already correct; raise only if you want TravelNet to dominate Prefect too), `pids_limit: 256`, and a Python-based healthcheck (the slim image has no `curl`). **The API has no `/health` route** (404; the only status-like routes are `/metadata/status`, `/metadata/widget_status`, `/compute/pc-status`). Add a trivial unauthenticated `GET /health` that returns 200 without touching the DB, or, as a no-code interim, treat any HTTP response `< 500` from `/` as alive (proves the event loop answers).

### 3.4 SQLite (`travel.db`, 942 MB)

- WAL mode set per connection (good). `timeout=30` on writers; **read-only connections use Python's default 5 s timeout** and no busy handling.
- Not set anywhere: `synchronous=NORMAL` (safe with WAL, fewer fsyncs), `busy_timeout` on the read-only path, `cache_size`, `mmap_size`, `wal_autocheckpoint`/periodic checkpoint tuning, `temp_store=MEMORY` on a memory-tight box (leave default here).
- `database is locked` (≈100/7 days, 28 in `upsert_aggregate`) generally comes from a read-then-write transaction upgrading while another writer holds the lock (SQLite returns BUSY immediately and ignores the timeout in that case), or long-running readers blocking checkpoints.
- Health monitor already watches WAL size (currently 5.8 MB) — good.

**Recommendations**
1. In `get_conn`, add `PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=30000;` (and a timeout on the read-only path).
2. Use `BEGIN IMMEDIATE` for any transaction that reads then writes (e.g. `upsert_aggregate`) so it waits instead of failing.
3. Add retry-with-backoff around Prefect tasks that hit `OperationalError: database is locked` (Prefect `retries=3, retry_delay_seconds=[5,15,45]`).
4. Keep write bursts out of the same minute: many cron slots coincide (00:00, 03:00, `*/5`, hourly noise run).
5. Consider a weekly `PRAGMA wal_checkpoint(TRUNCATE)` + `PRAGMA optimize` (cheap, in a low-traffic slot).
6. **Fix the root cause of the daily lock window:** make `Backfill Place` (and `Get Weather`, `Weekly Location Analysis`, transition detectors) commit in small batches (e.g. every few hundred rows) instead of holding one long write transaction, and do read-heavy computation outside the transaction. That removes the 3 failed power polls/day and protects ingest writes. Also stagger these long writers so they never overlap, and add retries with backoff on the ingest insert path.

### 3.5 Dashboard (`travelnet-dashboard`)

- 384 MB limit; steady 16–19 MB resident (the rest is swapped: **83 MB in zram**, so the first request after idle pays page-in cost), peak 207 MB. 21 PIDs.
- `/home/dan/services/Dashboard:/app` bind mount **replaces the image's `/app`**, including the frontend that the Dockerfile builds in stage 1. Production therefore depends on `static/dist` existing on the host, which makes the Node build stage dead weight (and the `server-dashboard` image is 360 MB for no benefit). Pick one: bind-mount for dev with a prod override, or bake the image.
- Dockerfile CMD says gevent × 2; confirm it's what's running (the running Constellation shows gthread; the dashboard process list wasn't distinguishable).

**Recommendations**: `mem_limit: 256m`, `mem_reservation: 96m` (keeps it resident), healthcheck, `pids_limit: 128`.

### 3.6 nginx (`travelnet-nginx`)

- 64 MB limit, 3 MB used. Right-sized.
- Config is minimal: no `worker_processes`, no gzip, no `proxy_http_version`/keepalive upstream, no timeouts beyond Constellation's; defaults are fine at this traffic.
- Monthly cron `tailscale cert … && docker restart travelnet-nginx` and the `scp` of that cert to the Watchdog Pi are leftovers: the Tailscale server block was retired in commit `468a612`. They run harmlessly today but will start failing (and restart nginx for nothing) once Tailscale is removed.

### 3.7 Health checker (`scripts/check_system_health.py`, root cron `*/15`)

Strong points: cooldown files, SMART throttling, WAL mitigation, OOM-kill check, auto-start of downed containers, emergency backup on SMART failure.

Gaps:
1. **`docker start` only helps exited containers.** A hung API (deadlocked, thread-starved, DB-locked) stays "running" and healthy-looking. Add Docker healthchecks and make the script act on `State.Health.Status == "unhealthy"` (restart), or add an HTTP probe.
2. **`EXPECTED_CONTAINERS` omits `constellation`**, and the host services that are the actual public ingress (`cloudflared`, `prefect-server.service`, `docker`) aren't checked.
3. **Swap alert is permanently tripped** (66 % ≥ 60 %, `suppressed (cooldown)` on every run). With zram the percentage is meaningless as a health signal; alert on *swap-in rate* (`pswpin` delta from `/proc/vmstat`) or PSI when available, otherwise real alerts get lost in noise.
4. **No per-container memory pressure**: add `memory.events` `max`/`oom_kill` deltas per container (this is exactly how the worker problem in 3.2 was found).
5. **No Prefect DB size / flow failure check**, no check that backups were produced in the last 26 h, no cloudflared/tunnel check (the Watchdog on the other Pi probably covers external reachability; confirm it checks `/api/health` through the tunnel).
6. `ENV_FILE` etc. use `/home/dan/services/...` (a symlink to `/mnt/ssd/services`); fine, but note the script and cron fail silently if the SSD mount is missing (`nofail` in fstab). Add a `mountpoint -q /mnt/ssd` check first, since *everything* lives there.
7. Cooldown state is in `/tmp/travelnet_health` (tmpfs-ish): it resets on every reboot, so the first run after the monthly reboot re-sends all alerts. Minor.
8. **Dashboard's API-health panel is probably always green.** `Dashboard/app.py:1380` (`/api/fastapi-health`) GETs `{FASTAPI_URL}/health` and, if the call doesn't raise, returns `status: "ok"` with whatever `code` came back. The API has no `/health` (404 with a JSON body), so the panel reports "ok, code 404" even if the API is half-dead. It should check `resp.ok`, and the API needs a real `/health` route.
9. **Dashboard `/health` is not a health route**: any unknown path returns 200 (SPA catch-all, verified with `/zzz-nonexistent`). Fine as a bare liveness probe (the gunicorn worker answered) but it says nothing about the app; use `/api/status` or a dedicated route if it is unauthenticated.

### 3.8 Watchdog (other Pi) — Pi-side hooks

- `check-watchdog` Prefect flow (every 5 min) alerts if no heartbeat in 10 min and skips when TravelNet has been up < 10 min. Logic is sound; its *cost* is the issue (see 3.1.1). A lighter alternative is to evaluate staleness lazily when the heartbeat endpoint is hit plus a 5-min in-process timer.
- `graceful_reboot.sh` POSTs to `http://<watchdog-lan-ip>/maintenance` on the LAN IP (good — already de-Tailscaled) and `docker compose stop`s before reboot. Two gaps: `prefect-server` isn't part of compose so it's stopped by systemd at shutdown instead of in order, and the script sleeps a fixed 5 s instead of waiting for the containers to exit (it's `compose stop`, which blocks, so this is fine).
- Prefect `serve()` depends on `prefect-server` being up at worker start; `depends_on: ingest` does not cover that. Moving Prefect into compose with a healthcheck and `depends_on: condition: service_healthy` removes the start-order dependence.
- `.env` has `WATCHDOG_MAINTENANCE_URL`; confirm it points at a LAN address rather than a Tailscale hostname.

---

## 4. Trevor

- 512 MB limit (swap up to 1 GB), **9.6 MB RSS idle, peak 313 MB** (chromadb + onnxruntime at startup/ingest). Chroma store is 156 KB. Limit is generous but harmless; **384 MB would still cover the peak** and free budget.
- Healthy: `/health` returns ok with DB and Chroma checks. No healthcheck declared in compose though; add one against `/health` using Python (`python -c "import urllib.request;urllib.request.urlopen('http://localhost:8300/health')"`).
- Resident set is mostly swapped out (102 MB in zram vs 11 MB resident); first chat after idle will be slower for it. `mem_reservation: 128m` would keep it warm.
- `./app:/app` bind mount with `CMD uvicorn` *without* `--reload`, so the comment "hot reload" is misleading; code changes require a restart.
- `gcc` is in the runtime image (966 MB) — use a multi-stage build or wheels if image size matters; not a runtime cost.
- Chroma telemetry logs an error each startup (`capture() takes 1 positional argument…`, a known chromadb 0.5.x / posthog version mismatch). Harmless; `ANONYMIZED_TELEMETRY=False` is already set but the bug logs anyway. Upgrading chromadb or pinning `posthog<6` silences it.
- Depends on `/mnt/ssd/.../travelnet/data:ro` for `travel.db` opened with `mode=ro&immutable=1` — **immutable** means SQLite won't see changes made after open and skips locking. Fine for sporadic queries if connections are short-lived (they are, per `_check_db`), but long-lived connections would serve stale data.
- Outbound dependencies: the Ollama and compute hosts are reached via Tailscale hostnames. `LLM_PROVIDER=openai` today so the Ollama path is idle, but the compute/SSH path will break when Tailscale goes.

---

## 5. Constellation

This is the reference implementation for "make way for TravelNet":

```yaml
mem_limit: 256m, memswap_limit: 256m   # hard cap, no swap
cpus: 1.0, cpu_shares: 256             # cgroup cpu.weight 35 vs 100 default
pids_limit: 128, user: 1000:1000
```
- Using 81 MB / 256 MB, 10 PIDs, 0 % CPU; no OOM or limit hits. Started 33 min ago (a deploy, 0 restarts, `unless-stopped`).
- gunicorn gthread 2 workers × 8 threads: ~60 MB each. One worker would save ~55 MB if traffic is low (family-scale site); not needed unless memory stays tight.
- Nightly `flask sync-locations` at 03:00 via root-less `docker compose exec` inside the 256 MB container; it competes with Prefect flows scheduled at 03:00/03:15 (and Sunday's docker prune at 03:00). Shift to 03:20 to avoid piling on.
- Media on the HDD (`/mnt/linux/.../media`, 190 MB) means a cold request may wait for disk spin-up; thumbs are on SSD, good.
- Not in `EXPECTED_CONTAINERS` and no healthcheck; add both, with alert-only (no auto-restart storm) behaviour.

---

## 6. Tailscale dependencies still live (relevant to the retirement)

| Where | What | Fix |
|---|---|---|
| `server/docker-compose.yml:11,84` | `PREFECT_API_URL=http://<pi-tailnet-host>:4200/api` | `http://prefect-server:4200/api` (same Docker network; faster, no tailnet hop) |
| `prefect-server.service` | `PREFECT_UI_API_URL=…ts.net` | Browser-reachable URL (LAN/Cloudflare) |
| `server/.env` | `WOL_HOST`, `COMPUTE_HOST` on `*.ts.net` | LAN IP / replacement |
| `Trevor/.env` | `OLLAMA_BASE_URL`, `COMPUTE_HOST` | Same |
| dan crontab (1st, 14:00/14:05) | `tailscale cert` + `docker restart travelnet-nginx` + `scp` cert to the Watchdog Pi | Remove (block is retired) |
| `gym-swim.service` | binds Tailscale IP `:5001`, `Wants=tailscaled` | Rebind to LAN/Cloudflare |

The first row is the critical one: **if Tailscale stops, `ingest` and `prefect-worker` lose the Prefect API and all scheduled flows stop**, with no alert from the host health check.

---

## 7. Resource budget proposal (3.7 GiB)

| Component | Now (limit / typical) | Proposed limit | Reservation (`memory.low`) |
|---|---|---|---|
| Host OS + docker + samba + misc | ~0.5 GB | — | — |
| travelnet (API) | 1 G / 130 MB | 1 G | 384 M |
| prefect-worker | 768 M / 35–140 MB (peaks at cap) | 768 M (after flow fix) | 192 M |
| prefect-server | **none** / 610 MB | 768 M (after DB prune) | 384 M |
| dashboard | 384 M / 17 MB | 256 M | 96 M |
| nginx | 64 M / 3 MB | 32 M | — |
| trevor | 512 M / 10 MB (313 peak) | 384 M | 128 M |
| constellation | 256 M, no swap / 81 MB | 256 M (keep) | — |
| Dev sessions (`user-1000.slice`) | none / ~1 GB | `MemoryHigh=1200M`, `CPUWeight=50` | — |

Docker's `mem_reservation` maps to cgroup v2 `memory.low` (best-effort protection from reclaim), which is what makes "TravelNet wins" real: under pressure the kernel reclaims from unprotected cgroups (Constellation, dev sessions) first, rather than evenly. Sum of reservations ≈ 1.2 GB, comfortably within RAM. Limits sum to ~2.9 GB + dev + host, so worst-case simultaneous peaks still need the zram swap, which is why keeping swap *enabled* for TravelNet containers and *disabled* for Constellation (already so) is the right asymmetry.

---

## 8. Prioritised action list

**Do first (hours, high payoff)**
1. Change `PREFECT_API_URL` to `http://prefect-server:4200/api` in compose (both services), recreate.
2. Add a partial index for tier-1 noise (`horizontal_accuracy`); fix hourly-vs-daily schedule; profile long flows (see §9 sampler).
3. Add `user-1000.slice` drop-in limits.
4. Add Docker healthchecks (api, dashboard, trevor, prefect-server, nginx) and teach `check_system_health.py` to restart on `unhealthy`; add `constellation`, `cloudflared`, `prefect-server.service` to checks; replace swap-% alert with swap-in-rate.

**Do next (a day)**
5. Prune Prefect history (>14 days) + `VACUUM`; add weekly retention flow; pin image; add limits.
6. Move `check-watchdog` and `poll-shelly` out of Prefect.
7. SQLite: `synchronous=NORMAL`, `busy_timeout` everywhere, `BEGIN IMMEDIATE` for read-modify-write, retries on lock errors.
8. Apply the memory budget table (limits + `mem_reservation`).

**Cleanup**
9. Remove stale Tailscale cron lines; move Prefect into compose.
10. Disable Bluetooth/ModemManager (+ wlan/avahi/winbind if unused).
11. Dashboard: decide bind-mount vs baked image.
12. *(Hardening item, tracked separately.)*
13. Separate Docker network for Constellation.
14. Make the Dashboard API-health proxy check the real status code, and add `/health` to the API (see §3.7 items 8–9).

---

## 9. What I could not verify

- ~~SSD wear~~ **Resolved:** SMART shows Wear_Leveling_Count 98 % remaining, 0 reallocated sectors, 0 CRC errors, 12,043 power-on hours, Total_LBAs_Written 32.1 G ≈ 16.4 TB (~5 % of the 870 EVO's 300 TBW rating). The lifetime average (~33 GB/day) is far above the current ~4.5 GB/day, so the drive had a heavier past life; wear is not a concern.
- ~~PSI support~~ **Resolved:** supported, disabled by default; add `psi=1` (see §2.4).
- ~~Health endpoint paths~~ **Partly resolved:** FastAPI has **no `/health`** (404). Prefect `/api/health` → 200. Dashboard `/health` → 200 but may be a SPA catch-all (to verify). Trevor `/health` OK.
- Which flow(s) cause the worker's anonymous-memory peaks: the noise-flow hypothesis was refuted (§3.2); the long flows remain unprofiled.
- Non-completed Prefect runs: most flows show exactly 3 non-completed of 9 in 7 days, which looks like pre-created future `Scheduled` runs rather than failures; **Resolved:** see §3.2a (15 power-poll failures, 4 FX failures, 5 `Submitting`). Still needed: failure messages and start times.
- Prefect retention setting names and valid `DB_VACUUM_ENABLED` members: **resolved** (§3.1 item 2). Open: whether `flow_runs` vacuum cascades to `log` rows.
- Failed/`Submitting` run details: **resolved** (§3.2a). Open follow-ups: (a) confirm that `Backfill Place` is the lock holder (e.g. check its transaction pattern, or log `PRAGMA` lock waits at 05:15 local); (b) check the API-side `database is locked` lines (98 in 7 days) to see whether they are lost ingest writes and whether clients retry.
- Worker memory sampler: running since 2026-10-06 23:40 (idle baseline: anon ≈ 20 MB, file ≈ 248 MB, swap ≈ 114 MB). Results pending, ideally through the Sunday 2026-10-11 03:45 weekly analysis.
- `user-1000.slice` usage: **resolved** (730 MB resident + 754 MB swap, peak 1.5 GB; §2).
- GymSwim's resource use (small enough not to appear in the top RSS list).
- Prefect 3.6.25's exact retention-setting names (check `prefect config view --show-defaults`).

---

## 10. Remediation plan (ordered by impact)

Ordering logic: first the steps that free the most memory and protect data for the least effort, then the changes that depend on them. Nothing here has been applied. Times are rough. Run changes outside the daily lock window (about 05:00–05:45 local) and outside `*/5` boundaries where practical.

### Before starting: baseline snapshot (save the output)

```bash
{ date; free -m; swapon --show; docker stats --no-stream --format '{{.Name}} {{.MemUsage}}'; ls -l /mnt/ssd/docker/services/prefect/data/prefect.db; systemctl status user-1000.slice --no-pager | grep -E 'Memory|Tasks'; for c in server-prefect-worker-1 travelnet; do echo "$c locked/24h: $(docker logs --since 24h $c 2>&1 | grep -c 'database is locked')"; done; } | tee ~/baseline_$(date +%F).txt
```

Targets after the plan: swap < 500 MB, prefect.db < 500 MB, Prefect server RSS < 350 MB, ~70 flow runs/day instead of ~660, zero lock failures, every container reporting healthy.

### Step 1: Reclaim memory from dev sessions (30 min, no app risk) — impact: ~750 MB of swap

1. Close idle Claude Code / VS Code sessions on the Pi (several are 12 h+ old).
2. Add `/etc/systemd/system/user-1000.slice.d/limits.conf` (`MemoryHigh=900M`, `CPUWeight=50`, `IOWeight=50`), `systemctl daemon-reload`.
3. Verify: `systemctl status user-1000.slice` shows swap falling; `free -m` available memory up.
4. Rollback: delete the drop-in and reload.

### Step 2: Prefect DB cleanup, settings, limits (1–2 h, config only) — impact: ~0.5 GB RAM, ~80 % of SSD writes, faster scheduler

1. Back up first: stop `prefect-server.service`, `cp` `prefect.db*` to `/mnt/linux/…/backups/` (2.4 GB, fine).
2. Bulk-delete flow runs older than 14 days (script against the Prefect API in batches, or SQL delete in dependency order on the stopped DB, deleting `log`, `task_run_state`, `task_run`, `flow_run_state`, then `flow_run`). Prefer the API; use SQL only if the API is too slow for ~75 k runs, and test on the backup copy first.
3. `VACUUM` the DB while stopped (needs ~2.5 GB temp; plenty free).
4. Add to the unit: `PREFECT_SERVER_SERVICES_DB_VACUUM_ENABLED=events,flow_runs`, `PREFECT_SERVER_SERVICES_DB_VACUUM_RETENTION_PERIOD=1209600`, `PREFECT_SERVER_ANALYTICS_ENABLED=false`. Pin the image to `prefecthq/prefect:3.6.25-python3.12`.
5. Add `--init --pids-limit=256` now. Add `--memory=1g --memory-swap=1g` first; tighten to 768m only after one week of data.
6. Verify: `ls -l prefect.db`, server RSS in `docker stats`, `select count(*) from flow_run` shrinking daily afterwards; UI loads.
7. Rollback: stop the service, restore the DB copy, revert the unit.

### Step 3: Point containers at Prefect over the Docker network (15 min, config only) — impact: removes the Tailscale single point of failure

1. In `server/docker-compose.yml`, set `PREFECT_API_URL: "http://prefect-server:4200/api"` for `ingest` and `prefect-worker`. Leave `PREFECT_UI_API_URL` in the unit as the browser-facing URL.
2. `docker compose up -d` (recreates the two containers; wait for no flow run in progress).
3. Verify: worker logs show it registered; next `*/5` flow runs complete; flow-run links in logs now use the new host or still resolve.
4. Rollback: revert the two lines.

### Step 4: Fix the daily write-lock window and SQLite settings (code, ~half a day) — impact: removes failed polls and the risk of lost ingest writes

1. **Check first (read-only):** are the 98 API-side lock errors ingest inserts? Do clients retry? That decides how urgent this is.
2. Confirm the lock holder by watching 05:15 local: `Backfill Place` start/end vs the failed polls.
3. Change `Backfill Place` (then `Get Weather`, transition detectors, weekly analysis) to commit in batches and do reads/compute outside write transactions.
4. `database/connection.py::get_conn`: add `PRAGMA synchronous=NORMAL`, `PRAGMA busy_timeout=30000`, and a timeout on the read-only path; use `BEGIN IMMEDIATE` for read-modify-write (`upsert_aggregate`).
5. Add retries with backoff to ingest inserts (`BackgroundTasks`) so a transient lock doesn't drop data.
6. Run `pytest` (per repo CLAUDE.md; remember the `config.logging.configure_logging` patch for app tests). Add a test for the batched backfill and for a lock retry.
7. Verify: next two mornings have zero `database is locked` and zero failed `Get Power Statistics` runs.

### Step 5: Move `check-watchdog` and `poll-shelly` out of Prefect (code, ~half a day) — impact: removes ~576 of ~660 runs/day, the 100 MB subprocess spawn every 5 minutes, and ~85 % of Prefect DB growth

1. Implement as in-process periodic tasks in the FastAPI app (lifespan-managed asyncio tasks or a small scheduler) calling the existing logic; keep the notification behaviour and the "skip while uptime < 10 min" guard.
2. Remove both from `SCHEDULE_CONFIGS` / `FLOW_REGISTRY` only after the new path has run for a day alongside the old one.
3. Verify: `watchdog_heartbeat` and power rows keep arriving; staleness alert still fires when you pause the Watchdog Pi; Prefect runs/day drops to ~70.
4. Rollback: re-enable the schedules.

### Step 6: Health checks and monitoring (code + config, ~half a day) — impact: failures are detected and auto-recovered instead of silently hanging

1. Add `GET /health` (200, no DB) to the API; fix `Dashboard/app.py:1380` to check `resp.ok`.
2. Add compose `healthcheck:` to `ingest`, `dashboard`, `trevor`, `nginx`, `prefect-worker`, and a healthcheck on `prefect-server` (`/api/health`, already returns 200). Use Python one-liners (slim images have no `curl`).
3. `check_system_health.py`: restart on `unhealthy`; add `constellation`, `cloudflared`, `prefect-server.service`, `/mnt/ssd` mount check; replace the swap-% alert with swap-in rate; add per-container `memory.events` (`max`, `oom_kill`) deltas; add a backup-freshness check.
4. Consider `psi=1` in `cmdline.txt` and alert on memory PSI once available (needs a reboot; do it during a planned graceful reboot).
5. Verify: `docker ps` shows `(healthy)`; stop a container's app process and confirm it is restarted and an alert is sent.

### Step 7: Resource budget (config, 1 h) — impact: TravelNet protected under pressure, bad actors capped

Apply after Steps 1–2 and one week of sampler data:
- `mem_reservation`: travelnet 384m, prefect-server 384m, worker 192m, dashboard 96m, trevor 128m.
- Limits: dashboard 256m, trevor 384m, nginx 32m, worker/travelnet unchanged (adjust per sampler results), prefect-server 768m after cleanup.
- `pids_limit` 256 (128 for dashboard/nginx), `cpus` caps on worker (2.0), `init: true` on all.
- Verify: `docker stats`, `memory.events` counters flat, no OOM kills for a week.

### Step 8: Noise-flow index and schedule, profile long flows (1–2 h) — impact: less page-cache churn, clearer picture of real memory hogs

1. Add a partial index on `location_overland(horizontal_accuracy) WHERE horizontal_accuracy > 100` (or whatever `LOCATION_NOISE_ACCURACY_THRESHOLD` is); fix the "daily" vs hourly cron mismatch.
2. Read the 24 h and Sunday sampler results; for any flow whose `anon` approaches the limit, replace `fetchall()` loads with cursors/chunks.

### Step 9: Cleanup and hardening (ongoing, low risk each)

1. Remove the stale Tailscale cron jobs (cert renewal, `scp` to the Watchdog Pi, `docker restart travelnet-nginx`); replace `WOL_HOST`, `COMPUTE_HOST`, Ollama URLs and GymSwim's bind when Tailscale is retired.
2. Disable `bluetooth`, `ModemManager`, and (if unused) `wpa_supplicant`/`dhcpcd-wlan0`, `avahi-daemon`, `nmbd`/`winbind`.
3. *(Hardening items, tracked separately.)*
4. Dashboard: decide bind-mount vs baked image.
5. Separate Docker network for Constellation; move Prefect server into compose; make the reboot script wait for running flow runs.
6. ~~Fix the FX API key/quota and run a manual FX backfill (516-day gap).~~ **Retracted (§23): not a real problem.**

### Dependency notes

- Do Step 2 before putting a tight memory cap on Prefect, and Step 5 before judging how big the Prefect DB will stay.
- Do Step 4's data check before Step 5 so you know whether ingest is losing writes.
- Step 7 numbers depend on the Step 8 sampler data; keep the sampler running at least through Sunday 2026-10-11.
- Take a fresh DB backup before Steps 2 and 4, and note that the monthly reboot (next 1 Nov, 18:00 UTC = 05:00 local) orphans in-flight flow runs unless Step 9.5 is done.

---

## 11. Restore point taken 2026-10-07 (before any remediation)

Location: `<hdd>/backups/system-image/pi-server-2026-10-07_0017/` (HDD, ext4), created by `~/pi_backup.sh` (not in the repo).

| Part | Result |
|---|---|
| `sd.img.zst` | 3.7 GB compressed; `zstd -t` passed; decompressed size 31,914,983,424 bytes (= the 29.7 GiB SD card) |
| `sd.img.zst.sha256` | `1c0d310819822d7a3ba714b1db3b9a1015253f48f285fe0a4bcba3982407fe93` |
| `db/` | 3.2 GB: `VACUUM INTO` snapshots of `travel.db`, `prefect.db`, `constellation.db`; script completed with its `integrity_check` step (it prints `done` only if all passed) |
| `ssd/` | rsync copy of `/mnt/ssd` minus `vscode-server` and the three live DBs; `du` as a normal user reported 1.4 GB but could not read `docker-lib` and `containerd` (root-only), so the true size needed `sudo du`: **12 GB** (verified) |

Verified 2026-10-07: Pushcut completion notification received; image partition table readable (disk identifier `0xc7f884d8` matches the Pi's PARTUUID prefix; 512 MB FAT32 boot + 28.6 GB Linux root); backup directory set to mode 700. Restore point is good.
| `info.txt` | `lsblk`, `fdisk -l`, `fstab` at backup time |

Image is of a live, mounted root (crash-consistent). The backup directory is restricted to root (`chmod 700`).

Restore: `zstd -dc sd.img.zst | sudo dd of=/dev/mmcblk0 bs=4M status=progress` onto a card of at least 29.7 GiB, then restore `ssd/` and the `db/` snapshots onto the SSD with containers stopped.

---

## 12. Change log

### 2026-10-07 — Step 1 applied: `user-1000.slice` limits (by user, with sudo)

Created `/etc/systemd/system/user-1000.slice.d/limits.conf` (`MemoryHigh=900M`, `CPUWeight=50`, `IOWeight=50`), `daemon-reload`. Verified active: `MemoryHigh=943718400`.

State immediately after, **before closing any sessions** (the 7 `ccd-cli` processes are all still running; five are 13–15 h old):

| Metric | Before limit | After limit |
|---|---|---|
| `user-1000.slice` memory | 730 MB (peak 1.5 GB) | 1,020 MB, `high: 900M`, `available: 0B` (peak 1.8 GB) |
| `user-1000.slice` swap | 754 MB | 867 MB (peak 932 MB) |
| System swap used | 1.3 GB of 2 GB | **1,874 MB of 2,047 MB (91 %)** |
| Free / available RAM | 152 MB / 1.4 GB | 30 MB / 1.8 GB (1.86 GB cache) |

Interpretation: swap rose ~0.5 GB during the system-image backup (dd/zstd/rsync/`VACUUM INTO` pushed idle anonymous pages out to make room for cache) and does not fall by itself. With only ~170 MB of swap free, a burst of anonymous memory (a long flow, a rebuild) could trigger the OOM killer. The slice limit alone does not free swap; closing the idle sessions does (their ~870 MB of swapped pages are released when the processes exit). Do **not** run `swapoff -a` while RAM available is below the swap in use.

**Result after closing five idle sessions (PIDs 886762, 886778, 886779, 893755, 919728; `kill` with SIGTERM, all exited within 5 s):**

| Metric | Before closing | After closing |
|---|---|---|
| System swap used | 1,874 MB (91 %) | **1,508 MB (74 %)** |
| Free / available RAM | 30 MB / 1.8 GB | 505 MB / **2.08 GB** |
| `user-1000.slice` memory | 1,020 MB (over `high`) | 576 MB (`available: 324M` under `high`) |
| `user-1000.slice` swap | 867 MB | 504 MB |
| Tasks in slice | 104 | 62 |

Remaining swap (~1.0 GB outside the slice) is mostly cold pages from the containers that the backup pushed out; it will shrink naturally as Prefect is trimmed (Step 2). No `swapoff` needed. Step 1 complete.

---

## 13. Step 2 / Step 5 addenda (2026-10-07)

**Prefect DB state measured just before Step 2 (read-only):** 74,592 flow runs and 755,449 log rows are older than 14 days (of 83.7 k runs / 848 k logs), so retention will remove ~90 % of rows. About 55 runs are stuck in `Submitting` (all with no start time, spread over the whole history: orphans from restarts and reboots) plus one `Running` entry dating from **2026-06-08** (a zombie) and one `Backup DB` in `Submitting`. The vacuum service most likely only deletes terminal-state runs, so these need a manual delete afterwards.

**Draft for review:** `docs/drafts/prefect-server.service.proposed` (pinned local image tag `prefect-pinned:3.6.25`, `--memory=1g --memory-swap=1g --pids-limit=256`, analytics off, `DB_VACUUM_ENABLED=events,flow_runs`, 14-day retention, temporary batch 1000 / loop 120 s). The tag is created locally from the already-running image (no pull).

**Maintenance window rule:** `check_system_health.py` runs from root's crontab at :00/:15/:30/:45 and `docker start`s any expected container that isn't running (it includes `prefect-server`, whose container the unit removes on stop). Keep any stop-to-start gap inside one 15-minute gap between ticks, or disable that cron line for longer jobs (the final `VACUUM`). Ticks of the `*/5` flows missed during the gap come back as `Late` runs.

**Step 5 design (not started): keep the exact 5-minute cadence, drop the flow overhead.**
- `poll_shelly`: one HTTP POST to the plug, one read, one upsert. Replace with an asyncio periodic task started in the FastAPI `lifespan` (single uvicorn worker, so exactly one instance), aligned to wall-clock multiples of 5 minutes, running the blocking work via `asyncio.to_thread`. Fix the race while moving it: do the read-modify-write as one `BEGIN IMMEDIATE` transaction (or a single SQL upsert), instead of reading in one connection and writing in another.
- `check_watchdog`: same loop, same staleness logic and 10-minute uptime guard. Existing behaviour sends a notification every 5 minutes while stale; decide whether to keep that or de-duplicate.
- Replace the Prefect hooks: `on_failure` → after N consecutive failures call the same `send_notification`; `log_on_success` posts to the API's own `/internal/log/info` over HTTP every run (576 requests/day): a plain `logger.info` in-process is enough.
- Alternative if you prefer isolation: a tiny scheduler container (same image, `python -m periodic`), at ~40–60 MB extra RSS. In-API costs no extra memory.
- Run old and new side by side for a day (the upsert is idempotent per reading, but double-counts readings: disable the Prefect schedule when the new task goes live) before deleting the Prefect deployments.

---

## 14. Step 2 executed 2026-10-07 (Prefect cleanup, limits, pin)

Applied by the user at ~01:02–01:08 UTC: server stopped, DB + unit backed up to `<hdd>/backups/prefect-pre-step2/`, new unit installed (pinned local image `prefect-pinned:3.6.25` = `sha256:e350ab60…`, `--memory=1g --memory-swap=1g --pids-limit=256`, analytics off, `DB_VACUUM_ENABLED=events,flow_runs`, retention 1,209,600 s, temporary batch 1000 / loop 120 s), server started, worker restarted. Settings verified `(from env)`.

| Metric | Before | After (01:11 UTC) |
|---|---|---|
| `flow_run` rows | 83.7 k | **9,281** (plateau ≈ 14 days) |
| `log` rows | 848 k | **92.8 k** (cascade confirmed) |
| Prefect server memory | 609 MB RSS, no limit | 431 MB, capped at 1 GiB |
| System swap | 1,508 MB | 1,085 MB |
| `prefect.db` file | 2.4 GB | still 2.4 GB (**460 k of 626 k pages free**) + 915 MB WAL; needs `VACUUM` |

Observations:
- The bulk delete ran far faster than the configured batch (loop kept going), ~75 k runs in ~3 minutes, but the server's own SQLite was contended: ~220 `database is locked` lines in the server log, a `503` on the worker's deployment update at startup, and the worker container restarted twice via its restart policy before stabilising (restart count 2, stable). `*/5` flows resumed and complete normally (e.g. Check Watchdog / Get Power Statistics `Completed`). Lesson: a smaller batch or a stopped-server SQL delete would have avoided the contention; for the steady state (~650 runs/day) the default 200 per hour is ample.
- Remaining zombies (not removed by retention): 60 `Submitting` (oldest 2026-06-01), 1 `Running` (2026-06-08), 1 `Pending` (2026-07-30).
- Live data after cleanup is ≈ 680 MB (166 k pages), dominated by `events`/`event_resources` (61 k / 341 k rows, 7-day retention), which the vacuum service will keep trimming.

Next: delete the zombies via the API; then one short stop-window to remove the temporary BATCH_SIZE/LOOP_SECONDS lines, checkpoint the 915 MB WAL and `VACUUM` (expected file size ≈ 0.7 GB, and lower again once events age out).

**Completed 01:15–01:17 UTC:** 62 zombie runs deleted via the API (all 60 `Submitting`, the 2026-06-08 `Running`, the `Pending`). Server stopped, temporary `BATCH_SIZE`/`LOOP_SECONDS` lines removed from the unit (2 vacuum lines remain), `PRAGMA wal_checkpoint(TRUNCATE)` + `VACUUM` + `integrity_check` = `ok`. **`prefect.db`: 2.4 GB → 622 MB** (WAL gone). Server restarted: health `true` / HTTP 200; it needed more than the 20 s the command waited, so the chained worker restart did not run (worker still the instance from 01:08, restart count 2; it kept running flows through the outage but logged `NoEventLoopError` noise while the server was down). One transient `database is locked` on a deployment `last_polled` update right after startup, which is expected while catch-up runs flood in. Remaining: restart the worker for a clean reconnect.

---

## 15. Revisiting Step 5: do the `*/5` flows need to leave Prefect? (2026-10-07)

**Measured after the Step 2 cleanup:** server 334 MB, worker 129 MB, swap 1,062 MB, `prefect.db` 623 MB (WAL 5 MB), `*/5` flows completing normally.

**Verified facts (Prefect 3.6.25):**
- `serve()` runs every flow run in its own subprocess (`run_flow_in_subprocess` / `DirectSubprocessStarter` in `prefect/runner/runner.py`); there is no in-process option in the runner. Concurrency cap `PREFECT_RUNNER_PROCESS_LIMIT=5`.
- One `Get Power Statistics` run writes: 5 `flow_run_state` + 3 `task_run` + 9 `task_run_state` + 10 `log` + 14 `event_resources` rows ≈ **41 rows/run** (≈ 24 k rows/day for both flows).
- Per-subprocess memory ≈ 105 MB transient (135 → 345 MB with two concurrent runs). CPU cost not yet measured reliably (one 60 s sample = 146 ms, may not have contained a tick).
- `logging.yml` documents the override pattern `PREFECT_LOGGING_[PATH]_[TO]_[KEY]`, and the `api` handler has `level: 0`, so `PREFECT_LOGGING_HANDLERS_API_LEVEL=WARNING` should ship only WARNING+ to the server. **Untested**: logging is configured lazily, so a plain `python -c` check shows no handlers; it needs a real flow run to confirm. It applies to every flow in the worker (inherited by subprocesses).

**Cheap Prefect-side options (keep Prefect scheduling):**
1. Convert the inner `@task` functions of `poll_shelly_flow` / `check_watchdog_flow` to plain functions: removes the 3 task runs and 9 task states per run and most task log lines (≈ 41 → ≈ 15 rows/run).
2. Set `PREFECT_LOGGING_HANDLERS_API_LEVEL=WARNING` on the worker (after a test) so successful runs write no log rows; failures still log. Trade-off: INFO logs of all other flows disappear from the Prefect UI (the app's own logging remains).
3. Retention (already done) keeps the steady state at ~14 days of history (~9 k runs).

**Conclusion:** with retention in place, the DB-growth argument for moving these flows out is much weaker than I originally rated it. The remaining reasons are independence from Prefect (a Prefect outage silences the watchdog check and the power poll, as seen during the maintenance window), the Shelly read-modify-write race, and ~100 MB transient memory per run. Step 5 is demoted from "high impact" to "optional, after the cheap options above".

---

## 16. Code changes prepared 2026-10-07 (not yet deployed)

**CPU cost test (user, 10 min):** the worker container used **76,422 ms of CPU in 600 s ≈ 12.7 % of one core (3.2 % of the Pi)**, versus ~0.24 % when idle (146 ms / 60 s measured earlier). So the `*/5` flows (two ticks = four subprocess runs in that window, plus anything else that ran) cost on the order of 15–19 CPU-seconds per run, mostly process start-up and importing Prefect + the app, not the ~0.5 s of useful work. This is larger than my earlier estimate (~1 %) and strengthens the case for eventually taking these two flows off the subprocess path (Step 5); plain functions reduce DB rows but **not** this CPU cost.

**Prefect-side changes (per the user's instruction), all in `/mnt/ssd/services/TravelNet/server`:**
- `app/scheduled_tasks/poll_shelly.py`: three `@task`s → plain functions; `fetch_shelly_reading` now raises and the flow logs a WARNING and skips; happy-path log is DEBUG; `log_on_success` hook dropped (it POSTed an INFO line to the API log on every run, ~288 requests/day per flow); `on_failure=[notify_on_completion]` kept.
- `app/database/power/table.py`: new pure `merge_reading()` and `PowerDailyTable.upsert_reading()`, which does the read-modify-write in **one `BEGIN IMMEDIATE` transaction** (fixes the lost-update race and takes the write lock up front); `insert()` shares the same SQL constant.
- `app/scheduled_tasks/check_watchdog.py`: three `@task`s → plain functions; staleness logic extracted to the pure `evaluate_staleness()`; same 10-minute threshold and 10-minute uptime guard; happy path DEBUG; `log_on_success` dropped; failures and stale-watchdog alerts unchanged.
- Tests: `tests/test_power_poll.py` (10) and `tests/test_check_watchdog.py` (8), 18 passing. Mutation check: with `BEGIN IMMEDIATE` downgraded to `BEGIN`, `test_concurrent_readings_are_not_lost` fails, so the test does guard the race. Baseline before changes: 1,021 passed.
- Deployment note: the worker bind-mounts `./app`, but the flows are imported at start-up, so a **worker restart** is needed to pick up the new code. Not applied yet.

**Step 3 (internal Prefect URL), prepared, not applied:**
- `server/docker-compose.yml`: `ingest` and `prefect-worker` → `PREFECT_API_URL=http://prefect-server:4200/api`, plus `PREFECT_UI_URL=http://<pi-tailnet-host>:4200` so run links in logs stay browser-usable; `dashboard` gets both variables (it previously fell back to the hard-coded Tailscale default in `app.py`). `docker compose config` validates. Verified beforehand that `http://prefect-server:4200/api/health` returns `true` from the worker, API and dashboard containers.
- **Found while doing this:** `Dashboard/app.py` built the browser-facing `prefect_ui_url` (run link in the result modal) by stripping `/api` from `PREFECT_API_URL`. With the internal URL that would have produced `http://prefect-server:4200/...`, unusable in a browser. Fixed in `/mnt/ssd/services/Dashboard/app.py`: new `PREFECT_UI_URL` setting (default unchanged) used for that link. The dashboard repo's working tree was clean before this edit; it is a separate repo.
- **Still hard-coded, unaffected by Step 3:** `Dashboard/src/pages/Schedule.jsx:8` (`PREFECT_UI_URL = 'http://<pi-tailnet-host>:4200/deployments'`, the "Open Prefect" button; changing it needs a frontend rebuild) and `Dashboard/src/components/Layout.jsx:84` (Watchdog link on a `*.ts.net` host). Both belong to the Tailscale retirement list (§6).
- Applying Step 3 recreates `ingest`, `prefect-worker` and `dashboard`: the API (which receives phone uploads) is briefly down.

---

## 17. Step 3 + Prefect-side changes deployed 2026-10-07 ~01:40 UTC

`docker compose up -d ingest prefect-worker dashboard` run by the user. Verified:
- All three containers up; API `/docs` 200; worker `PREFECT_API_URL=http://prefect-server:4200/api`; dashboard has `PREFECT_API_URL` (internal) and `PREFECT_UI_URL` (Tailscale host, browser-facing). No errors or `database is locked` in the API or worker logs since the restart.
- `Check Watchdog` and `Get Power Statistics` complete on schedule with the new code. Per run: **0 task runs (was 3), 3 log rows (was 10), 5 flow states** (unchanged). The remaining 3 log rows are Prefect's own state messages; a worker-level `PREFECT_LOGGING_HANDLERS_API_LEVEL=WARNING` would remove them but is still untested.
- Power data intact: today's row had 23 readings by 01:52 UTC (no gaps through both Prefect stop windows, since `Late` runs executed on return); yesterday's row has 286 of 288 readings, consistent with the daily lock-window failures (§3.2a).
- Side effect: recreating the worker invalidated the cgroup path used by the memory sampler (it logged empty values from 01:4x). Restarted with a version that looks the container up on every sample.

**State after Steps 1–3 (01:52 UTC), against the original audit:**

| Metric | Audit start | Now |
|---|---|---|
| System swap used | 1.3 GB (66 %) peaking at 1.87 GB | **713 MB (35 %)** |
| Available RAM | 1.4 GB | **1.9 GB** |
| Prefect server | 609 MB RSS, no limit, 2.4 GB DB | 352 MB, 1 GiB cap, 622 MB DB |
| Prefect rows per `*/5` run | ~41 | ~8 (5 flow states + 3 logs) |
| Prefect flow runs retained | 83.7 k | 9.3 k (14-day retention) |
| Prefect dependency on Tailscale | yes | no (containers use `prefect-server`); browser links still on `*.ts.net` |

---

## 18. Step 4 investigation: the daily lock window explained (2026-10-07, read-only)

**API-side lock lines are not lost uploads.** All 91 `database is locked` lines in `server.log`/`server.log.1` (2026-08-19 → 10-07) are Prefect flow-failure hooks relayed to `internal.router`: 83 × *Get Power Statistics*, 8 × *Geocode Places*. None originate in upload handlers. (The 98 lines seen earlier in `docker logs travelnet` were wiped when the container was recreated, so they could not be reclassified; they are most likely the same relays printed more than once.) They occur every day at the same hour (19 UTC before a daylight-saving change, 18 UTC after), i.e. 05:15–05:30 local, and the Geocode failures sit at `:16–:17` past the hour like the others.

**Exposure of phone data is small:** only 0–3 GPS points per day arrive in the 18:10–18:45 UTC window (a quiet overnight period). Harm to date: ~3 missed Shelly readings/day (≈1 %) plus 8 Geocode failures over six weeks. No evidence of lost uploads, but not disproven for other upload types.

**Root cause: `scheduled_tasks/backfill_place.py::backfill_all_places`.**
1. The entire task runs inside one `with get_conn() as conn:`. Python's `sqlite3` opens a write transaction at the first `UPDATE` and only commits when the block exits, so the **write lock is held from the first update until the end of the run** (~15 min, 911 s average, 1,177 s max). Every other writer (Shelly upsert, Geocode, ingest) waits out its 30 s busy timeout and fails.
2. Each lookup (`_NEAREST_PLACE_SQL`) selects from the `location_unified` view and filters with `CAST(strftime('%s', timestamp) AS INTEGER) BETWEEN …`. SQLite materialises the whole view (388 k overland rows + 33 k shortcut rows with a correlated `NOT EXISTS`) and scans it with a non-indexable expression: **≈1.0 s per lookup**. With ~440 rows pending (health_quantity 221, photos 103, heart_rate 81, sleep 21, transactions 17, …) that is the 15 minutes.

**Measured alternative:** a range query on the base table (`timestamp BETWEEN :lo AND :hi`, ISO `…Z` strings, which both `location_overland` and `location_shortcuts` use and index) runs in **≈19 ms** (index `idx_overland_ts_latlon`/`idx_lshortcuts_timestamp`), ~50× faster.

**Proposed fix (not applied):**
1. Look up nearest places against the two base tables with sargable ISO ranges (bounds computed in Python), preserving the view's rule that a shortcuts point within ±3 min of an overland point is ignored, and the same "prefer most recent at-or-before, else earliest after" ordering. Expected runtime: seconds, not minutes.
2. Do the lookups on a read-only connection (no lock), collect `(id, place_id)` pairs, then write each table in one short `executemany` transaction. The write lock is held for milliseconds instead of minutes.
3. Add tests (none exist for this module): nearest-before preferred, fallback to after, window limit, shortcuts-dedup rule, and that rows with no nearby point stay NULL.
4. Same pattern applies to the other long writers (`Get Weather`, transition detectors, weekly analysis, `Geocode Places`) once profiled.

---

## 19. Step 4 implemented (not yet deployed): Backfill Place rewrite (2026-10-07)

**Files** (all under `server/`; uncommitted, alongside the unrelated CommBank work):
- `app/database/location/nearest_place.py` (new, stdlib only): `nearest_place_id(conn, ts, window_s)`. Fast path = indexed `timestamp BETWEEN :lo AND :hi` range queries on `location_overland` and `location_shortcuts`, unioned, same ordering as before (most recent at-or-before, else earliest after; nearest within that group), and the view's shortcuts-exclusion expression copied verbatim. Timestamps that are not canonical `YYYY-MM-DDTHH:MM:SSZ` fall back to the legacy view query, so behaviour is preserved for odd formats.
- `app/scheduled_tasks/backfill_place.py` (rewritten): table-driven (`_TARGETS`) with two phases. **Read phase** (read-only connection, no write lock) resolves a place for every unmatched row; **write phase** applies results with `executemany`, **500 rows per short transaction**. Result-dict keys unchanged. Each UPDATE additionally requires the column to still be NULL, so a row filled by another process between read and write is never overwritten.
- `tests/test_backfill_place.py` (new, 17 tests).

**Verification**
- Full suite: **1,056 passed** (1,039 before).
- Mutation check: removing the "prefer before" ordering fails both the semantic test and the old-vs-new equivalence test.
- Real-data equivalence (read-only on the live DB, niced): **598 lookups** (every currently pending row in all eight tables plus 20 random already-filled rows per table), **0 mismatches** between the legacy view query and the new one; 478 resolved a place. Legacy 1,153 ms per lookup vs new **3.3 ms (≈ 350× faster)**.
- Expected effect: the daily run drops from ~911 s (max 1,177 s) to a couple of seconds, and the write lock is held for milliseconds per chunk instead of ~15 minutes. This should end the daily 05:15–05:30 local lock failures of `Get Power Statistics` and `Geocode Places`.

**Side finding (not changed):** the `location_unified` view's de-duplication (`o.timestamp BETWEEN datetime(s.timestamp,'-3 minutes') AND datetime(s.timestamp,'+3 minutes')`) compares `…T…Z` strings with `YYYY-MM-DD HH:MM:SS` strings, so it never matches within the same UTC day. The only shortcuts rows it drops are the 116 (of 32,849) within ±3 minutes of UTC midnight (verified: 0 dropped rows outside 23:57–00:03). The intended "ignore shortcuts fixes within 3 min of an overland fix" rule is effectively not applied, which also affects the dashboard's location views. Fixing it would change displayed data, so it is left as is; `unified.py` separately deduplicates in Python.

**Deploy:** restart the worker (flows are imported at start-up). Optionally trigger one run of *Backfill Place* from the dashboard Schedule page to confirm the new runtime; it performs the same writes as the daily run.

**Deployed and confirmed 2026-10-07 02:12 UTC** (worker restarted 02:10, manual run `fresh-swan` via `prefect deployment run`): **Completed in 1.1 s** (previous runs: 739 s, 897 s, 1,020 s). It backfilled health_quantity 221/221, health_heart_rate 81/81, health_sleep 21/21, workouts 1/1, trigger_log 1/1, and all those columns now have no NULLs. `transactions` (17 found) and `photo_metadata` (103 found) matched 0: their oldest timestamps (2026-06-01 and 2026-04-15) predate any stored location fix, so there is nothing within the window; they stay NULL and are retried cheaply each day (~120 lookups × 3 ms). No worker errors or lock lines during the run. The next scheduled run (18:15 UTC) is the real test of the lock fix: expect no failed *Get Power Statistics* / *Geocode Places* runs in 18:10–18:45 UTC.

---

## 20. Step 6 drafted, not deployed: healthchecks and self-healing (2026-10-07)

**Why:** `restart: unless-stopped` only helps when a process *exits*. A hung API, a wedged Prefect runner or a dead nginx stays "running" forever, and the old health script only checked "is it in `docker ps`". The new design: containers report health, the host script restarts the unhealthy ones (rate-limited), and the dashboard stops reporting a missing endpoint as healthy.

**Application code (in the working tree; live only after the containers restart):**
- `server/app/main.py`: `GET /health` (async, no DB, no auth, hidden from OpenAPI). Verified blocked on `api.travelnet.dev` / `public.travelnet.dev` by the allow-list middleware, so it is local-only. Tests: `tests/test_health_endpoint.py` (5).
- `Dashboard/app.py`: explicit unauthenticated `GET /healthz` (the SPA catch-all returns 200 for any path, so probing `/health` proved nothing); `/api/fastapi-health` now returns 503 when the API answers non-2xx. Exercised in a throwaway interpreter inside the dashboard container: `/healthz` → 200; `/api/fastapi-health` → 503 `{"code":404,"status":"error"}` against today's API (it used to say "ok").

**Healthchecks (compose/nginx edits in the working tree, `docker compose config` valid for all three projects):**

| Container | Probe | Notes |
|---|---|---|
| `travelnet` (API) | `GET 127.0.0.1:8000/health` | 30 s interval, 3 retries, 90 s start period (`init_db` on a ~1 GB DB) |
| `travelnet-dashboard` | `GET 127.0.0.1:5000/healthz` | now `depends_on: ingest: service_healthy` |
| `travelnet-nginx` | `wget /nginx-health` on :80 | new `location = /nginx-health` answered by nginx itself; `nginx -t` passed inside the running container |
| `server-prefect-worker-1` | `GET localhost:8080/health` | needs `PREFECT_RUNNER_SERVER_ENABLE=true` (added); Prefect's runner returns 503 if it has not polled for 20 s (2 × 10 s). **Not yet exercised live**; `depends_on: ingest: service_healthy`; 120 s start period |
| `trevor` | `GET 127.0.0.1:8300/docs` | not `/health`, which opens a Chroma client per call |
| `constellation` | any HTTP response < 500 on `/` | it answers 401 behind Cloudflare Access (checked live) |
| `prefect-server` | `GET 127.0.0.1:4200/api/health` | in the systemd unit (`docs/drafts/prefect-server.service.proposed`; differs from the installed unit by two `--health-*` lines only). `systemd-analyze verify` clean; verified systemd passes `--health-cmd` as one argument; the command exits 0 in the live container |

**Host script (`docs/drafts/check_system_health.py.proposed`; the live script is untouched because root's cron runs it straight from the repo path):**
- New checks: required mounts (`/mnt/ssd`, `/mnt/linux`); Docker health status; restart-count increase since last run; per-container cgroup `oom_kill` increase; systemd units (`docker`, `cloudflared`, `prefect-server`); DB-backup age (< 36 h); `prefect.db` size (≥ 1.5 GB); swap-in rate from `/proc/vmstat` (≥ 100 pages/s between runs); `constellation` added to the expected containers.
- Self-healing: `unhealthy` → `docker restart` (or `systemctl restart prefect-server.service`), at most **3 restarts per 6 h** per container/unit so a crash loop cannot restart forever; down `cloudflared` / `prefect-server` units are restarted under the same budget; `docker` itself is never restarted automatically.
- Noise fix: the "swap > 60 %" alert (permanently tripped with zram) is replaced by "swap > 90 %" (critical) plus the swap-in rate, which measures actual thrashing.
- Bugs fixed on the way: `check_containers` used substring matching (`travelnet` counted as running if only `travelnet-nginx` was), and `mitigate_container` could `docker start` the wrong container; both now match exact names. `docker start prefect-server` could never work (the unit removes the container on stop): the cron ran it during the Step 2 stop window (cooldown file `alert_container_prefect-server` at 01:15:07 UTC) and it now goes through systemd.
- State now persists in `/var/tmp/travelnet_health_state.json` (survives the monthly reboot, unlike the `/tmp` cooldown files).
- `--dry-run`: runs every check, prints alerts, and sends nothing, restarts nothing, writes no state or cooldown files. Dry-run on the live system: all mounts/containers/services OK, backups 12.3 h old, `prefect.db` 0.66 GB, **0 alerts**, no side effects.
- Tests: `tests/test_health_script.py` (26; loads the live script once it has the new checks, otherwise the draft). Mutation checks (budget off-by-one, substring matching, OOM detection disabled) each fail a test.

**Verification:** full suite **1,087 passed** (1,056 before).

**Deployment order matters:**
1. *Healthchecks first, script second.* Until the new script is installed nothing acts on health, so a probe that turns out wrong (the worker's is the untested one) only shows `unhealthy` in `docker ps` and cannot cause restarts.
2. Restart `prefect-server` with the new unit *before* recreating the worker (the worker's runner health depends on the server answering).
3. Recreating `travelnet` takes the API (and phone uploads) down for roughly 30–90 s; do it in a quiet gap between health-cron ticks (:00/:15/:30/:45 UTC), because the *old* script `docker start`s anything it finds down.
4. Watch every container reach `(healthy)`, and the worker survive a few `*/5` ticks and the top of the hour, then install the new host script.
5. Rollbacks: `git checkout` the compose/nginx files and re-run `docker compose up -d`; restore `/etc/systemd/system/prefect-server.service` from `<hdd>/backups/prefect-pre-step2/prefect-server.service.orig` (or the Step 2 version) plus `daemon-reload`/restart; `git checkout scripts/check_system_health.py`.

---

## 21. Step 6 phases 0–4 deployed 2026-10-07 02:30–02:41 UTC

Run by the user, in this order: saved the installed unit (`prefect-server.service.step2`), installed the new unit and restarted `prefect-server` (healthy after ~40 s), `docker compose up -d` in TravelNet (API healthy after 28.9 s; dashboard, worker, nginx recreated), then Trevor and Constellation.

**Result: all 7 containers `healthy`, 0 restarts**, including the one untested probe (the Prefect worker's runner endpoint returns `{"message":"OK"}`). Endpoints verified from the host: API `/health` `{"status":"ok"}`, dashboard `/healthz` `ok`, nginx `/nginx-health` `ok`. After 5 minutes `Check Watchdog` and `Get Power Statistics` were `Completed` with 0 worker errors/lock lines. The 02:30:03 health-cron tick ran before the window opened (0 alerts), so the old script never saw a container down. Memory afterwards: swap 576 MB (down from 1.87 GB peak), 1.79 GB available.

API downtime for the recreate was under ~30 s. Pending: ~1 h observation, then phase 5 (install the new host script).

---

## 22. Noise flow and reboot script (2026-10-07, in the working tree; API + worker restart needed for the first)

### 22.1 Identify Location Noise: daily, and indexed

- **Schedule:** `config/schedules.py` `identify-location-noise` changed from `0 * * * *` (hourly, although its description said "Daily") to **`0 4 * * *`** (04:00 local; before the 04:30 geocode and 08:00 daily summary, away from the 03:15 retro-scan, unique cron slot). Safe because ingest flags noise in real time (`insert_payload` → `pending_noise`); this flow is only the retroactive catch-up. Removes 23 runs/day. Takes effect when the worker restarts and re-registers its deployments.
- **Index:** `idx_overland_accuracy` on `location_overland(horizontal_accuracy)`, created by `OverlandTable.init()` (`IF NOT EXISTS`, so it is built on the next API start). A *partial* index would not work: the threshold is an editable setting passed as a bound parameter, which SQLite cannot match to a partial-index `WHERE`. Measured on a scratch copy of the live DB (388,928 rows, 2,341 with accuracy > 100): tier-1 query **4,564 ms → 3 ms** (plan changes from `SCAN o` to a covering-index `SEARCH`); index build 1.8 s. Tier 2 was already cheap (185 ms for 4,623 rows).
- **Side note (not changed):** tier 2's watermark `datetime(MAX(ts),'-5 minutes')` has the same `T…Z` vs `YYYY-MM-DD HH:MM:SS` string-comparison quirk as the `location_unified` view, so it effectively restarts from 00:00 UTC of the watermark's day. Harmless (the `NOT EXISTS location_noise` guard prevents duplicates) and cheap.
- **Tests:** `tests/test_location_noise_schedule_and_index.py` (7): daily schedule, unique slot, index exists, `init()` idempotent, plan uses the index for different thresholds, query results unchanged (strictly greater, NULL never matches). Removing the index fails 3 of them.

### 22.2 graceful_reboot.sh waits for in-flight flow runs

- **Problem:** the Oct 1 reboot orphaned flow runs (they sat in `Submitting` for 5 days). The script went straight to `docker compose stop`.
- **Change (`server/scripts/graceful_reboot.sh`, live immediately; next use is the 1 Nov cron or a watchdog call):** after the notification it polls Prefect (`POST /flow_runs/count` for `RUNNING`/`PENDING`, which includes `Submitting`) every 10 s, up to 300 s, then proceeds regardless. It never blocks the reboot: Prefect unreachable or a non-numeric answer → proceeds at once; timeout → logs "Gave up" and proceeds. Reason **`watchdog` never waits** (the Watchdog Pi calls it over SSH with a 60 s timeout, and that path means the system is already unhealthy). Added UTC timestamps to the log lines. Verified against the live API: the same request returns `0` now.
- **Testability:** paths and delays are overridable by environment variables (`TRAVELNET_ENV_FILE`, `TRAVELNET_COMPOSE_DIR`, `PREFECT_LOCAL_API`, `FLOW_WAIT_MAX_S`, `FLOW_WAIT_POLL_S`, `REBOOT_DELAY_S`); defaults are the production values, so behaviour is unchanged otherwise.
- **Tests:** `tests/test_graceful_reboot.py` (14) run the real script end to end with stub `curl`/`docker`/`sudo` first on `PATH`; each test first asserts those commands resolve to the stubs, so the real reboot is unreachable (the Pi's uptime was checked unchanged afterwards). They cover event order (notify → count… → stop → maintenance → reboot), waiting, immediate proceed, give-up, Prefect down, garbage response, the watchdog path, notification text per reason, working directory, and a missing `.env`. Three mutations (never waits, watchdog also waits, Prefect-down aborts the reboot) each fail tests. One test was timing-flaky in the first full run (bash `$SECONDS` has 1 s granularity); fixed, then 8/8 repeats and a run under CPU load pass.
- **Not changed:** `prefect-server` is still stopped by systemd at shutdown rather than by this script (stopping it from here would race its `Restart=always`). With the worker idle first this is clean.
- **Side finding:** `update_timezone.py` writes the reboot cron as "04:00 local converted to UTC" (`0 18 1 * *` = 04:00 local when it was written). Plain cron keeps the UTC time, so after a daylight-saving change the reboot happens at **05:00 local**, and it will shift again at the next DST change until `update_reboot_cron` is re-run. Prefect schedules are timezone-aware and follow DST; only these system-cron jobs drift.

**Verification:** full suite **1,108 passed** (1,087 before).

**To deploy 22.1:** restart the API (builds the index, ~2 s) and the worker (re-registers the daily schedule): `docker restart travelnet server-prefect-worker-1`. Expected afterwards: `identify-location-noise` has one scheduled run per day at 17:00 UTC.

**Deployed and confirmed ~03:05 UTC:** `docker restart travelnet server-prefect-worker-1`. `idx_overland_accuracy` exists in the live DB; `Identify Location Noise` now has one scheduled run per day at 17:00 UTC (04:00 local) for the next three days; both containers `healthy`.

---

## 23. Correction: the "Backfill FX failures" were test runs; pytest pollutes production Prefect (2026-10-07)

**Retraction.** §3.2a / §10 / the summary claimed the FX backfill was failing (invalid key, exhausted quota, 516-day gap) and that FX data might be missing. That was wrong. Evidence:
- The error messages (`No API quota remaining`, `invalid_access_key … Invalid key`, `Date range exceeds 365 day API limit (516 days)`) are the scenarios in `tests/test_get_fx_up_to_date.py`; the bursts of 7 runs within 8 s (02:52 and 02:58 UTC today, and 05:09 UTC on Oct 1) coincide with `pytest` runs, and those runs have `auto_scheduled = 0` and no deployment.
- Real data: the daily *Get FX* and *Backfill GBP* deployment runs complete every day (latest 2026-10-06), and `fx_rates` holds 126 consecutive days (2026-06-01 → 2026-10-04) for all 15 currencies. `get_fx_flow` deliberately fetches `now − 2 days`, so a newest date of Oct 4 on Oct 6 is by design. `api_usage` shows 7 `exchangerate.host` calls for September.
I should have checked the real data before reporting it; I inferred from run states alone.

**Real finding: running the test suite on the Pi writes into the production Prefect server.**
- `~/.prefect/profiles.toml` (profile `ephemeral`) sets `PREFECT_API_URL=http://<pi-tailnet-host>:4200/api`. Any test that invokes a flow object through the engine therefore creates real flow runs, task runs, states and logs in production Prefect.
- Only two test modules do this: `test_get_fx_up_to_date.py` (7 calls) and `test_backfill_gbp.py` (5). Footprint in the production DB: **49 *Backfill FX* and 35 *Backfill GBP* runs** with no deployment (since 2026-10-01 05:08 UTC), several of them `Failed`. Most are mine from this audit (the suite was run ~8 times today). They show up in the Prefect UI and in any failed-run statistic. The other non-deployment runs in the DB are legitimate sub-flows (Compute Daily Summary ×3, Detect *, Geocode Places) and one manual *Identify Location Noise* run.
- It also ties the test suite to Tailscale: when that hostname goes away, these 12 tests will start failing or retrying (`fetch_fx_timeframe` has `retries=3, retry_delay_seconds=10`).
- The notification side is safe: `conftest.py` patches `send_notification`, and the failure hook's HTTP call goes to `ingest:8000`, which does not resolve from the host.

**Options evaluated:**
1. Isolate with an ephemeral Prefect for tests (`PREFECT_API_URL=""`, throwaway `PREFECT_HOME`, longer startup timeout). Verified it keeps production untouched, but on this Pi a trivial flow takes **59 s cold / 40 s warm** and emits server errors: too slow and heavy for the production machine.
2. Convert the two modules to call `flow.fn()` with the tasks and run-logger patched (the style `test_flag_location_noise.py` already uses). Fast and engine-free, but the 12 tests currently assert through the engine (retries, `caplog`), so it is a real rewrite (~1–2 h).
3. Leave the tests and let Prefect retention clear the runs (14 days); stop running these two modules on the production Pi.

**Interim practice:** during this work, avoid running the full suite on the Pi; run targeted modules and exclude the two above (`--ignore=server/tests/test_get_fx_up_to_date.py --ignore=server/tests/test_backfill_gbp.py`), or run the full suite only when needed. Cleaning up the 84 existing test runs (delete via the Prefect API, as was done for the zombies) is optional and needs the owner's go-ahead.

**Side observation (same period):** swap rose from 576 MB (02:40) to 1.5 GB (03:04). The containers hold only ~49 MB of it; `user-1000.slice` holds 1.2 GB (the slice limit pushed idle pages of my test and scratch-copy work into swap, the intended "dev gives way to TravelNet" behaviour). Swap-in is ~0 pages/s (no thrashing); swap free is 530 MB. It does not shrink by itself.

### 23.1 Resolved 2026-10-07: tests no longer touch production Prefect; test runs deleted

- **Guard (`/mnt/ssd/services/TravelNet/conftest.py`):** at import time (before anything imports prefect) sets `PREFECT_API_URL=http://127.0.0.1:9/api`, `PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=false` and `PREFECT_LOGGING_TO_API_ENABLED=false`. Environment variables beat the profile, so any test that uses the Prefect engine now fails fast with a connection error instead of writing to production. Proven against the old tests: the first one failed in 3 s and production's row counts did not move.
- **Conversion (no production code changed):** `tests/test_get_fx_up_to_date.py` (23 tests incl. the GBP file) and `tests/test_backfill_gbp.py` now call `.fn()`, replace `get_run_logger` with a standard logger (so `caplog` assertions still work), swap the flow's task objects for their plain functions, and mock `record_flow_result`. Assertions that only existed because the *engine* logged failures became assertions on the raised exception text. The engine behaviour no longer exercised is asserted as configuration: the API task's `retries=3, retry_delay_seconds=10`, and both flows' `on_failure=[notify_on_completion]` / `on_completion=[log_on_success]` hooks and names. Added tests for unknown quota, empty quotes response, the exact requested date range, the 14-day default target date, empty results being recorded, and the flow recording the task's result. Three mutations (365-day guard removed, GBP conversion inverted, retry policy dropped) each fail a test; production flow files verified unchanged by `git diff`.
- **Cleanup in production Prefect (via the API, IDs saved to the session scratchpad log):** deleted **84 flow runs** (49 *Backfill FX* + 35 *Backfill GBP*, all `deployment_id IS NULL`, `auto_scheduled = 0`) and **42 orphan task runs** (`get_missing_fx_dates` with no flow run). Left in place: the real deployment runs (FX 4, GBP 17) and two June 2026 orphan task runs (`fetch_fx_timeframe`, `store_fx_and_backup`) that look like a manual backfill, not tests. Zero failures; post-delete counts: FX/GBP non-deployment runs 0, orphan task runs 2.
- **Result:** full suite **1,116 passed in 60 s** (was 135 s, since engine round-trips and retry delays are gone); the full run created no new runs in production.
- **Tailscale dependency removed:** the 12 tests no longer depend on the `*.ts.net` Prefect hostname in the user's profile. (The profile itself still points at it and should be updated when Tailscale is retired.)

---

## 24. Idle dev-session reaper (2026-10-07; script and unit drafts ready, timer not yet installed)

**Problem:** dev sessions (Claude Code processes started from the desktop app) linger for 13+ hours after use, holding RAM on the production Pi (about 1.5 GB resident+swap at the worst point). Their transcripts are on disk, so a stopped session can be resumed.

**Policy (user's choice):** option 1, a timer-driven reaper; idle threshold **6 hours** (first set to 3; raised after the reaper stopped a conversation that was only waiting on the user); send a custom Pushcut notification when it identifies and stops a session.

**Files:** `scripts/reap_idle_dev_sessions.py` (stdlib only), `docs/drafts/dev-session-reaper.service` / `.timer` (systemd *user* units, no sudo; `systemd-analyze --user verify` clean), `server/tests/test_reap_idle_dev_sessions.py` (27 tests).

**What counts as a session:** a process owned by the user whose **program path** (`argv[0]`) contains `/.claude/remote/ccd-cli/`. Matching only `argv[0]` matters: a first draft matched the path anywhere in the command line, and a measurement script's own shell wrapper showed up as a "session". Nested matches are folded into the top-level one.

**Idle detection (measured, not guessed):**
- Measured CPU of an idle session: ~0.8 % of a core (+0.99 s in 120 s); this conversation, mostly waiting, ~2 %. CPU alone cannot separate idle from lightly active, so the primary signal is the **transcript**: the newest `~/.claude/projects/<cwd with non-alphanumerics replaced by "-">/*.jsonl` write. Verified on the live system: the Constellation session's last write was 00:53 UTC and the reaper reported it 2h41m idle at 03:35.
- Secondary signal: the session's whole process tree used CPU above 5 % of a core between two runs (e.g. a long test run), which keeps it alive.
- A session must be observed on **two runs** before it can be stopped (the CPU rule needs a baseline), so the earliest stop is ~30 min after the first observation.

**Safety:** never touches the process chain that launched it; PID reuse guarded by process start time (a mutation removing this was first missed by my tests, which exposed a weak test; a direct test now catches it); at most 5 sessions per run; SIGTERM to the whole tree, SIGKILL only for survivors after 30 s; one notification per run listing each session (working directory, idle time, age, RAM and swap freed); a failed notification is logged and never undoes the stop; `--dry-run` stops nothing and reports each would-be stop once per session.

**Verification:** 27 tests, including a fake-`/proc` suite (first sight, thresholds, CPU/transcript/child-process activity, protection, PID reuse, max-reaped, notification content, state pruning) and an end-to-end test that stops **real** throwaway processes (a session plus its child, while an unrelated `sleep` survives). Six mutations (ignore caller chain, match anywhere in argv, stop on first sight, ignore CPU, never SIGKILL, ignore PID reuse) each fail a test. Full suite **1,143 passed** in 66 s. Live `--dry-run` on the Pi identified exactly the two real sessions and touched nothing.

**Notification:** uses `CUSTOM_NOTIFICATION_NOT_TIME_SENSITIVE` from the TravelNet `.env` (an idle-session stop is not urgent); `--notify-var` selects another (e.g. `CUSTOM_NOTIFICATION_TIME_SENSITIVE`).

**Rollout:** the service unit ships with `--dry-run`, so the first days only report (once per session) what it would stop. Going live is deleting that flag. The user timer runs while the user has a login session (`Linger=no`), which is exactly when dev sessions exist.

**Installed 2026-10-07 ~03:37 UTC by the user and switched live immediately** (`--dry-run` removed from the user unit; timer every 30 min). The first live run's log shows both sessions seen twice; the Constellation session (idle since its last transcript write at 00:53 UTC) becomes eligible at 03:53 UTC and will be stopped on the next run after that (~04:07 UTC), with one Pushcut.

**Live behaviour (2026-10-07):** the first live run stopped the idle Constellation session at 04:07 UTC (SIGTERM was enough; the process was gone and a Pushcut was sent). Three hours later it also stopped the conversation it was being developed in, after 3 h without a transcript write while the user was away; the app resumed it from its transcript on the next message. That is why the window was raised to 6 hours.
