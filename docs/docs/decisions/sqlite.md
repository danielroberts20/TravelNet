---
layout: doc
title: Why SQLite
section: Decision Log
description: "Why TravelNet uses SQLite rather than PostgreSQL, MySQL, or a cloud database."
permalink: /docs/decisions/sqlite/
prev_page:
  url: /docs/decisions/
  title: All Decisions
  section: Decision Log
next_page:
  url: /docs/decisions/location-sources/
  title: Why Overland + Shortcuts
  section: Decision Log
---

<div class="callout callout-decision">
  <span class="callout-icon">📋</span>
  <div><strong>Decision:</strong> Use SQLite as the sole database for all TravelNet data.<br>
  <strong>Status:</strong> Settled — in use since project start.<br>
  <strong>Revisit if:</strong> Data volume exceeds ~10GB, or concurrent write requirements change significantly.</div>
</div>

## The alternatives considered

| Option | Pros | Cons |
|--------|------|------|
| **SQLite** | Single file, zero config, trivially backed up | No concurrent writes, no network access |
| **PostgreSQL** | Full ACID, concurrent access, rich features | Needs a running server, complex backup |
| **MySQL/MariaDB** | Widely used, good tooling | Same operational overhead as Postgres |
| **Cloud DB (e.g. PlanetScale)** | Managed, no ops | Requires internet, costs money, privacy concern |

## Why SQLite won

**Scale doesn't justify the overhead.** TravelNet collects data from one person. At the highest expected rate — Overland GPS at 5s intervals — that's ~17,000 points per day, or ~6M points per year. SQLite handles hundreds of millions of rows without complaint.

**The backup story is unbeatable.** The entire database is a single file at a known path. Backing it up is `cp travel.db travel.db.bak`. The offsite backup cron does exactly this, age-encrypts it, and pushes it to Cloudflare R2. Restoring is equally trivial.

**No server to manage.** Running PostgreSQL on a Raspberry Pi means managing another process, another set of logs, another failure mode. SQLite is a library — it lives inside the FastAPI process and disappears when the process does.

**Write concurrency isn't a real problem here.** The only writer is the FastAPI ingest container. Uploads happen in `BackgroundTasks` which serialise writes naturally. The `get_conn()` utility uses a 10-second timeout on write connections to handle any lock contention.

<div class="callout callout-note">
  <span class="callout-icon">ℹ️</span>
  <div>The dashboard (Flask) is read-only against the DB. It opens short-lived connections and closes them immediately — no lock contention with the ingest container.</div>
</div>

## The one real limitation

SQLite doesn't support concurrent writes from multiple processes. This matters for the cron scripts — each runs as a separate `docker exec` process.

The solution: `get_conn()` includes `timeout=10` on all write connections, and the `DailyDigestHandler` uses a queue-draining daemon thread rather than writing directly from the logging call. This avoids `database is locked` errors in practice.

<div class="callout callout-warning">
  <span class="callout-icon">⚠️</span>
  <div>Never hold a SQLite connection open across multiple requests or long-running operations. Open, write, close — every time. See <code>database/util.py</code> for the <code>get_conn()</code> pattern.</div>
</div>

## Would I change this?

No. For a personal data platform on a Raspberry Pi, SQLite is the correct choice. The operational simplicity it provides — especially for backup, restore, and recovery on the road — outweighs any theoretical benefits of a server-based database at this scale.
