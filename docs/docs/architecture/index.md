---
layout: doc
title: System Overview
section: Architecture
description: "How TravelNet's components fit together — containers, storage, networking, and data flow."
permalink: /docs/architecture/
next_page:
  url: /docs/architecture/docker/
  title: Docker & Containers
  section: Architecture
---

TravelNet runs as three Docker containers on a Raspberry Pi 4B, collecting data 24/7 throughout a 2–3 year trip. The design prioritises **reliability over complexity** — everything is chosen to survive reboots, network drops, and months of unattended operation.

## High-level diagram

```
iOS Devices                    Raspberry Pi 4B
──────────────                 ─────────────────────────────────────
Overland (GPS)   ──────────►  nginx (443)
iOS Shortcuts    ──────────►    └─► ingest (FastAPI :8000)
Health Auto Export ────────►          └─► SQLite (external HDD)
Revolut / Wise   ──────────►
                               dashboard (Flask :8080)  ← Tailscale only
                                 └─► SQLite (read)

                               Cloudflare Tunnel
                                 └─► /public/stats  ← public internet
```

## The three containers

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `travelnet` | FastAPI (custom) | 8000 | Data ingest — all upload endpoints |
| `dashboard` | Flask (custom) | 8080 (internal) | Admin UI — view DB, logs, config |
| `nginx` | nginx:alpine | 80, 443 | TLS termination, reverse proxy |

All three are defined in `server/docker-compose.yml` and managed together. They share a Docker network (`travelnet`) so the dashboard can reach the ingest container internally.

## Storage

A single SQLite file lives on an external HDD mounted at `/mnt/linux/docker/services/travelnet/data/`. The HDD is volume-mounted into the `travelnet` container at `/data`.

<div class="callout callout-decision">
  <span class="callout-icon">📋</span>
  <div><strong>Why SQLite?</strong> See the <a href="/docs/decisions/sqlite/">Decision Log: Why SQLite</a> for the full reasoning. Short answer: it's a single file, trivially backed up, and more than sufficient for one person's data at this scale.</div>
</div>

## Networking

- **Public ingest** — nginx handles TLS (cert via Tailscale) on port 443, proxies to FastAPI on 8000
- **Private dashboard** — only reachable via Tailscale at `pi-server.tail186ff8.ts.net`
- **Public stats endpoint** — Cloudflare Tunnel exposes only `GET /public/stats` at `api.travelnet.dev`

## Remote access

Tailscale provides the VPN layer. The Pi is always reachable at `pi-server.tail186ff8.ts.net` as long as it has internet — even across hotel WiFi, mobile data, or NAT.

## Reliability features

- All containers have `restart: unless-stopped`
- Cloudflare Tunnel runs as a `systemd` service, starts on boot
- Offsite backup to Cloudflare R2 runs on the 2nd of each month (age-encrypted, rclone)
- Email alerts fire on any cron failure via `CronJobMailer`

<div class="callout callout-note">
  <span class="callout-icon">ℹ️</span>
  <div>The dashboard container is intentionally kept separate from the ingest container. This means a crash or restart of the admin UI never affects data collection.</div>
</div>
