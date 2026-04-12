# TravelNet &mdash; Cost of Time & Space

> *A personal data platform built across 2–3 years of travel through the USA, Australia, New Zealand, South-East Asia, and Canada.*

![Tests](https://github.com/danielroberts20/TravelNet/actions/workflows/tests.yml/badge.svg)
[![Licence: CC BY-NC 4.0](https://img.shields.io/badge/Licence-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)

## 🌟 Highlights

- **End-to-end personal data pipeline** — transactions, GPS traces, health metrics, and weather, all collected passively in real time during travel
- **Multi-source, conflict-aware ingestion** — Revolut, Wise, cash, and local bank accounts reconciled into a single unified ledger
- **Continuous location tracking** — dual-source GPS (Overland + iOS Shortcuts) with automatic gap-filling and ~99.4% coverage validated on a pre-departure dry run
- **Orchestrated scheduling** — self-hosted [Prefect](https://www.prefect.io) server manages all scheduled tasks with a live dashboard, structured logging, and success/failure notifications
- **ML-powered insights** — travel segmentation, spend forecasting, location clustering, and anomaly detection derived from live data
- **Public demo dataset** — anonymised and coordinate-fuzzed subset available for exploration via the [interactive dashboard](https://travelnet.dev)

---

## ℹ️ Overview

TravelNet is a self-hosted data collection, analysis, and visualisation system designed to run unattended on a Raspberry Pi throughout 2–3 years of international travel. Every transaction I make, every location I visit, and every health metric I track is ingested, enriched, and stored — building a dataset that grows richer the longer I travel.

The project has two goals. The first is personal: to build an honest, granular record of what long-term travel actually costs — in money, time, distance, and routine. The second is technical: to serve as a real-world ML project with genuinely messy, self-collected data, deployed in constrained conditions from day one.

The backend is a FastAPI service running in Docker on a Raspberry Pi, with SQLite on an external HDD and Tailscale for remote access. Data is collected passively via Prefect-orchestrated scheduled flows and lightweight iOS integrations. A companion Flask dashboard provides a live admin interface for monitoring, DB management, and manual uploads — accessible only over Tailscale.

Check out the [interactive dashboard](https://travelnet.dev) to explore the demo dataset.

---

## 🗺️ Data Sources

| Domain | Sources |
|--------|---------|
| **Transactions** | Revolut (CSV), Wise (multi-currency ZIP), Cash (manual endpoint), local bank (on arrival) |
| **Location** | Overland (continuous GPS, background), iOS Shortcuts (5-min interval gap-fill) |
| **Health** | Health Auto Export — step count, heart rate, sleep stages, workouts, mood/state of mind |
| **FX Rates** | exchangerate.host (automated, bi-weekly via Prefect) |
| **Weather** | Open-Meteo (retroactive enrichment by coordinates + date) |

---

## 🏗️ Architecture

```
                 📱 iPhone
                /          \
    Overland &         Revolut/Wise
    Health Auto        CSV uploads
    Export (4-hourly)        \
               \              ▼
                ▼    ┌───────────────────────────────────────────┐
                ──►  │           Raspberry Pi (Docker)           │
                     │                                           │
                     │  FastAPI  ──►  SQLite (ext. HDD)          │
                     │     ▲               │                     │
                     │  Prefect       Backfill &                 │
                     │  Scheduler     FX enrichment              │
                     │     │                                     │
                     │  Flask Dashboard (Tailscale-only)         │
                     └─────┬────────────────┴────────────────────┘
                           │ Tailscale (remote access)
                           ▼
                     Laptop (local ML / dashboard dev)
                           │
                           ▼
                     travelnet.dev (Cloudflare Pages)
```

Data flows from iOS apps and manual uploads into FastAPI ingest endpoints, is enriched with FX rates and weather retroactively by Prefect-scheduled flows, and is periodically snapshotted for ML work and the public dashboard.

**Key infrastructure decisions:**
- **SQLite over PostgreSQL** — sufficient for write-once telemetry at this scale, zero operational overhead on constrained hardware
- **Dual-source location** — Overland provides high-frequency GPS; iOS Shortcuts fills gaps when Overland drops out of background. Both sources are unified into a single SQL view (`location_unified`) with a cleaned view (`location_overland_cleaned`) that filters noise by accuracy threshold, displacement spikes, and cluster outliers
- **Prefect self-hosted** — all scheduled tasks (FX pulls, weather enrichment, backups, health gap checks, log digests) run as Prefect flows with structured logging on the Prefect dashboard and push notifications on completion and failure
- **Offsite backup** — SQLite snapshot encrypted with age, uploaded to Cloudflare R2 via rclone

---

## 🤖 Machine Learning

ML work begins once enough baseline data has been collected from the first leg of the trip. Planned analyses include:

- **Travel segmentation** — automatically identify cities, day trips, and country transitions over time using Hidden Markov Models
- **Spend forecasting** — predict daily cost by destination, activity, and travel style
- **Location clustering** — discover meaningful places (accommodation, cafés, workplaces) without manual labelling via DBSCAN
- **Anomaly detection** — flag unusual spending or movement patterns against established baselines, with plain-English explanations surfaced via Trevor (see below)

The public demo dataset is coordinate-fuzzed (~1–2 km Gaussian noise) and subsampled to remove personal information, while preserving enough structure for the ML models to be meaningfully demonstrated.

---

## 🤝 Trevor

Trevor is a companion RAG-based conversational assistant — a separate service embedded on the TravelNet demo site. It queries journal entries from a vector store and structured telemetry from TravelNet's database to answer questions like *"Was I happier in countries where I spent less?"* or *"What was I doing the week I overspent in Bangkok?"*

Its flagship feature is the Anomaly Explainer: when TravelNet detects an unusual pattern in spending or movement, it calls Trevor's `/explain` endpoint, which retrieves surrounding journal context and returns a plain-English explanation of what likely happened.

Trevor is in active design — see its [repository](https://github.com/danielroberts20/Trevor-For-TravelNet) for the current spec.

---

## 🚀 Trip Route

**June 2026 →** Philadelphia / DC → USA (summer camp) → Seattle → Fiji (stopover) → **Australia** (WHV) → **New Zealand** (WHV) → **South-East Asia** (backpacking) → **Canada** (WHV)

---

## 📖 Further Reading

- [Interactive dashboard and demo dataset](https://travelnet.dev)
- [Portfolio and blog](https://danielroberts20.github.io) — posts documenting TravelNet's development and design choices
- [Licence: CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — free to use and adapt with attribution, not for commercial purposes; applies to both code and demo dataset

---

*Built and maintained on the road. Feedback and questions welcome via [Issues](https://github.com/danielroberts20/TravelNet/issues).*