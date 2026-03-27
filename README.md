# TravelNet &mdash; Cost of Time & Space

![Pixel art zoomed in view of Earth with a map pin and wire traces](/assets/icon.jpg)

> *A personal data platform built across 2–3 years of travel through the USA, Australia, New Zealand, South-East Asia, and Canada.*

[![Licence: CC BY-NC 4.0](https://img.shields.io/badge/Licence-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)

## 🌟 Highlights

- **End-to-end personal data pipeline** — transactions, GPS traces, health metrics, and weather, all collected in real time during travel
- **Multi-source, conflict-aware ingestion** — Revolut, Wise, cash, and local bank accounts reconciled into a single unified ledger
- **Continuous location tracking** — dual-source GPS (Overland + iOS Shortcuts) with automatic gap-filling and ~99.4% coverage
- **ML-powered insights** — travel segmentation, spend forecasting, location clustering, and anomaly detection derived from live data
- **Public demo dataset** — anonymised and coordinate-fuzzed subset available for exploration via the [interactive dashboard](https::/travelnet.dev)

---

## ℹ️ Overview

TravelNet is a self-hosted data collection, analysis, and visualisation system designed to run unattended on a Raspberry Pi throughout 2–3 years of international travel. Every transaction I make, every location I visit, and every health metric I track is ingested, enriched, and stored — building a dataset that grows richer the longer I travel.

The project has two goals. The first is personal: to build an honest, granular record of what long-term travel actually costs — in money, time, distance, and routine. The second is technical: to serve as a real-world ML project with genuinely messy, self-collected data, deployed in constrained conditions from day one.

The backend is a FastAPI service running in Docker, with SQLite on an external HDD and Tailscale for remote access. Data is collected passively via scheduled jobs and lightweight iOS integrations, with email alerts for any failures.

Check out the [interactive dashboard](https://danielroberts20.github.io/TravelNet) to explore the demo dataset.

---

## 🗺️ Data Sources

| Domain | Sources |
|---|---|
| **Transactions** | Revolut (CSV), Wise (multi-currency zip), Cash (manual endpoint), local bank (on arrival) |
| **Location** | Overland (continuous GPS), iOS Shortcuts (5-min interval gap-fill) |
| **Health** | Health Auto Export (daily upload) |
| **FX Rates** | exchangerate.host (automated, bi-weekly) |
| **Weather** | Open-Meteo (retroactive enrichment by coordinates + date) |

---

## 🤖 Machine Learning

ML work begins once enough baseline data has been collected from the first leg of the trip. Planned analyses include:

- **Travel segmentation** — automatically identify cities, day trips, and country transitions over time
- **Spend forecasting** — predict daily cost by destination, activity, and travel style
- **Location clustering** — discover meaningful places (accommodation, cafés, workplaces) without manual labelling
- **Anomaly detection** — flag unusual spending or movement patterns against established baselines

The public demo dataset is coordinate-fuzzed (~1–2 km Gaussian noise) and subsampled to remove personal information, while preserving enough structure for the ML models to be meaningfully demonstrated.

---

## 🏗️ Architecture
```
                 📱 iPhone
                /          \
    Overland &         Revolut/Wise
    Health Auto        CSV uploads
    Export (3am)            \
               \              ▼
                ▼    ┌─────────────────────────────────────┐
                ──►  │          Raspberry Pi (Docker)       │
                     │                                      │
                     │  FastAPI  ──►  SQLite (ext. HDD)     │
                     │     ▲               │                │
                     │  Scheduled      Backfill &           │
                     │  cron jobs      FX enrichment        │
                     └─────┬───────────────┴────────────────┘
                           │ Tailscale (remote access)
                           ▼
                     Laptop (local ML / dashboard dev)
                           │
                           ▼
                     GitHub Pages (public demo)
```

Data flows from iOS apps and manual uploads into FastAPI endpoints, is enriched with FX rates and weather retroactively, and is periodically snapshotted for ML work and dashboard updates.

---

## 🚀 Trip Route

**June 2026 →** Philadelphia / DC → USA (summer camp) → Seattle → Fiji (stopover) → **Australia** (WHV) → **New Zealand** (WHV) → **South-East Asia** (backpacking) → **Canada** (WHV)

---

## 📖 Further Reading

- [Interactive dashboard and demo dataset](https://danielroberts20.github.io/TravelNet)
- [Blog posts](https://danielroberts20.github.io/blog/) documenting TravelNet's development and design choices
- [Licence: CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) — free to use and adapt with attribution, not for commercial purposes; applies to both code and demo dataset

---

*Built and maintained on the road. Feedback and questions welcome via [Issues](https://github.com/danielroberts20/TravelNet/issues).*
