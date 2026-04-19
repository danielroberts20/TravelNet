# TravelNet — Demo Site

Public demo dashboard for the TravelNet travel data platform.

## Local development

```bash
npm install
npm run dev
# → http://localhost:5173
```

## Build

```bash
npm run build   # outputs to dist/
npm run preview # preview production build locally
```

## Structure

```
src/
  main.tsx          # React entry point
  App.tsx           # Router (6 routes)
  components/
    Layout.tsx      # Navbar, footer, Trevor chat widget
    GPSCanvas.tsx   # Animated deck.gl + MapLibre map preview
    ComingSoon.tsx  # Shared stub for unreleased pages
  pages/
    Home.tsx        # Hero, countdown, stats, features, trip timeline
    Journey.tsx     # Trip legs, build status
    Explorer.tsx    # Coming soon (late 2026)
    ML.tsx          # Coming soon (2027)
    Trevor.tsx      # AI assistant showcase
    About.tsx       # Architecture & data sources
  hooks/
    useStats.ts     # Fetches live stats from API with fallback
  data/
    travel.ts       # Build-time constants from travel.yml
assets/
  css/main.css      # Full design system — edit CSS variables to retheme
public/
  public_stats.json # Fallback stats if API unavailable
```

## Trip data

All trip metadata lives in `travel.yml` at the repo root — leg dates, map route stops, display strings. The Vite build reads it via a virtual module plugin and bakes it into the bundle.

## Stats API

Live stats are fetched from `https://api.travelnet.dev/public/stats` at runtime. If the API is unreachable (pre-departure, network error), `public/public_stats.json` is used as a fallback.

## Deploying

Deployed via Netlify. Push to `main` — Netlify builds automatically. SPA routing is handled by `public/_redirects`.
