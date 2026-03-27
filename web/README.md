# TravelNet — Demo Site

Public demo dashboard for the TravelNet travel data platform.

## Local development

```bash
bundle install
bundle exec jekyll serve --livereload
# → http://localhost:4000
```

## Structure

```
_config.yml          # Site config + trip metadata
_data/stats.yml      # Update this as you travel — stats auto-populate
_layouts/
  default.html       # Main layout (nav + footer)
  coming-soon.html   # Stub layout for unreleased pages
_pages/              # (reserved for future markdown pages)
assets/
  css/main.css       # Full design system — edit CSS variables to retheme
  js/main.js         # Countdown, scroll reveal, GPS canvas animation
index.html           # Homepage
journey/             # Coming soon → swap for Kepler.gl embed
explorer/            # Coming soon → swap for Plotly/Dash embed
ml/                  # Coming soon → swap for ML results
about/               # Project overview
```

## Updating stats (while travelling)

Edit `_data/stats.yml`:

```yaml
status: "live"
last_synced: "2026-09-15T14:32:00Z"
days_travelling: 7
countries_visited: 2
gps_points: 150000
health_records: 2800
transactions: 143
current_location: "Sydney, Australia"
current_leg: "Australia (WHV)"
```

Commit and push — GitHub Pages rebuilds automatically.

## Adding Kepler.gl (Journey page)

Replace `journey/index.html` with an iframe embed:

```html
---
layout: default
title: Journey
permalink: /journey/
---
<div style="height: calc(100vh - 52px);">
  <iframe src="/assets/kepler/index.html" style="width:100%;height:100%;border:none;"></iframe>
</div>
```

Drop the exported Kepler.gl bundle into `assets/kepler/`.

## Deploying

Push to GitHub. Set Pages source to `main` branch root in repo Settings.
