Temporary README.

# TravelNet &mdash; Cost of Time & Space
![Pixel art zoomed in view of Earth with a map pin and wire traces](/assets/icon.jpg)

A project completed while I travelled across the USA, Australia, New Zealand, South-East Asia and Canada. Check out the [website](https://danielroberts20.github.io/TravelNet)!

TO-DO:
-
- Enabling watch logs
  - Check if there is multiple logs within +/- 0.5*log frequency
  - Prefer data from watch logs, fill gaps with phone logs
- Server-side, calculate country (and state?) from lat/lon
- Document/write-ups
  - Describe docker containers
  - ML GPU compute service on PC
  - Full documentation for all functions and methods
- Additional GET endpoints
- Machine Learning
    - Travel segmentation
      - Automatically segment into different cities/activities/daytrip/country
      - HMMs, change point detection (link to dissertation)
    - Currency/spending prediction
      - Forecast cost per day
      - Time series regression
    - Cluster locations to meaningful  locations
      - Automatically discover "home" in each country/city
      - Auto discover cafés, airports, workplace etc.
      - Clustering models
    - Detect unusual movement
      - Detect if I do something out of the ordinary
      - Anomaly detection
- Include *much* smaller DB on Github (ensure no personal information) to be used with the demonstration website.