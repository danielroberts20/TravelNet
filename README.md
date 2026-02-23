Temporary README.

A project completed while I travelled across the USA, Australia, New Zealand, Canada and South-East Asia.

TO-DO:
-
- Enabling watch logs
  - Check if there is multiple logs within +/- 0.5*log frequency
  - Prefer data from watch logs, fill gaps with phone logs
- Server-side, calculate country (and state?) from lat/lon
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
      - Auto discover cafes, airports, workplace etc.
      - Clustering models
    - Detect unusual movement
      - Detect if I do something out of the ordinary
      - Anomaly detection