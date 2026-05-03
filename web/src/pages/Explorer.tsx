import { useEffect } from 'react';
import { useStats, fmt, fmtDate } from '../hooks/useStats';

function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll<HTMLElement>('.reveal');
    if (!els.length) return;
    const observer = new IntersectionObserver(
      entries => entries.forEach(e => {
        if (e.isIntersecting) { e.target.classList.add('visible'); observer.unobserve(e.target); }
      }),
      { threshold: 0.1, rootMargin: '0px 0px -40px 0px' },
    );
    els.forEach(el => observer.observe(el));
    return () => observer.disconnect();
  });
}

const STREAMS = [
  {
    icon: '📍',
    title: 'GPS Traces',
    accent: 'teal',
    source: 'Overland + iOS Shortcuts',
    rate: '5-second intervals',
    description: 'Continuous location tracking via Overland for iOS, recording a point every 5 seconds. iOS Shortcuts fills gaps — waypoints logged whenever the shortcut runs, covering periods where Overland is paused or in power-saving mode. Both streams are deduped and merged into a single SQL view.',
    schema: 'location_unified (lat, lon, timestamp, accuracy, source)',
  },
  {
    icon: '❤️',
    title: 'Health Metrics',
    accent: 'red',
    source: 'Apple Watch Ultra + Health Auto Export',
    rate: 'Continuous to daily',
    description: 'Apple Watch Ultra records continuous heart rate, HRV, sleep staging, VO₂ max estimates, respiratory rate, SpO₂, and workout data. Health Auto Export syncs all HealthKit quantities to the ingest API — over 40 distinct metric types at varying cadences.',
    schema: 'health_quantity, health_heart_rate, health_sleep, workouts, state_of_mind',
  },
  {
    icon: '💳',
    title: 'Financial Transactions',
    accent: 'orange',
    source: 'Revolut + Wise',
    rate: 'Manual upload, weekly',
    description: 'Revolut CSV exports and Wise ZIP archives uploaded to the ingest API and parsed into a normalised transactions table. All amounts are GBP-normalised via daily FX rates fetched by a scheduled task. Merchant name, category, currency, and local amount are all preserved.',
    schema: 'transactions (amount_local, amount_gbp, currency, merchant, category, ts)',
  },
  {
    icon: '🌤️',
    title: 'Weather',
    accent: 'blue',
    source: 'Open-Meteo API',
    rate: 'Nightly automated',
    description: 'Historical and forecast weather fetched automatically from Open-Meteo — a free, open-source weather API with no rate limits. Queried nightly for the current location: temperature, precipitation, UV index, and WMO weather code. Used as a confounder in every ML model.',
    schema: 'weather_daily (date, temp_avg_c, precip_mm, uv_index_max, weathercode)',
  },
];

const COMING_FEATURES = [
  {
    title: 'GPS Density Map',
    text: 'A Kepler.gl heatmap of all GPS points — where time was spent, animated day by day across the full trip.',
  },
  {
    title: 'Spending by Country',
    text: 'Monthly spend broken down by category and country, normalised to GBP. Effective cost of living per destination.',
  },
  {
    title: 'Health Trends',
    text: 'Sleep quality, resting HR, and HRV over time — with country transitions overlaid to show adaptation patterns.',
  },
  {
    title: 'Transport Breakdown',
    text: 'Fraction of time on foot, cycling, in vehicles, and on flights — classified from raw GPS velocity profiles.',
  },
];

export default function Explorer() {
  const stats = useStats();
  useReveal();

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────── */}
      <section style={{ paddingBottom: 'var(--space-6)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Data Explorer</p>
          <h1 className="section-title reveal" style={{ fontSize: 'clamp(36px, 5vw, 56px)' }}>
            Everything collected.<br />
            <span className="accent-teal">All in one place.</span>
          </h1>
          <p className="section-subtitle reveal">
            TravelNet is a continuous data collection system — GPS traces, health metrics,
            financial transactions, and weather, funnelled into a single SQLite database
            on a Raspberry Pi. This page will become a live, interactive explorer of
            everything collected across the full trip.
          </p>
          <p className="section-eyebrow reveal" style={{ marginTop: 'var(--space-4)' }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block', marginRight: 6 }}></span>
            Live interactive charts — available once the trip is underway, late 2026
          </p>
        </div>
      </section>

      {/* ── Pre-departure baseline ──────────────────────── */}
      <section className="stats-bar">
        <div className="stats-bar-inner">
          <p className="section-eyebrow reveal">Pre-departure baseline — collected in London</p>
          <div className="stats-grid">

            <div className="stat-card reveal">
              <span className="stat-icon">📍</span>
              <span className="stat-value accent-teal">{fmt(stats?.gps_points)}</span>
              <span className="stat-label">GPS points</span>
              <span className="stat-sublabel">Overland + Shortcuts</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">❤️</span>
              <span className="stat-value accent-red">{fmt(stats?.health_records)}</span>
              <span className="stat-label">Health records</span>
              <span className="stat-sublabel">40+ distinct metrics</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">💳</span>
              <span className="stat-value accent-orange">{fmt(stats?.transactions)}</span>
              <span className="stat-label">Transactions</span>
              <span className="stat-sublabel">GBP-normalised</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">🌤️</span>
              <span className="stat-value accent-blue">Nightly</span>
              <span className="stat-label">Weather sync</span>
              <span className="stat-sublabel">Open-Meteo API</span>
            </div>

          </div>
          {stats?.last_synced && (
            <p className="stats-sync-info reveal">Last synced: {fmtDate(stats.last_synced)}</p>
          )}
        </div>
      </section>

      {/* ── Data pipeline ─────────────────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Data pipeline</p>
          <h2 className="section-title reveal">Four streams. One database.</h2>
          <p className="section-subtitle reveal">
            Each data source has its own ingest path and normalisation logic. A nightly
            scheduled task aggregates them into a unified{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>daily_summary</span>{' '}
            table — the foundation of every ML model.
          </p>

          <div className="feature-grid" style={{ marginTop: 'var(--space-6)' }}>
            {STREAMS.map(s => (
              <div key={s.title} className="feature-card reveal" data-accent={s.accent}>
                <div className="feature-icon-wrap" data-bg={s.accent}>{s.icon}</div>
                <h3 className="feature-title">{s.title}</h3>
                <p className="feature-desc" style={{ marginBottom: 'var(--space-3)' }}>
                  {s.description}
                </p>
                <div style={{
                  fontFamily: 'var(--font-mono)',
                  fontSize: 11,
                  color: 'var(--text-tertiary)',
                  background: 'var(--bg-sunken)',
                  borderRadius: 'var(--radius-sm)',
                  padding: 'var(--space-2) var(--space-3)',
                  marginBottom: 'var(--space-3)',
                }}>
                  {s.schema}
                </div>
                <span className="feature-tag">
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: `var(--accent-${s.accent})`, display: 'inline-block' }}></span>
                  {s.source} &middot; {s.rate}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── What's coming ─────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">What&apos;s coming</p>
          <h2 className="section-title reveal">Live data. Interactive charts.</h2>
          <p className="section-subtitle reveal">
            As the trip progresses, this page will be populated with real data from every leg.
          </p>

          <div className="journey-build-grid" style={{ marginTop: 'var(--space-6)' }}>
            {COMING_FEATURES.map(item => (
              <div key={item.title} className="journey-build-card journey-build-card--future reveal">
                <div className="journey-build-card-top">
                  <span className="journey-build-title">{item.title}</span>
                </div>
                <p className="journey-build-text">{item.text}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Visualisation stack ───────────────────────────── */}
      <section>
        <div className="section-inner" style={{ maxWidth: 760 }}>
          <p className="section-eyebrow reveal">Built on</p>
          <h2 className="section-title reveal" style={{ fontSize: 28 }}>The visualisation stack</h2>

          <div className="stack-grid" style={{ marginTop: 'var(--space-5)' }}>
            <div className="stack-item reveal"><span className="stack-item-icon">🗺️</span>Kepler.gl — GPS trace maps</div>
            <div className="stack-item reveal"><span className="stack-item-icon">📊</span>Deck.GL — custom data layers</div>
            <div className="stack-item reveal"><span className="stack-item-icon">📈</span>D3 — statistical charts</div>
            <div className="stack-item reveal"><span className="stack-item-icon">⚡</span>React + Vite — frontend</div>
            <div className="stack-item reveal"><span className="stack-item-icon">🗄️</span>FastAPI — data API</div>
            <div className="stack-item reveal"><span className="stack-item-icon">🐍</span>pandas + SQLite — aggregation</div>
          </div>
        </div>
      </section>
    </>
  );
}
