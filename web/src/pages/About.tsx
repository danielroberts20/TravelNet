import { Link } from 'react-router-dom';
import { GITHUB_REPO, PLANNED_COUNTRIES, PLANNED_KM } from '../data/travel';

export default function About() {
  return (
    <section style={{ padding: 'var(--space-10) var(--space-6) var(--space-8)' }}>
      <div className="section-inner" style={{ maxWidth: 760 }}>

        <p className="section-eyebrow">About the project</p>
        <h1 className="section-title">Why I built this</h1>

        <p style={{ fontSize: 18, color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          I&apos;m Dan — a CS graduate about to spend 2–3 years travelling across four continents
          on working holiday visas. Before I left, I wanted to answer a question:{' '}
          <em style={{ color: 'var(--text-primary)' }}>what does long-term nomadic life actually look like in data?</em>
        </p>

        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          TravelNet is the answer. A Raspberry Pi 4B runs unattended throughout the trip, ingesting
          GPS traces, health metrics, financial transactions, and weather data around the clock.
          By the time I return, there will be a continuous, multi-year personal dataset unlike
          anything that typically ends up in a portfolio.
        </p>

        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          The trip spans {PLANNED_COUNTRIES} countries across roughly {(PLANNED_KM / 1000).toFixed(0)}k km
          of planned travel — USA, Fiji, Australia, New Zealand, SE Asia, and Canada. The
          ML pipeline kicks off from Australia, using the pre-departure London baseline for
          comparison. The public demo here is the live output of the system during the
          pre-departure phase.
        </p>

        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-7)' }}>
          The infrastructure had to be solid enough to run completely unattended on a Raspberry Pi
          for three years before I left. That constraint drove the entire design — no cloud
          dependencies for data storage, encrypted remote backups, fault-tolerant ingest with
          email alerting, and a comprehensive test suite to catch regressions before deployment.
        </p>

        <hr className="divider" />

        <h2 style={{ fontSize: 24, fontWeight: 600, letterSpacing: '-0.02em', marginBottom: 'var(--space-3)' }}>Architecture</h2>
        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          All services run on the same Raspberry Pi 4B: a FastAPI ingest server on port 8000,
          a Flask admin dashboard on port 8080, and nginx as a TLS-terminating reverse proxy.
          Tailscale provides a WireGuard mesh so the Pi is reachable from anywhere without
          a static IP or open firewall ports. Automated database backups are age-encrypted and
          shipped to Cloudflare R2 via a nightly cron job.
        </p>

        <div className="stack-grid" style={{ marginBottom: 'var(--space-6)' }}>
          <div className="stack-item"><span className="stack-item-icon">ingest</span> FastAPI on port 8000</div>
          <div className="stack-item"><span className="stack-item-icon">dash</span> Flask admin UI on port 8080</div>
          <div className="stack-item"><span className="stack-item-icon">proxy</span> nginx, TLS via Tailscale</div>
          <div className="stack-item"><span className="stack-item-icon">store</span> SQLite on external HDD</div>
          <div className="stack-item"><span className="stack-item-icon">sync</span> Tailscale remote access</div>
          <div className="stack-item"><span className="stack-item-icon">bkp</span> Cloudflare R2, age-encrypted</div>
        </div>

        <h2 style={{ fontSize: 24, fontWeight: 600, letterSpacing: '-0.02em', marginBottom: 'var(--space-3)' }}>Data sources</h2>
        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          Each source has a dedicated ingest path. GPS data is the highest volume stream —
          Overland tracks at 5-second intervals and iOS Shortcuts fills any gaps. Health
          metrics arrive via Health Auto Export, which syncs HealthKit data directly to the
          ingest API. Financial data requires manual CSV/ZIP uploads weekly. Weather is fetched
          automatically each night from Open-Meteo, keyed to the current GPS position.
        </p>

        <div className="stack-grid" style={{ marginBottom: 'var(--space-6)' }}>
          <div className="stack-item"><span className="stack-item-icon">GPS</span> Overland (iOS) — 5s intervals</div>
          <div className="stack-item"><span className="stack-item-icon">GPS</span> iOS Shortcuts — gap fill</div>
          <div className="stack-item"><span className="stack-item-icon">♥</span> Apple Watch Ultra + HAE</div>
          <div className="stack-item"><span className="stack-item-icon">💳</span> Revolut CSV exports</div>
          <div className="stack-item"><span className="stack-item-icon">💳</span> Wise ZIP exports</div>
          <div className="stack-item"><span className="stack-item-icon">🌤</span> Open-Meteo weather API</div>
        </div>

        <h2 style={{ fontSize: 24, fontWeight: 600, letterSpacing: '-0.02em', marginBottom: 'var(--space-3)' }}>Pre-departure baseline</h2>
        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-7)' }}>
          Data collection started months before departure — GPS traces from London, Apple Watch
          health metrics, and financial transactions from day-to-day life. This pre-departure
          period serves as a stationary baseline: the ML pipeline uses it to establish personal
          norms before the disruption of travel begins. When the first country transition happens,
          every deviation from that baseline is meaningful.
        </p>

        <hr className="divider" />

        <div style={{ display: 'flex', gap: 'var(--space-3)', flexWrap: 'wrap' }}>
          <a href={GITHUB_REPO} className="btn btn-primary" target="_blank" rel="noopener">View on GitHub</a>
          <Link to="/journey" className="btn btn-secondary">The journey →</Link>
          <Link to="/ml" className="btn btn-secondary">ML pipeline →</Link>
          <Link to="/" className="btn btn-secondary">← Home</Link>
        </div>

      </div>
    </section>
  );
}
