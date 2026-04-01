import { Link } from 'react-router-dom';
import { GITHUB_REPO } from '../data/travel';

export default function About() {
  return (
    <section style={{ padding: 'var(--space-10) var(--space-6) var(--space-8)' }}>
      <div className="section-inner" style={{ maxWidth: 760 }}>

        <p className="section-eyebrow">About the project</p>
        <h1 className="section-title">Why I built this</h1>

        <p style={{ fontSize: 18, color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-6)' }}>
          I&apos;m Dan — a CS graduate about to spend 2–3 years travelling across four continents on working holiday visas. Before I left, I wanted to answer a question: <em style={{ color: 'var(--text-primary)' }}>what does long-term nomadic life actually look like in data?</em>
        </p>

        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }}>
          TravelNet is the answer. A Raspberry Pi 4B runs unattended throughout the trip, ingesting GPS traces, health metrics, financial transactions, and weather data around the clock. By the time I return, I&apos;ll have a continuous, multi-year personal dataset unlike anything that typically ends up in a portfolio.
        </p>

        <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-7)' }}>
          The ML pipeline — clustering, segmentation, anomaly detection — kicks off from Australia, using the US baseline data for comparison. The public demo here is the anonymised output of all of it.
        </p>

        <hr className="divider" />

        <h2 style={{ fontSize: 24, fontWeight: 600, letterSpacing: '-0.02em', marginBottom: 'var(--space-5)' }}>Architecture</h2>

        <div className="stack-grid" style={{ marginBottom: 'var(--space-6)' }}>
          <div className="stack-item"><span className="stack-item-icon">ingest</span> FastAPI on port 8000</div>
          <div className="stack-item"><span className="stack-item-icon">dash</span> Flask admin UI on port 8080</div>
          <div className="stack-item"><span className="stack-item-icon">proxy</span> nginx, TLS via Tailscale</div>
          <div className="stack-item"><span className="stack-item-icon">store</span> SQLite on external HDD</div>
          <div className="stack-item"><span className="stack-item-icon">sync</span> Tailscale remote access</div>
          <div className="stack-item"><span className="stack-item-icon">bkp</span> Cloudflare R2, age-encrypted</div>
        </div>

        <h2 style={{ fontSize: 24, fontWeight: 600, letterSpacing: '-0.02em', marginBottom: 'var(--space-5)' }}>Data sources</h2>

        <div className="stack-grid" style={{ marginBottom: 'var(--space-6)' }}>
          <div className="stack-item"><span className="stack-item-icon">GPS</span> Overland (iOS) — 5s intervals</div>
          <div className="stack-item"><span className="stack-item-icon">GPS</span> iOS Shortcuts — gap fill</div>
          <div className="stack-item"><span className="stack-item-icon">♥</span> Apple Watch Ultra + HAE</div>
          <div className="stack-item"><span className="stack-item-icon">💳</span> Revolut CSV exports</div>
          <div className="stack-item"><span className="stack-item-icon">💳</span> Wise ZIP exports</div>
          <div className="stack-item"><span className="stack-item-icon">🌤</span> Open-Meteo weather API</div>
        </div>

        <hr className="divider" />

        <div style={{ display: 'flex', gap: 'var(--space-3)', flexWrap: 'wrap' }}>
          <a href={GITHUB_REPO} className="btn btn-primary" target="_blank" rel="noopener">View on GitHub</a>
          <Link to="/" className="btn btn-secondary">← Home</Link>
        </div>

      </div>
    </section>
  );
}
