import { useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { useStats, fmt, fmtDate } from '../hooks/useStats';
import { TRIP_START, LEGS, GITHUB_REPO, TREVOR_REPO, PLANNED_COUNTRIES } from '../data/travel';

/* ------------------------------------------------------------------ */
/* Countdown                                                            */
/* ------------------------------------------------------------------ */
function Countdown() {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const departure = new Date(TRIP_START);

    function tick() {
      const el = ref.current;
      if (!el) return;
      const diff = departure.getTime() - Date.now();

      if (diff <= 0) {
        el.innerHTML = '<span class="countdown-value mono" style="font-size:20px;color:var(--accent-teal)">Travelling now ✈</span>';
        return;
      }

      const d = Math.floor(diff / 86400000);
      const h = Math.floor((diff % 86400000) / 3600000);
      const m = Math.floor((diff % 3600000) / 60000);
      const s = Math.floor((diff % 60000) / 1000);
      const z = (n: number) => String(n).padStart(2, '0');

      el.innerHTML = `
        <div class="countdown-unit"><span class="countdown-value">${d}</span><span class="countdown-label">days</span></div>
        <span class="countdown-sep">:</span>
        <div class="countdown-unit"><span class="countdown-value">${z(h)}</span><span class="countdown-label">hrs</span></div>
        <span class="countdown-sep">:</span>
        <div class="countdown-unit"><span class="countdown-value">${z(m)}</span><span class="countdown-label">min</span></div>
        <span class="countdown-sep">:</span>
        <div class="countdown-unit"><span class="countdown-value">${z(s)}</span><span class="countdown-label">sec</span></div>
      `;
    }

    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, []);

  return <div className="countdown" id="countdown" ref={ref}><span className="countdown-value mono">—</span></div>;
}

/* ------------------------------------------------------------------ */
/* Scroll reveal                                                         */
/* ------------------------------------------------------------------ */
function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll<HTMLElement>('.reveal');
    if (!els.length) return;
    const observer = new IntersectionObserver(
      entries => entries.forEach(e => { if (e.isIntersecting) { e.target.classList.add('visible'); observer.unobserve(e.target); } }),
      { threshold: 0.1, rootMargin: '0px 0px -40px 0px' },
    );
    els.forEach(el => observer.observe(el));
    return () => observer.disconnect();
  });
}

/* ------------------------------------------------------------------ */
/* Home page                                                            */
/* ------------------------------------------------------------------ */
export default function Home() {
  const stats = useStats();
  useReveal();

  const heroStatusText =
    stats?.status === 'travelling' && stats.current_leg
      ? `Live — currently in ${stats.current_leg.name} ${stats.current_leg.emoji}`
      : stats?.status === 'finished'
      ? 'Trip complete — full dataset available'
      : 'Pre-departure \u2014 launching June 2026';

  const statDaysSubLabel =
    stats?.current_leg
      ? `${stats.current_leg.emoji} ${stats.current_leg.name}${stats.current_leg.stopover ? ' (stopover)' : ''}`
      : 'Across all legs';

  return (
    <>
      {/* ============================================================
          HERO
          ============================================================ */}
      <section className="hero">
        <div className="hero-glow"></div>

        <div className="hero-eyebrow">
          <span className="nav-status-dot"></span>
          <span id="hero-status-text">{heroStatusText}</span>
        </div>
        <h1 className="hero-title">
          Three years.<br />
          <span className="accent-teal">Three continents.</span><br />
          One <span className="accent-blue">dataset.</span>
        </h1>

        <p className="hero-subtitle">
          TravelNet is a personal data collection and machine learning platform built to run unattended across 2&ndash;3 years of international travel. GPS traces, health metrics, finances &mdash; all in one place.
        </p>

        <div className="hero-cta-group">
          <Link to="/journey" className="btn btn-primary">View the Journey</Link>
          <Link to="/about" className="btn btn-secondary">How it works</Link>
        </div>

      </section>

      {/* ============================================================
          COUNTDOWN
          ============================================================ */}
      <section style={{ padding: 'var(--space-5) var(--space-6)', textAlign: 'center', background: 'var(--bg-sunken)' }}>
        <p className="section-eyebrow" style={{ textAlign: 'center', marginBottom: 'var(--space-3)' }}>Departure countdown</p>
        <Countdown />
        <p style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-tertiary)', marginTop: 'var(--space-3)', letterSpacing: '0.06em' }}>
          Philadelphia, PA &rarr; Global
        </p>
      </section>

      {/* ============================================================
          STATS
          ============================================================ */}
      <section className="stats-bar">
        <div className="stats-bar-inner">
          <p className="section-eyebrow reveal">Platform stats</p>
          <div className="stats-grid">

            <div className="stat-card reveal">
              <span className="stat-icon">📍</span>
              <span className="stat-value accent-teal" data-stat="gps_points">{fmt(stats?.gps_points)}</span>
              <span className="stat-label">GPS points logged</span>
              <span className="stat-sublabel">Overland + Shortcuts</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">🌍</span>
              <span className="stat-value accent-blue" data-stat="total_countries">
                {stats?.status === 'pre_departure' || !stats
                  ? PLANNED_COUNTRIES
                  : fmt(stats.total_countries)}
              </span>
              <span className="stat-label">
                {stats?.status === 'pre_departure' || !stats ? 'Countries planned' : 'Countries visited'}
              </span>
              <span className="stat-sublabel">
                {stats?.status === 'pre_departure' || !stats
                  ? 'USA · AUS · NZ · SE Asia · CAN'
                  : stats?.current_leg?.name ?? 'Across all legs'}
              </span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">❤️</span>
              <span className="stat-value accent-red" data-stat="health_records">{fmt(stats?.health_records)}</span>
              <span className="stat-label">Health records</span>
              <span className="stat-sublabel">Apple Health via HAE</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">💳</span>
              <span className="stat-value accent-orange" data-stat="transactions">{fmt(stats?.transactions)}</span>
              <span className="stat-label">Transactions</span>
              <span className="stat-sublabel">Revolut · Wise · Cash</span>
            </div>

            <div className="stat-card reveal">
              <span className="stat-icon">📅</span>
              <span className="stat-value" data-stat="days_travelling">{fmt(stats?.days_travelling)}</span>
              <span className="stat-label">Days travelling</span>
              <span className="stat-sublabel">{statDaysSubLabel}</span>
            </div>

          </div>

          {stats?.last_synced && (
            <p className="stats-sync-info reveal" id="stats-sync-info">
              Last synced: {fmtDate(stats.last_synced)}
            </p>
          )}
        </div>
      </section>

      {/* ============================================================
          FEATURES
          ============================================================ */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">What&apos;s in here</p>
          <h2 className="section-title reveal">Data. Analysis. Insights.</h2>
          <p className="section-subtitle reveal">
            Every module of TravelNet feeds into a unified dataset &mdash; built to be explored, visualised, and learned from.
          </p>

          <div className="feature-grid">

            <div className="feature-card reveal" data-accent="teal">
              <div className="feature-icon-wrap" data-bg="teal">🗺️</div>
              <h3 className="feature-title">Journey</h3>
              <p className="feature-desc">
                Interactive GPS traces from six countries, rendered in Kepler.gl. Flight arcs, movement density, and time-animated paths across the full trip.
              </p>
              <Link to="/journey" className="feature-tag">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block' }}></span>
                Coming September 2026
              </Link>
            </div>

            <div className="feature-card reveal" data-accent="blue">
              <div className="feature-icon-wrap" data-bg="blue">📊</div>
              <h3 className="feature-title">Data Explorer</h3>
              <p className="feature-desc">
                Live database statistics, interactive charts, and spending breakdowns by country, currency, and category &mdash; updated as I travel.
              </p>
              <Link to="/explorer" className="feature-tag">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block' }}></span>
                Coming 2026
              </Link>
            </div>

            <div className="feature-card reveal" data-accent="orange">
              <div className="feature-icon-wrap" data-bg="orange">🧠</div>
              <h3 className="feature-title">ML Insights</h3>
              <p className="feature-desc">
                Travel leg segmentation, fitness trend decomposition, spending pattern detection &mdash; machine learning models trained on two years of personal data.
              </p>
              <Link to="/ml" className="feature-tag">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block' }}></span>
                Coming late 2026
              </Link>
            </div>

            <div className="feature-card reveal" data-accent="purple">
              <div className="feature-icon-wrap" data-bg="purple">🧠</div>
              <h3 className="feature-title">Trevor</h3>
              <p className="feature-desc">
                A RAG-based AI assistant that lets you have a conversation with three years of personal travel data — journal entries, GPS traces, health metrics, and spending logs.
              </p>
              <Link to="/trevor" className="feature-tag">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block' }}></span>
                Coming soon
              </Link>
            </div>

            <div className="feature-card reveal" data-accent="red">
              <div className="feature-icon-wrap" data-bg="red">🔬</div>
              <h3 className="feature-title">Open Dataset</h3>
              <p className="feature-desc">
                An anonymised, privacy-fuzzed subset of the full dataset. GPS coordinates offset by ~1&thinsp;km Gaussian noise. Free to explore and build on.
              </p>
              <a href={GITHUB_REPO} className="feature-tag" target="_blank" rel="noopener">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-orange)', display: 'inline-block' }}></span>
                GitHub &rarr;
              </a>
            </div>

          </div>
        </div>
      </section>

      {/* ============================================================
          TRIP TIMELINE
          ============================================================ */}
      <section className="timeline">
        <div className="section-inner">
          <p className="section-eyebrow reveal">The route</p>
          <h2 className="section-title reveal">Six legs. 3 years.</h2>
        </div>
        <div className="timeline-track reveal">
          {LEGS.map(leg => {
            const isCurrent = stats?.current_leg?.id === leg.id;
            const allLegs = LEGS;
            const currentIdx = allLegs.findIndex(l => l.id === stats?.current_leg?.id);
            const legIdx = allLegs.findIndex(l => l.id === leg.id);
            const isDone = currentIdx > -1 && legIdx < currentIdx;
            return (
              <div
                key={leg.id}
                className={`timeline-leg${isCurrent ? ' active' : ''}${isDone ? ' done' : ''}`}
                data-leg-id={leg.id}
              >
                <div className="timeline-dot">{leg.emoji}</div>
                <span className="timeline-name">{leg.name}</span>
              </div>
            );
          })}
        </div>
      </section>

      {/* ============================================================
          TECH STACK
          ============================================================ */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Built with</p>
          <h2 className="section-title reveal">The stack</h2>
          <p className="section-subtitle reveal">
            A Raspberry Pi 4B runs unattended, collecting data 24/7. Everything streams into a single SQLite database.
          </p>

          <div className="stack-grid">
            <div className="stack-item reveal">
              <span className="stack-item-icon">🖥️</span>
              Raspberry Pi 4B — always-on server
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">⚡</span>
              FastAPI — data ingest endpoints
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">🗄️</span>
              SQLite — unified travel database
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">📍</span>
              Overland + iOS Shortcuts
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">❤️</span>
              Apple Watch Ultra + Health Auto Export
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">💳</span>
              Revolut · Wise · exchangerate.host
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">🧮</span>
              scikit-learn · HDBSCAN · pandas
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">📊</span>
              Kepler.gl · Plotly · Dash
            </div>
            <div className="stack-item reveal">
              <span className="stack-item-icon">🔒</span>
              Tailscale · Cloudflare R2
            </div>
          </div>

          <div style={{ marginTop: 'var(--space-6)', textAlign: 'center' }}>
            <Link to="/about" className="btn btn-secondary reveal">Full architecture overview →</Link>
          </div>
        </div>
      </section>

      {/* ============================================================
          TREVOR
          ============================================================ */}
      <section className="trevor-section">
        <div className="section-inner">
          <p className="section-eyebrow reveal">AI Assistant</p>
          <h2 className="section-title reveal">Meet Trevor.</h2>
          <p className="section-subtitle reveal">
            A conversational interface built on three years of personal travel data. Ask anything &mdash; from journal entries to GPS anomalies.
          </p>

          <div className="trevor-split">

            <div>
              <div className="trevor-feature-list">
                <div className="trevor-feature-item reveal">
                  <span className="trevor-feature-item-icon">💬</span>
                  <div>
                    <span className="trevor-feature-item-title">Journal Querying</span>
                    <span className="trevor-feature-item-text">Ask questions about daily entries, moods, and places — semantic search across thousands of journal chunks enriched with GPS and HealthKit metadata.</span>
                  </div>
                </div>
                <div className="trevor-feature-item reveal">
                  <span className="trevor-feature-item-icon">🔗</span>
                  <div>
                    <span className="trevor-feature-item-title">Cross-Stream Correlation</span>
                    <span className="trevor-feature-item-text">"Did my step count drop in weeks I overspent?" Trevor joins structured telemetry with narrative journal context to answer questions no single dataset could.</span>
                  </div>
                </div>
                <div className="trevor-feature-item reveal">
                  <span className="trevor-feature-item-icon">💭</span>
                  <div>
                    <span className="trevor-feature-item-title">Reasoning Freedom</span>
                    <span className="trevor-feature-item-text">Allow Trevor to think creatively and provide nuanced insights that go beyond the TravelNet database. Any speculative reasoning is clearly indicated.</span>
                  </div>
                </div>
              </div>

              <a href={TREVOR_REPO} className="btn btn-secondary reveal" target="_blank" rel="noopener">
                <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                  <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
                </svg>
                Trevor on GitHub &rarr;
              </a>
            </div>

            <div className="trevor-chat-preview reveal">
              <div className="trevor-chat-chrome">
                <div className="trevor-avatar-sm">✦</div>
                <span className="trevor-chat-chrome-name">Trevor</span>
                <span className="chrome-label mono">TravelNet Assistant</span>
              </div>
              <div className="trevor-chat-messages">
                <div className="trevor-msg trevor-msg-user">
                  <span className="trevor-msg-label">You</span>
                  <div className="trevor-msg-bubble">Summarise how I felt during Southeast Asia</div>
                </div>
                <div className="trevor-msg trevor-msg-ai">
                  <span className="trevor-msg-label">Trevor</span>
                  <div className="trevor-msg-bubble">Across 47 journal entries from Thailand, Vietnam, and Cambodia your average valence was +0.34 — meaningfully above baseline. The highest-rated days clustered around slow travel weeks with low spending.</div>
                </div>
                <div className="trevor-msg trevor-msg-user">
                  <span className="trevor-msg-label">You</span>
                  <div className="trevor-msg-bubble">What caused the spending spike in week 14?</div>
                </div>
                <div className="trevor-msg trevor-msg-ai">
                  <span className="trevor-msg-label">Trevor</span>
                  <div className="trevor-msg-bubble">TravelNet flagged a 3.1σ outlier. Your entry from that day mentions an unexpected flight rebook and two nights in an unplanned city — consistent with the transaction breakdown.</div>
                </div>
              </div>
              <div className="trevor-chat-overlay">
                <span className="trevor-overlay-badge">
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-purple)', display: 'inline-block' }}></span>
                  Coming soon
                </span>
                <a href={TREVOR_REPO} className="btn btn-secondary" style={{ fontSize: 13, padding: '10px var(--space-4)' }} target="_blank" rel="noopener">
                  View on GitHub &rarr;
                </a>
              </div>
            </div>

          </div>
        </div>
      </section>
    </>
  );
}
