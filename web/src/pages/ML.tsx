import { useEffect } from 'react';
import { Link } from 'react-router-dom';

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

type AILink = { label: string; anchor: string };

const TIER1 = [
  {
    id: 'causal-wellbeing-graph',
    emoji: '🔗',
    accent: 'orange',
    title: 'Causal Wellbeing Graph',
    question: 'Not "are sleep and mood correlated?" — but "does poor sleep cause worse mood, or does bad mood cause poor sleep?"',
    method: 'PC algorithm (causal discovery) learns a Directed Acyclic Graph from observational data using conditional independence tests. 500-bootstrap resampling produces per-edge confidence scores. No experiments needed — just enough longitudinal data.',
    viz: 'Force-directed DAG in D3. Node size = causal centrality. Edge opacity = bootstrap confidence. Interactive "do-calculus" panel: hypothetically set any variable and see propagated effects through the graph.',
    phase: 'Phase 3',
    libs: 'causallearn · D3',
    needs: '200+ days · sleep, HRV, mood, spend, steps, weather',
    aiLinks: [
      { label: 'Multi-Agent Analyst',    anchor: 'multi-agent-analyst' },
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
  {
    id: 'day-embeddings',
    emoji: '🧮',
    accent: 'purple',
    title: 'Multimodal Day Embeddings',
    question: 'What does each day of the trip look like as a point in learned space? Can a model discover types of days that were never explicitly defined?',
    method: 'Tabular autoencoder maps each day\'s feature vector to a 12-dimensional latent space. UMAP projects to 2D. HDBSCAN discovers cluster types without specifying k. The decoder translates cluster centroids back to feature space — optionally labelled by an LLM.',
    viz: 'Scatter plot of all days in 2D space, coloured by discovered cluster. Animated path through space ordered by date — watch your travel rhythm evolve in real time. "Most similar day" lookup: find the day anywhere in the trip that is structurally closest to a chosen date.',
    phase: 'Phase 3',
    libs: 'PyTorch · UMAP · HDBSCAN',
    needs: '200+ days · full daily_summary table',
    aiLinks: [
      { label: 'Multi-Agent Analyst',    anchor: 'multi-agent-analyst' },
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
  {
    id: 'settling-in-curve',
    emoji: '📉',
    accent: 'teal',
    title: 'The Settling-In Curve',
    question: 'When you arrive in a new country, how long does each metric take to return to your personal baseline? Does your adaptation speed improve over the course of the trip?',
    method: 'Exponential decay model fitted per metric per country transition: metric(t) = baseline + disruption × exp(−λt). The decay rate λ quantifies adaptation speed. After 3+ transitions: does λ increase over the trip? That would be evidence you\'re getting better at adapting.',
    viz: 'Small multiples panel: one column per country, one row per metric (sleep, HRV, mood, steps, spend). Each cell shows raw daily values with the fitted curve overlaid. A heatmap of all λ values — countries × metrics — shows your adaptation profile at a glance.',
    phase: 'Phase 2',
    libs: 'scipy · statsmodels · D3',
    needs: '2+ country transitions · daily_summary · country_transitions table',
    aiLinks: [
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
  {
    id: 'destination-signatures',
    emoji: '🗺️',
    accent: 'blue',
    title: 'Destination Behavioural Signatures',
    question: 'How did you actually behave in each place — not how it\'s rated online, but what the data says about movement, spending, sleep, and mood?',
    method: 'Feature vector per destination: movement radius, daily spend, food vs accommodation split, sleep quality, mood valence, exploration rate, workout frequency. Hierarchical clustering groups destinations by behaviour. A personal PPP index — effective cost of living based on what was actually bought — across all countries.',
    viz: 'Radar chart per destination. Heatmap of all destinations × features sorted by cluster. Interactive recommender: given a target state (low budget, want to explore), find the destination with the most similar feature vector.',
    phase: 'Phase 3',
    libs: 'scikit-learn · D3',
    needs: '3+ country legs · daily_summary · transactions · location_unified',
    aiLinks: [
      { label: 'Multi-Agent Analyst',    anchor: 'multi-agent-analyst' },
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
  {
    id: 'transport-mode',
    emoji: '🚶',
    accent: 'red',
    title: 'Transport Mode Archaeology',
    question: 'What fraction of time in each country was on foot, cycling, in vehicles, and on flights? What does total mobility look like across three years?',
    method: 'GPS traces segmented into contiguous journeys by time gaps. Each segment classified by speed profile: walking, cycling, vehicle, train, flight. The flights table provides ground-truth labels for FLIGHT segments. XGBoost trained on weakly labelled segments; retrained monthly as labels accumulate.',
    viz: 'GPS trace animated and coloured by transport mode in Kepler.gl. Stacked bar of time-in-mode per country. "You\'ve walked X,XXX km across Y countries" — a compelling summary stat computed entirely from raw GPS data.',
    phase: 'Phase 1',
    libs: 'XGBoost · scikit-learn · Kepler.gl',
    needs: 'location_unified · flights table · workouts · from day one',
    aiLinks: [
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
];

const TIER2 = [
  {
    id: 'spend-anomaly',
    title: 'Spend Anomaly Detection',
    method: 'IsolationForest + SHAP',
    desc: 'Daily spend anomalies flagged and explained — "this day was anomalous because accommodation was 4× your country average." Feeds Trevor\'s /explain endpoint directly.',
    phase: 'Phase 1',
    aiLinks: [
      { label: 'Morning Briefing',    anchor: 'morning-briefing' },
      { label: 'Multi-Agent Analyst', anchor: 'multi-agent-analyst' },
    ] as AILink[],
  },
  {
    id: 'hmm-segmentation',
    title: 'HMM Travel Phase Segmentation',
    method: 'GaussianHMM',
    desc: 'Unsupervised discovery of behavioural phases across the full trip — "active backpacking", "settled working", "transit/recovery" — without being told what to look for.',
    phase: 'Phase 2',
    aiLinks: [
      { label: 'Morning Briefing',       anchor: 'morning-briefing' },
      { label: 'Field Report Generator', anchor: 'field-report' },
    ] as AILink[],
  },
  {
    id: 'budget-trajectory',
    title: 'Budget Trajectory',
    method: 'Gaussian Process Regression',
    desc: 'GP regression on cumulative spend with calibrated uncertainty bands that widen realistically into the future. Honest about what it doesn\'t know, unlike a trend line.',
    phase: 'Phase 1',
    aiLinks: [
      { label: 'Morning Briefing', anchor: 'morning-briefing' },
    ] as AILink[],
  },
  {
    id: 'sleep-regression',
    title: 'Sleep Quality Regression',
    method: 'Random Forest + SHAP',
    desc: '"What predicts your sleep tonight?" Training load, afternoon café visits (caffeine proxy from transactions), settling_day, and social activity as features. SHAP waterfall per night.',
    phase: 'Phase 1',
  },
  {
    id: 'fitness-trajectory',
    title: 'Fitness Trajectory (ATL/CTL)',
    method: 'Sports science standard',
    desc: 'Acute/Chronic Training Load — the same model used by TrainingPeaks and Garmin. STL decomposition on CTL answers: is the trip making you fitter over time?',
    phase: 'Phase 1',
  },
  {
    id: 'location-clustering',
    title: 'Movement Novelty Score',
    method: 'BallTree spatial index',
    desc: 'For each day, what fraction of GPS points were in locations never visited before? The explorer index: watch it spike every time you enter a new country.',
    phase: 'Phase 1',
    aiLinks: [
      { label: 'Multi-Agent Analyst', anchor: 'multi-agent-analyst' },
    ] as AILink[],
  },
  {
    id: 'circadian-rhythm',
    title: 'Circadian Rhythm Tracking',
    method: 'Two-process sleep model',
    desc: 'Sleep midpoint tracked over time to quantify jet lag recovery per timezone crossing. Does eastward recovery take longer than westward — for you specifically? Your data will say.',
    phase: 'Phase 2',
  },
  {
    id: 'social-pattern',
    title: 'Social vs Solitary Pattern',
    method: 'Transaction classification',
    desc: 'Bar/restaurant/nightlife vs supermarket/café/convenience — a daily social ratio from transaction data. Does social engagement correlate with positive mood? Varies enormously between people.',
    phase: 'Phase 2',
  },
];

const PHASES = [
  {
    phase: 'Phase 0',
    label: 'Before departure',
    timing: 'Now — June 2026',
    items: [
      'Schema defined: daily_summary, location_segments, ml_* tables',
      'Nightly population flow — ATL/CTL, movement entropy',
      'movement_entropy and new_places_count from day one',
      'Pre-departure baseline data collected in London',
    ],
    status: 'active',
  },
  {
    phase: 'Phase 1',
    label: 'Australia baseline',
    timing: 'Months 1–3 post-departure',
    items: [
      'Transport mode classifier (heuristics → XGBoost)',
      'Spend anomaly detection (IsolationForest)',
      'Movement novelty score and ATL/CTL charts',
      'Sleep quality regression (90+ days)',
    ],
    status: 'planned',
  },
  {
    phase: 'Phase 2',
    label: 'First country transition',
    timing: 'After leaving Australia',
    items: [
      'Settling-in curve (exponential decay per metric)',
      'HMM travel phase segmentation (100+ days)',
      'Circadian rhythm and jet lag quantification',
      'Budget trajectory GP regression',
    ],
    status: 'planned',
  },
  {
    phase: 'Phase 3',
    label: 'Mid-trip',
    timing: '6+ months in',
    items: [
      'Day embeddings autoencoder + UMAP (200+ days)',
      'Destination behavioural signatures (3+ legs)',
      'Causal wellbeing graph (PC algorithm)',
      'Social pattern cross-country analysis',
    ],
    status: 'planned',
  },
];

export default function ML() {
  useReveal();

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────── */}
      <section style={{ paddingBottom: 'var(--space-4)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">ML Insights</p>
          <h1 className="section-title reveal" style={{ fontSize: 'clamp(36px, 5vw, 56px)' }}>
            Machine learning on<br />
            <span className="accent-orange">three years of personal data.</span>
          </h1>
          <p className="section-subtitle reveal">
            Every model described here trains on data from the same person it predicts. GPS,
            health, finance, mood, and weather — all from one source, continuously, for up to three years.
          </p>

          <div className="ml-principle reveal">
            The most distinctive thing about this ML work is not the technique — it is that
            the training data and the subject are the same person. Every model gets more accurate
            the longer the trip runs. A causal graph with 300 data points is more robust than
            one with 30. An anomaly detector that has seen your spending in five countries knows
            what is actually unusual for you — not for a population average.
          </div>
        </div>
      </section>

      {/* ── Tier 1 features ───────────────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Portfolio features</p>
          <h2 className="section-title reveal">Five things worth building.</h2>
          <p className="section-subtitle reveal">
            These are the analyses that don&apos;t appear in typical project portfolios — not because
            the techniques are obscure, but because getting here requires two years of continuous
            personal data collection.
          </p>

          <div className="feature-grid" style={{ marginTop: 'var(--space-6)' }}>
            {TIER1.map(f => (
              <div key={f.title} id={f.id} className="feature-card reveal" data-accent={f.accent}>
                <span className="ml-tier-badge ml-tier-badge--t1">Tier 1</span>
                <div className="feature-icon-wrap" data-bg={f.accent}>{f.emoji}</div>
                <h3 className="feature-title">{f.title}</h3>
                <p style={{ fontSize: 14, color: 'var(--accent-orange)', fontStyle: 'italic', marginBottom: 'var(--space-3)', lineHeight: 1.5 }}>
                  &ldquo;{f.question}&rdquo;
                </p>
                <p className="feature-desc" style={{ marginBottom: 'var(--space-3)' }}>{f.method}</p>
                <p style={{ fontSize: 13, color: 'var(--text-tertiary)', lineHeight: 1.5, marginBottom: 'var(--space-3)' }}>
                  <strong style={{ color: 'var(--text-secondary)', fontWeight: 500 }}>Visualisation:</strong> {f.viz}
                </p>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-1)' }}>
                  <span className="feature-tag" style={{ marginBottom: 0 }}>
                    <span style={{ width: 6, height: 6, borderRadius: '50%', background: `var(--accent-${f.accent})`, display: 'inline-block' }}></span>
                    {f.phase} &middot; {f.libs}
                  </span>
                  <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-tertiary)', letterSpacing: '0.04em' }}>
                    {f.needs}
                  </span>
                </div>
                {'aiLinks' in f && f.aiLinks && (
                  <div style={{ marginTop: 'var(--space-3)', paddingTop: 'var(--space-3)', borderTop: '1px solid var(--border)' }}>
                    <span style={{ fontSize: 11, color: 'var(--text-tertiary)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.08em' }}>Used by AI</span>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 'var(--space-2)', marginTop: 'var(--space-2)' }}>
                      {f.aiLinks.map((l: AILink) => (
                        <Link key={l.anchor} to={`/ai#${l.anchor}`} style={{ fontSize: 12, color: 'var(--accent)', textDecoration: 'none', background: 'var(--bg-sunken)', borderRadius: 'var(--radius-pill)', padding: '2px 10px', border: '1px solid var(--border)' }}>
                          → {l.label}
                        </Link>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Tier 2 features ───────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Analytical features</p>
          <h2 className="section-title reveal">And eight more.</h2>
          <p className="section-subtitle reveal">
            Solid, useful analyses that build alongside the Tier 1 work — some starting from the
            first week of the trip, others becoming meaningful after a few country transitions.
          </p>

          <div className="ml-tier2-grid">
            {TIER2.map(f => (
              <div key={f.title} id={f.id} className="ml-tier2-card reveal">
                <div className="ml-tier2-card-top">
                  <span className="ml-tier2-title">{f.title}</span>
                  <span className="ml-tier2-method">{f.method}</span>
                </div>
                <p className="ml-tier2-desc">{f.desc}</p>
                <span className="ml-phase-tag">{f.phase}</span>
                {'aiLinks' in f && f.aiLinks && (
                  <div style={{ marginTop: 'var(--space-2)', paddingTop: 'var(--space-2)', borderTop: '1px solid var(--border)' }}>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 'var(--space-2)' }}>
                      {f.aiLinks.map((l: AILink) => (
                        <Link key={l.anchor} to={`/ai#${l.anchor}`} style={{ fontSize: 11, color: 'var(--accent)', textDecoration: 'none', background: 'var(--bg-sunken)', borderRadius: 'var(--radius-pill)', padding: '2px 8px', border: '1px solid var(--border)' }}>
                          → {l.label}
                        </Link>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── The daily_summary foundation ──────────────────── */}
      <section>
        <div className="section-inner" style={{ maxWidth: 760 }}>
          <p className="section-eyebrow reveal">Foundation</p>
          <h2 className="section-title reveal" style={{ fontSize: 28 }}>One table to rule them all.</h2>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-4)' }} className="reveal">
            Almost every feature above depends on a clean,{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>daily_summary</span>{' '}
            table — one row per day, aggregating GPS movement, health metrics, financial spend, mood,
            and weather into a flat structure. This table must be populated from day one; daily
            aggregates cannot be reliably reconstructed after the fact.
          </p>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            Columns like{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>movement_entropy</span>,{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>new_places_count</span>, and{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>settling_day</span>{' '}
            require the full day&apos;s GPS distribution and country transition history to compute correctly.
            Similarly, ATL/CTL training load is a rolling exponential average — if you start computing
            it mid-trip, earlier values will always be wrong. The schema is defined before departure
            precisely to avoid these problems.
          </p>
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-tertiary)', background: 'var(--bg-sunken)', borderRadius: 'var(--radius-md)', padding: 'var(--space-4) var(--space-5)', lineHeight: 1.8, overflowX: 'auto' }} className="reveal">
            <span style={{ color: 'var(--accent-teal)' }}>-- movement</span>{'\n'}
            steps, distance_m, movement_entropy, new_places_count, time_moving_mins{'\n\n'}
            <span style={{ color: 'var(--accent-red)' }}>-- health</span>{'\n'}
            resting_hr, hrv_avg, sleep_duration_hr, sleep_efficiency, deep_sleep_hr, vo2_max{'\n\n'}
            <span style={{ color: 'var(--accent-orange)' }}>-- finance</span>{'\n'}
            spend_gbp, spend_food_gbp, spend_accommodation_gbp, spend_social_gbp{'\n\n'}
            <span style={{ color: 'var(--accent-purple)' }}>-- mood + journal</span>{'\n'}
            mood_valence, mood_classification, journal_written{'\n\n'}
            <span style={{ color: 'var(--accent)' }}>-- ML outputs (populated later)</span>{'\n'}
            travel_phase, novelty_score, anomaly_score, settling_day, atl, ctl, tsb
          </div>
        </div>
      </section>

      {/* ── Roadmap ───────────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Implementation roadmap</p>
          <h2 className="section-title reveal">Built as the data arrives.</h2>
          <p className="section-subtitle reveal">
            ML models are only as good as the data they train on. Each phase unlocks new analyses
            that weren&apos;t possible before.
          </p>

          <div className="ml-phase-grid">
            {PHASES.map(p => (
              <div key={p.phase} className={`ml-phase-card reveal${p.status === 'active' ? ' ml-phase-card--active' : ''}`}>
                <div className="ml-phase-number">{p.phase}</div>
                <div className="ml-phase-title">{p.label}</div>
                <div className="ml-phase-timing">{p.timing}</div>
                <ul className="ml-phase-items">
                  {p.items.map(item => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
                <span className={`ml-phase-status ml-phase-status--${p.status === 'active' ? 'active' : 'planned'}`}>
                  {p.status === 'active' ? '⬤ underway' : '◌ planned'}
                </span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── AI cross-link ─────────────────────────────────── */}
      <section>
        <div className="section-inner" style={{ textAlign: 'center', maxWidth: 600 }}>
          <p className="section-eyebrow reveal">AI agents</p>
          <h2 className="section-title reveal" style={{ fontSize: 28 }}>ML outputs feed AI agents.</h2>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            Some ML outputs feed directly into TravelNet&apos;s AI agents — precomputed anomaly flags,
            destination profiles, and phase labels consumed at agent runtime rather than computed on demand.
          </p>
          <Link to="/ai" className="btn btn-primary reveal" style={{ display: 'inline-flex' }}>
            View AI Features →
          </Link>
        </div>
      </section>

      {/* ── Libraries ─────────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner" style={{ maxWidth: 760 }}>
          <p className="section-eyebrow reveal">Libraries</p>
          <h2 className="section-title reveal" style={{ fontSize: 28 }}>The Python stack</h2>

          <div className="stack-grid" style={{ marginTop: 'var(--space-5)' }}>
            <div className="stack-item reveal"><span className="stack-item-icon">🔗</span>causallearn — PC algorithm, DAG learning</div>
            <div className="stack-item reveal"><span className="stack-item-icon">🧮</span>PyTorch — tabular autoencoder</div>
            <div className="stack-item reveal"><span className="stack-item-icon">📉</span>UMAP + HDBSCAN — embedding projection</div>
            <div className="stack-item reveal"><span className="stack-item-icon">🎯</span>hmmlearn — GaussianHMM segmentation</div>
            <div className="stack-item reveal"><span className="stack-item-icon">🔍</span>scikit-learn — IsolationForest, RF, GP</div>
            <div className="stack-item reveal"><span className="stack-item-icon">💡</span>shap — TreeExplainer for all tree models</div>
            <div className="stack-item reveal"><span className="stack-item-icon">📊</span>statsmodels — STL, structural time series</div>
            <div className="stack-item reveal"><span className="stack-item-icon">⚡</span>XGBoost — transport mode classifier</div>
            <div className="stack-item reveal"><span className="stack-item-icon">📈</span>D3 + Kepler.gl — all visualisations</div>
          </div>
        </div>
      </section>
    </>
  );
}
