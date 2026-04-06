import { useEffect } from 'react';
import { LEGS } from '../data/travel';

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

const BUILD_DONE = [
  {
    title: 'Data Ingest API',
    text: 'FastAPI service running on a Raspberry Pi — ingests GPS tracks from Overland and iOS Shortcuts, health metrics from Health Auto Export, and transaction CSVs from Revolut and Wise.',
    tag: 'live',
  },
  {
    title: 'Admin Dashboard',
    text: 'Internal Flask dashboard for monitoring ingest health, cron jobs, database state, logs, and configuration. Runs alongside the API on the same Pi.',
    tag: 'live',
  },
  {
    title: 'Location Pipeline',
    text: 'Unified location view merging two GPS sources, with geocoding and DBSCAN-based place clustering to identify meaningful stops from raw GPS tracks.',
    tag: 'live',
  },
  {
    title: 'Scheduled Tasks',
    text: 'Cron-driven jobs for FX rate fetching, DB backups, health gap detection, GBP normalisation, and a daily error digest — all with email reporting.',
    tag: 'live',
  },
  {
    title: 'Trevor (Architecture)',
    text: 'RAG-based conversational AI designed to query TravelNet data. Tool-calling retrieval, journal ingestion pipeline, and multi-provider LLM support are all designed and partially built.',
    tag: 'in progress',
  },
];

const BUILD_COMING = [
  {
    title: 'ML Pipeline',
    text: 'HMM-based travel leg segmentation, DBSCAN cluster labelling, spending pattern detection, and anomaly flagging — precomputed and persisted to the DB for fast query-time access.',
  },
  {
    title: 'Trevor — Full Deployment',
    text: 'Journal ingestion from Apple Journal exports, ChromaDB vector store population, and live /chat endpoint — queryable once real trip data starts flowing in mid-2026.',
  },
  {
    title: 'Public Explorer',
    text: 'An interactive data explorer on this site letting visitors drill into GPS tracks, health trends, and spending by leg — built on real collected data, not placeholders.',
  },
  {
    title: 'Anomaly Explainer',
    text: 'TravelNet detects a statistical anomaly in the data stream and calls Trevor to explain it in plain English using the surrounding journal context.',
  },
];

export default function Journey() {
  useReveal();

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────── */}
      <section style={{ paddingBottom: 'var(--space-8)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">2026 – 2029</p>
          <h1 className="section-title reveal" style={{ fontSize: 'clamp(40px, 6vw, 64px)' }}>
            Three years.<br />Three continents.
          </h1>
          <p className="section-subtitle reveal">
            In June 2026 I'm leaving the UK to spend roughly three years travelling — working
            holidays in Australia, New Zealand, and Canada, backpacking through Southeast Asia,
            and a summer camp stint in the US. TravelNet exists to make sure none of that data
            disappears into thin air.
          </p>
        </div>
      </section>

      {/* ── Route timeline ────────────────────────────────── */}
      <div className="timeline">
        <div className="section-inner">
          <p className="section-eyebrow reveal">The route</p>
          <h2 className="section-title reveal">Where I'm going.</h2>
        </div>
        <div className="timeline-track" style={{ maxWidth: 1200, margin: '0 auto', padding: 'var(--space-6) var(--space-6)' }}>
          {LEGS.map(leg => (
            <div key={leg.id} className="timeline-leg">
              <div className="timeline-dot">{leg.emoji}</div>
              <div className="timeline-name">{leg.name}</div>
              {leg.stopover && (
                <span style={{
                  fontFamily: 'var(--font-mono)',
                  fontSize: 9,
                  letterSpacing: '0.08em',
                  textTransform: 'uppercase',
                  color: 'var(--text-tertiary)',
                }}>stopover</span>
              )}
            </div>
          ))}
        </div>

        {/* Leg detail cards */}
        <div className="section-inner">
          <div className="journey-leg-grid">
            {LEGS.filter(l => !l.stopover).map((leg, i) => (
              <div key={leg.id} className="journey-leg-card reveal">
                <div className="journey-leg-card-header">
                  <span className="journey-leg-emoji">{leg.emoji}</span>
                  <div>
                    <div className="journey-leg-name">{leg.name}</div>
                    <div className="journey-leg-num">Leg {i + 1}</div>
                  </div>
                </div>
                <p className="journey-leg-desc">
                  {leg.id === 'usa' && 'A summer camp lifeguarding and counselling stint on a J-1 visa — Philadelphia and DC to start, a Seattle send-off at the end.'}
                  {leg.id === 'australia' && 'First working holiday visa. A year across Australia — the east coast, the outback, whatever comes up. The ML pipeline kicks off here using US data as baseline.'}
                  {leg.id === 'new_zealand' && 'Second working holiday. New Zealand for the better part of eighteen months — slower pace, more hiking, Trevor starts getting real data to work with.'}
                  {leg.id === 'se_asia' && 'Three to four months backpacking the Banana Pancake Trail. Countries TBC, but Thailand, Vietnam, and Cambodia are likely starting points.'}
                  {leg.id === 'canada' && 'Final working holiday. Open-ended end date — whenever the trip feels done. The complete dataset hands off to Trevor for a permanent conversational record.'}
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── Building journey ──────────────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">The build</p>
          <h2 className="section-title reveal">Building while travelling.</h2>
          <p className="section-subtitle reveal">
            TravelNet and Trevor are built alongside the trip — not before it, not after.
            The infrastructure had to be solid enough to run unattended on a Raspberry Pi
            for three years before I left.
          </p>

          <div style={{ marginTop: 'var(--space-7)' }}>
            <p style={{
              fontFamily: 'var(--font-mono)',
              fontSize: 11,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'var(--text-tertiary)',
              marginBottom: 'var(--space-4)',
            }}>So far</p>
            <div className="journey-build-grid">
              {BUILD_DONE.map(({ title, text, tag }) => (
                <div key={title} className="journey-build-card reveal">
                  <div className="journey-build-card-top">
                    <span className="journey-build-title">{title}</span>
                    <span className={`journey-build-tag journey-build-tag--${tag === 'live' ? 'live' : 'wip'}`}>
                      {tag === 'live' ? '⬤ live' : '◌ in progress'}
                    </span>
                  </div>
                  <p className="journey-build-text">{text}</p>
                </div>
              ))}
            </div>
          </div>

          <div style={{ marginTop: 'var(--space-7)' }}>
            <p style={{
              fontFamily: 'var(--font-mono)',
              fontSize: 11,
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'var(--text-tertiary)',
              marginBottom: 'var(--space-4)',
            }}>What's coming</p>
            <div className="journey-build-grid">
              {BUILD_COMING.map(({ title, text }) => (
                <div key={title} className="journey-build-card journey-build-card--future reveal">
                  <div className="journey-build-card-top">
                    <span className="journey-build-title">{title}</span>
                  </div>
                  <p className="journey-build-text">{text}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>
    </>
  );
}
