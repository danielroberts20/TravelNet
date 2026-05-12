import { useEffect } from 'react';
import { Link } from 'react-router-dom';

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

function InDesignBadge() {
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 6,
      background: 'var(--accent-orange)', color: 'white',
      borderRadius: 'var(--radius-pill)', padding: '3px 12px',
      fontSize: '0.75rem', fontWeight: 600,
    }}>
      In Design
    </span>
  );
}

function TechTable({ rows }: { rows: [string, string][] }) {
  return (
    <div style={{
      display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '1px',
      background: 'var(--border)', borderRadius: 'var(--radius-md)', overflow: 'hidden',
      marginTop: 'var(--space-4)',
    }}>
      {rows.map(([label, detail]) => (
        <div key={label} style={{ display: 'contents' }}>
          <div style={{
            background: 'var(--bg-sunken)', padding: 'var(--space-2) var(--space-4)',
            fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-secondary)',
            fontWeight: 600, whiteSpace: 'nowrap',
          }}>
            {label}
          </div>
          <div style={{
            background: 'var(--surface)', padding: 'var(--space-2) var(--space-4)',
            fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.5,
          }}>
            {detail}
          </div>
        </div>
      ))}
    </div>
  );
}

function AgentGraph({ lines }: { lines: string }) {
  return (
    <div style={{
      fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-secondary)',
      background: 'var(--bg-sunken)', borderRadius: 'var(--radius-md)',
      padding: 'var(--space-4) var(--space-5)', lineHeight: 1.8,
      overflowX: 'auto', marginTop: 'var(--space-4)', whiteSpace: 'pre',
    }}>
      {lines}
    </div>
  );
}

function MLLinks({ links }: { links: { label: string; anchor: string }[] }) {
  return (
    <div style={{
      marginTop: 'var(--space-4)', paddingTop: 'var(--space-4)',
      borderTop: '1px solid var(--border)',
    }}>
      <span style={{
        fontSize: 11, color: 'var(--text-tertiary)', fontWeight: 600,
        textTransform: 'uppercase', letterSpacing: '0.08em',
      }}>
        Draws from ML
      </span>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 'var(--space-2)', marginTop: 'var(--space-2)' }}>
        {links.map(l => (
          <Link
            key={l.anchor}
            to={`/ml#${l.anchor}`}
            style={{
              fontSize: 12, color: 'var(--accent-purple)', textDecoration: 'none',
              background: 'var(--bg-sunken)', borderRadius: 'var(--radius-pill)',
              padding: '2px 10px', border: '1px solid var(--border)',
            }}
          >
            → {l.label}
          </Link>
        ))}
      </div>
    </div>
  );
}

export default function AI() {
  useReveal();

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────── */}
      <section style={{ paddingBottom: 'var(--space-4)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">AI Features</p>
          <h1 className="section-title reveal" style={{ fontSize: 'clamp(36px, 5vw, 56px)' }}>
            Intelligent Agents.
          </h1>
          <p className="section-subtitle reveal">
            Four AI systems that observe, reason, and act on top of TravelNet&apos;s data pipeline.
          </p>
          <div className="ml-principle reveal">
            Built with LangGraph, LangChain, and the Anthropic/OpenAI APIs. All agents run on the
            Raspberry Pi or a GPU compute box — not in the browser. The site surfaces their outputs;
            the agents themselves operate server-side on a schedule or in response to data events.
          </div>
        </div>
      </section>

      {/* ── Morning Briefing ──────────────────────────────── */}
      <section id="morning-briefing" style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <div className="reveal" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', marginBottom: 'var(--space-4)' }}>
            <InDesignBadge />
          </div>
          <p className="section-eyebrow reveal">Agent 1 of 4</p>
          <h2 className="section-title reveal">Morning Briefing Agent</h2>
          <p style={{ color: 'var(--accent-orange)', fontStyle: 'italic', marginBottom: 'var(--space-5)' }} className="reveal">
            &ldquo;A daily digest prepared before you wake up.&rdquo;
          </p>

          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-4)' }} className="reveal">
            Every morning at 08:00 local time, a Prefect-scheduled flow queries TravelNet&apos;s database
            across five domains — health, movement, spending, weather, and pipeline health — and calls
            an LLM to synthesise the results into a structured daily briefing. The output is stored in
            a <span className="mono" style={{ fontSize: '0.9em' }}>morning_briefing</span> table and served via a public FastAPI endpoint.
          </p>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            The briefing is designed to be consumed as an iOS home screen widget built in Scriptable —
            a free app that renders JavaScript widgets from URL-fetched JSON. Because the endpoint lives
            on <span className="mono" style={{ fontSize: '0.9em' }}>public.travelnet.dev</span> via
            Cloudflare Tunnel, the widget works seamlessly alongside NordVPN on iOS without requiring
            any VPN reconfiguration.
          </p>

          <div className="reveal">
            <TechTable rows={[
              ['Scheduler', 'Prefect flow, CronSchedule("0 8 * * *"), local timezone'],
              ['LLM',       'GPT-4o-mini, structured output via Pydantic model'],
              ['Output',    'morning_briefing table + GET /public/briefing/today'],
              ['iOS',       'Scriptable widget, fetches JSON, renders inline'],
            ]} />
          </div>

          <h3 style={{ marginTop: 'var(--space-6)', marginBottom: 'var(--space-3)', fontSize: 16, fontWeight: 600, color: 'var(--text-primary)' }} className="reveal">
            What the briefing contains
          </h3>
          <ul style={{ color: 'var(--text-secondary)', lineHeight: 1.8, paddingLeft: 'var(--space-5)', marginBottom: 'var(--space-5)' }} className="reveal">
            <li>Yesterday&apos;s health snapshot: steps, HRV, sleep efficiency, resting heart rate</li>
            <li>Spend vs daily budget target for the current country</li>
            <li>Weather today at the current location</li>
            <li>Any anomaly flags raised by the prior night&apos;s ML run</li>
            <li>One LLM-generated insight over the trailing 7 days of <span className="mono" style={{ fontSize: '0.9em' }}>daily_summary</span> — not templated</li>
          </ul>

          <div className="feature-card reveal" style={{ marginBottom: 0 }}>
            <p style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-3)' }}>
              <strong style={{ color: 'var(--text-primary)' }}>The insight step is where the LLM earns its place.</strong>{' '}
              The agent is given the last 7 rows of <span className="mono" style={{ fontSize: '0.9em' }}>daily_summary</span> and
              asked: &ldquo;What pattern is worth mentioning today that wouldn&apos;t be obvious from glancing at the numbers?&rdquo;
              The response is constrained to 2 sentences maximum via the Pydantic structured output model. No templates,
              no hardcoded insights — the model decides what&apos;s worth surfacing each morning.
            </p>
          </div>

          <div className="reveal">
            <MLLinks links={[
              { label: 'Spend Anomaly Detection', anchor: 'spend-anomaly' },
              { label: 'Budget Trajectory GP',    anchor: 'budget-trajectory' },
              { label: 'HMM Travel Phase Segmentation', anchor: 'hmm-segmentation' },
            ]} />
          </div>
        </div>
      </section>

      {/* ── Multi-Agent Analyst ───────────────────────────── */}
      <section id="multi-agent-analyst">
        <div className="section-inner">
          <div className="reveal" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', marginBottom: 'var(--space-4)' }}>
            <InDesignBadge />
          </div>
          <p className="section-eyebrow reveal">Agent 2 of 4</p>
          <h2 className="section-title reveal">Multi-Agent Travel Analyst</h2>
          <p style={{ color: 'var(--accent-orange)', fontStyle: 'italic', marginBottom: 'var(--space-5)' }} className="reveal">
            &ldquo;Specialist agents working in parallel to answer questions no single query could.&rdquo;
          </p>

          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-4)' }} className="reveal">
            Trevor handles straightforward queries well. But some questions are genuinely cross-domain:
            &ldquo;Was Melbourne my most expensive city, adjusted for what I was actually doing there?&rdquo; requires
            reasoning over spend, activity type, location clusters, and ML destination profiles simultaneously.
          </p>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            The Multi-Agent Analyst is a LangGraph <span className="mono" style={{ fontSize: '0.9em' }}>StateGraph</span> that
            Trevor can escalate complex queries to. An orchestrator agent reads the query, decomposes it, and dispatches
            to specialist sub-agents that run in parallel. Results are collected and synthesised into a single coherent answer.
          </p>

          <div className="reveal">
            <AgentGraph lines={`User query (via Trevor)
        ↓
  Orchestrator Agent
  (decomposes query, selects relevant sub-agents)
        ↓ (parallel dispatch via LangGraph send())
┌──────────────────┬──────────────────┬──────────────────┐
│   BudgetAgent    │  ActivityAgent   │   HealthAgent    │
│  (transactions,  │  (workouts,      │  (daily_summary, │
│   CoL norm,      │   movement,      │   HRV, sleep,    │
│   spend cols)    │   location       │   training load) │
│                  │   clusters)      │                  │
└──────────────────┴──────────────────┴──────────────────┘
        ↓ (collect)
  Narrative Agent
  (synthesises sub-agent outputs into final answer)
        ↓
  Trevor response`} />
          </div>

          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginTop: 'var(--space-5)', marginBottom: 'var(--space-5)' }} className="reveal">
            Each sub-agent is constrained to tool calls only — it cannot speculate without querying data.
            This prevents hallucination by design: if the data doesn&apos;t exist, the agent returns{' '}
            <span className="mono" style={{ fontSize: '0.9em' }}>null</span> for that domain rather than
            fabricating an answer. The narrative agent is only as confident as the data it receives.
          </p>

          <div className="reveal">
            <TechTable rows={[
              ['Orchestration', 'LangGraph StateGraph with typed AgentState'],
              ['Parallelism',   'LangGraph send() API for concurrent sub-agent execution'],
              ['Tools',         'SQLite query tools per domain, read-only'],
              ['LLM',           'Claude Haiku 3.5 for sub-agents (speed), Claude Sonnet for narrative'],
              ['Integration',   'Trevor /analyse endpoint triggers the graph'],
            ]} />
          </div>

          <div className="reveal">
            <MLLinks links={[
              { label: 'Causal Wellbeing Graph',           anchor: 'causal-wellbeing-graph' },
              { label: 'Day Embeddings',                   anchor: 'day-embeddings' },
              { label: 'Destination Behavioural Signatures', anchor: 'destination-signatures' },
              { label: 'Spend Anomaly Detection',          anchor: 'spend-anomaly' },
              { label: 'Movement Novelty Score',           anchor: 'location-clustering' },
            ]} />
          </div>
        </div>
      </section>

      {/* ── Self-Healing Pipeline ─────────────────────────── */}
      <section id="self-healing" style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <div className="reveal" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', marginBottom: 'var(--space-4)' }}>
            <InDesignBadge />
          </div>
          <p className="section-eyebrow reveal">Agent 3 of 4</p>
          <h2 className="section-title reveal">Self-Healing Pipeline</h2>
          <p style={{ color: 'var(--accent-orange)', fontStyle: 'italic', marginBottom: 'var(--space-5)' }} className="reveal">
            &ldquo;A nightly agent that detects data quality issues and resolves what it can.&rdquo;
          </p>

          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            TravelNet runs unattended for 2–3 years across unreliable mobile networks and time zones.
            A nightly LangGraph agent checks every domain of the pipeline for integrity issues, classifies
            each as auto-fixable or alert-only, and acts accordingly — re-triggering failed flows,
            backfilling gaps, and logging everything to the morning briefing&apos;s system health section.
          </p>

          <div className="reveal" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-4)', marginBottom: 'var(--space-5)' }}>
            <div className="feature-card" style={{ margin: 0 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: 'var(--accent-teal)', marginBottom: 'var(--space-3)' }}>Auto-fixable</h3>
              <ul style={{ color: 'var(--text-secondary)', lineHeight: 1.8, paddingLeft: 'var(--space-4)', margin: 0, fontSize: 13 }}>
                <li>FX rate gaps</li>
                <li>Transaction classification NULLs after flow ran</li>
                <li>Daily summary domain incomplete after 24 hours</li>
                <li>Geocoding rows missing coordinates</li>
              </ul>
            </div>
            <div className="feature-card" style={{ margin: 0 }}>
              <h3 style={{ fontSize: 14, fontWeight: 600, color: 'var(--accent-red)', marginBottom: 'var(--space-3)' }}>Alert only</h3>
              <ul style={{ color: 'var(--text-secondary)', lineHeight: 1.8, paddingLeft: 'var(--space-4)', margin: 0, fontSize: 13 }}>
                <li>Overland GPS gap &gt; 12 hours</li>
                <li>R2 backup missed expected window</li>
                <li>DB size exceeding threshold</li>
                <li>Cloudflare Tunnel health failure</li>
              </ul>
            </div>
          </div>

          <div className="reveal">
            <AgentGraph lines={`Nightly trigger (03:30 UTC)
        ↓
  Run Checks (parallel tool calls per domain)
        ↓
  Triage Node
  (classify each issue: fixable / alert-only / clean)
        ↓ (conditional edges)
┌──────────────────┬──────────────────┬──────────────┐
│   Repair Nodes   │   Alert Node     │  Done Node   │
│ (one per fix     │  (Pushcut +      │  (if clean)  │
│  type)           │   email)         │              │
└──────────────────┴──────────────────┴──────────────┘
        ↓
  Write Health Report
  → appends system_health section to morning_briefing JSON`} />
          </div>

          <div className="feature-card reveal" style={{ marginTop: 'var(--space-5)', marginBottom: 0 }}>
            <p style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.7, margin: 0 }}>
              <strong style={{ color: 'var(--text-primary)' }}>Why LangGraph here:</strong>{' '}
              The repair action depends on what the check found. Conditional edges based on typed state —
              exactly the pattern LangGraph&apos;s graph model is designed for. A simple Prefect flow
              with if/else would work for 2 cases; at 8+ issue types with branching logic, the graph
              model is cleaner and easier to extend.
            </p>
          </div>
        </div>
      </section>

      {/* ── Field Report Generator ────────────────────────── */}
      <section id="field-report">
        <div className="section-inner">
          <div className="reveal" style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-3)', marginBottom: 'var(--space-4)' }}>
            <InDesignBadge />
          </div>
          <p className="section-eyebrow reveal">Agent 4 of 4</p>
          <h2 className="section-title reveal">Field Report Generator</h2>
          <p style={{ color: 'var(--accent-orange)', fontStyle: 'italic', marginBottom: 'var(--space-5)' }} className="reveal">
            &ldquo;When you leave a country, the system writes the story of your time there.&rdquo;
          </p>

          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-4)' }} className="reveal">
            When <span className="mono" style={{ fontSize: '0.9em' }}>travel.yml</span> records a departure
            from a country leg, a Prefect flow triggers an agent that compiles a structured retrospective
            of that entire leg and publishes it to the demo site. The site writes itself, from real data,
            without any manual authorship.
          </p>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            The agent combines four data sources to produce a report no template could generate: structured
            statistics from <span className="mono" style={{ fontSize: '0.9em' }}>daily_summary</span>, ML
            outputs (anomaly flags, cluster labels, destination profile, settling curve), journal entries
            retrieved semantically via Trevor&apos;s RAG pipeline, and an LLM synthesis pass that finds
            the one thing the numbers don&apos;t tell you.
          </p>

          <h3 style={{ fontSize: 16, fontWeight: 600, marginBottom: 'var(--space-3)', color: 'var(--text-primary)' }} className="reveal">
            Report structure
          </h3>
          <div className="reveal" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--space-3)', marginBottom: 'var(--space-5)' }}>
            {[
              { section: 'Overview',              source: 'daily_summary aggregate stats: days, spend, distance, avg HRV' },
              { section: 'Highlights',            source: 'Anomaly flags + LLM narration of the most significant days' },
              { section: 'Health & Fitness',      source: 'ATL/CTL trend, sleep efficiency trajectory, settling curve' },
              { section: 'Spending',              source: 'Category breakdown, CoL-normalised daily rate, budget vs actual' },
              { section: 'What the Data Missed',  source: 'LLM synthesis over journal entries via Trevor RAG' },
              { section: 'One Unexpected Thing',  source: 'LLM prompt: "Find something in this data I wouldn\'t have noticed"' },
            ].map(row => (
              <div key={row.section} style={{
                background: 'var(--bg-sunken)', borderRadius: 'var(--radius-md)',
                padding: 'var(--space-3) var(--space-4)', border: '1px solid var(--border)',
              }}>
                <div style={{ fontWeight: 600, fontSize: 13, color: 'var(--text-primary)', marginBottom: 4 }}>{row.section}</div>
                <div style={{ fontSize: 12, color: 'var(--text-tertiary)', lineHeight: 1.5 }}>{row.source}</div>
              </div>
            ))}
          </div>

          <div className="reveal">
            <TechTable rows={[
              ['Trigger',         'Prefect flow watching travel.yml for leg transitions'],
              ['Data',            'daily_summary date range query for the full leg'],
              ['ML context',      'ml_anomalies, ml_destination_profiles, settling_day from daily_summary'],
              ['Journal',         'Trevor /search endpoint — semantic search over Chroma vector store'],
              ['Generation',      'Claude Sonnet (long context, structured output via Pydantic)'],
              ['Storage',         'field_reports table (leg_id PK, country_code, report_json, generated_at)'],
              ['Public API',      'GET /public/field-reports/{leg_id}'],
              ['Frontend',        'This demo site renders field reports as they become available'],
            ]} />
          </div>

          <div className="feature-card reveal" style={{ marginTop: 'var(--space-5)' }}>
            <p style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 0 }}>
              <strong style={{ color: 'var(--text-primary)' }}>The &ldquo;What the Data Missed&rdquo; section</strong>{' '}
              is the most technically interesting: the agent retrieves the 10 most relevant journal
              entries for the leg period using semantic search — not keyword search — passes them
              alongside the structured stats to the LLM, and asks it to find moments the data doesn&apos;t
              capture. A conversation that changed something. A place that didn&apos;t register as significant
              in the GPS trace but clearly mattered in writing. Structured data and unstructured text
              are cross-referenced at generation time.
            </p>
          </div>

          <div className="reveal">
            <MLLinks links={[
              { label: 'Causal Wellbeing Graph',             anchor: 'causal-wellbeing-graph' },
              { label: 'Day Embeddings',                     anchor: 'day-embeddings' },
              { label: 'Settling-In Curve',                  anchor: 'settling-in-curve' },
              { label: 'Destination Behavioural Signatures', anchor: 'destination-signatures' },
              { label: 'Transport Mode Archaeology',         anchor: 'transport-mode' },
              { label: 'Spend Anomaly Detection',            anchor: 'spend-anomaly' },
              { label: 'HMM Travel Phase Segmentation',      anchor: 'hmm-segmentation' },
            ]} />
          </div>
        </div>
      </section>

      {/* ── Footer cross-link ─────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner" style={{ textAlign: 'center', maxWidth: 600 }}>
          <p className="section-eyebrow reveal">ML foundation</p>
          <h2 className="section-title reveal" style={{ fontSize: 28 }}>Built on top of the ML layer.</h2>
          <p style={{ color: 'var(--text-secondary)', lineHeight: 1.7, marginBottom: 'var(--space-5)' }} className="reveal">
            These agents draw from TravelNet&apos;s ML analysis layer — precomputed outputs stored
            in the database and consumed at agent runtime. The ML models run first; the agents reason
            over what they find.
          </p>
          <Link
            to="/ml"
            className="btn btn-primary reveal"
            style={{ display: 'inline-flex' }}
          >
            View ML Insights →
          </Link>
        </div>
      </section>
    </>
  );
}
