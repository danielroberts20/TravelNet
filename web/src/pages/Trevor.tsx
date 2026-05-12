import { useEffect } from 'react';
import { Link } from 'react-router-dom';
import { TREVOR_REPO, GITHUB_REPO } from '../data/travel';

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

const GITHUB_SVG = (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
    <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
  </svg>
);

const EXAMPLE_QUERIES = [
  { q: 'Summarise how I felt during Southeast Asia',           tag: 'journal'       },
  { q: 'Did my step count drop in weeks I overspent?',        tag: 'cross-stream'  },
  { q: 'What triggered the spending spike in week 14?',       tag: 'anomaly'       },
  { q: 'Which country had the highest average mood?',         tag: 'journal + db'  },
  { q: 'How did my sleep change after leaving Japan?',        tag: 'health'        },
  { q: 'What was I spending most on when least active?',      tag: 'cross-stream'  },
];

const ARCH_STEPS = [
  {
    icon: '💬',
    label: 'search_journal',
    title: 'Journal Search',
    text: 'Semantic search over Apple Journal entries embedded in a vector store, enriched with GPS, mood, and HealthKit metadata.',
  },
  {
    icon: '🗄️',
    label: 'query_db',
    title: 'Structured Query',
    text: 'Text-to-SQL against TravelNet\'s database — precise answers on spending, steps, sleep, location history, and ML model outputs.',
  },
  {
    icon: '🔀',
    label: 'hybrid',
    title: 'Hybrid Retrieval',
    text: 'Both tools in a single turn for cross-stream questions. The LLM decides what to call — no hardcoded router.',
  },
];

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
          }}>{label}</div>
          <div style={{
            background: 'var(--surface)', padding: 'var(--space-2) var(--space-4)',
            fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.5,
          }}>{detail}</div>
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
      overflowX: 'auto', marginTop: 'var(--space-6)', whiteSpace: 'pre',
    }}>{lines}</div>
  );
}

const RAG_CARDS = [
  {
    icon: '🔍',
    label: 'retrieval',
    title: 'Retrieval',
    text: 'Journal entries are embedded at ingestion time and stored in a Chroma vector store. When a query arrives, Trevor performs semantic search to retrieve the most relevant entries — matching by meaning, not keywords. Structured data (health, spend, location, weather) is retrieved via SQL tool calls against TravelNet\'s SQLite database.',
  },
  {
    icon: '🔀',
    label: 'orchestration',
    title: 'Orchestration',
    text: 'Trevor\'s LLM decides which tools to call based on the query. For a question about a specific day it might call one SQL tool. For a complex cross-domain question it might call several — or delegate to a specialist sub-agent. The number of steps is not fixed; the model determines the path at runtime.',
  },
  {
    icon: '✦',
    label: 'generation',
    title: 'Generation',
    text: 'Once Trevor has retrieved sufficient context, it generates an answer grounded in the actual data. Responses cite specific dates, amounts, and journal excerpts rather than generalisations. If the data doesn\'t support a confident answer, Trevor says so.',
  },
];

const AGENT_DIAGRAM = `User query
     ↓
Trevor (orchestrator agent)
     ├── Semantic search  →  Chroma vector store (journal entries)
     ├── SQL tools        →  TravelNet SQLite (health, spend, location, weather)
     └── Sub-agent tools (when deep domain reasoning needed)
           ├── BudgetAgent   — spending, CoL normalisation, budget trajectory
           ├── HealthAgent   — HRV, sleep, training load, recovery
           └── ActivityAgent — movement, location clusters, transport modes
     ↓
Synthesised answer grounded in retrieved data`;

const TECH_STACK: [string, string][] = [
  ['Vector store',       'Chroma (persistent, Docker volume)'],
  ['Embeddings',         'OpenAI text-embedding-3-small'],
  ['LLM',               'GPT-4o-mini (swappable — Haiku 3.5 planned)'],
  ['Framework',         'FastAPI, LangChain tool-calling'],
  ['Sub-agents',        'LangGraph StateGraph (planned)'],
  ['Data access',       'Direct SQLite read-only bind mount'],
  ['Ingestion trigger', 'iOS Shortcut → TravelNet API → Trevor pipeline'],
];

const FEATURES = [
  {
    icon: '💬',
    title: 'Journal Querying',
    text: 'Ask questions about daily entries, moods, and places — semantic search across thousands of journal chunks enriched with GPS and HealthKit metadata.',
  },
  {
    icon: '🔗',
    title: 'Cross-Stream Correlation',
    text: '"Did my step count drop in weeks I overspent?" Trevor joins structured telemetry with narrative journal context to answer questions no single dataset could.',
  },
  {
    icon: '🚨',
    title: 'Anomaly Explainer',
    text: 'TravelNet flags a statistical anomaly in the data. Trevor pulls surrounding journal context and returns a plain-English explanation of what likely happened.',
  },
];

export default function Trevor() {
  useReveal();

  return (
    <>
      {/* ── Hero ──────────────────────────────────────────── */}
      <section className="trevor-page-hero">
        <div className="trevor-page-glow" />

        <div className="trevor-page-avatar-lg reveal">✦</div>

        <div className="trevor-page-badge reveal">
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: 'var(--accent-purple)', display: 'inline-block' }} />
          Coming soon
        </div>

        <h1 className="trevor-page-title reveal">Trevor</h1>

        <p className="trevor-page-subtitle reveal">
          A RAG-based conversational AI that lets you have a conversation with three years of
          personal travel data &mdash; GPS tracks, health metrics, spending logs, and daily journal entries.
        </p>

        <div className="trevor-page-ctas reveal">
          <a href={TREVOR_REPO} className="btn btn-primary" target="_blank" rel="noopener">
            {GITHUB_SVG}
            Trevor on GitHub
          </a>
          <a href={GITHUB_REPO} className="btn btn-secondary" target="_blank" rel="noopener">
            {GITHUB_SVG}
            TravelNet on GitHub
          </a>
        </div>
      </section>

      {/* ── Example queries ───────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">What you can ask</p>
          <h2 className="section-title reveal">Ask anything.</h2>
          <p className="section-subtitle reveal">
            Trevor grounds every response in retrieved data — journal entries, structured telemetry,
            or both. No guessing. No fabrication.
          </p>

          <div className="trevor-queries-grid">
            {EXAMPLE_QUERIES.map(({ q, tag }) => (
              <div key={q} className="trevor-query-card reveal">
                <span className="trevor-query-card-tag">{tag}</span>
                <p className="trevor-query-card-text">&ldquo;{q}&rdquo;</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Mock chat ─────────────────────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Preview</p>
          <h2 className="section-title reveal">See it in action.</h2>
          <p className="section-subtitle reveal">
            A snapshot of the kind of conversation Trevor enables once deployed.
          </p>

          <div className="trevor-page-chat-wide reveal">
            <div className="trevor-chat-chrome">
              <div className="trevor-avatar-sm">✦</div>
              <span className="trevor-chat-chrome-name">Trevor</span>
              <span className="chrome-label mono" style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--text-tertiary)' }}>TravelNet Assistant</span>
            </div>
            <div className="trevor-chat-messages" style={{ paddingBottom: 'var(--space-9)' }}>
              <div className="trevor-msg trevor-msg-user">
                <span className="trevor-msg-label">You</span>
                <div className="trevor-msg-bubble">Summarise how I felt during Southeast Asia</div>
              </div>
              <div className="trevor-msg trevor-msg-ai">
                <span className="trevor-msg-label">Trevor</span>
                <div className="trevor-msg-bubble">
                  Across 47 journal entries from Thailand, Vietnam, and Cambodia your average valence
                  was +0.34 — meaningfully above your overall trip baseline of +0.19. The highest-rated
                  days clustered around slow travel weeks with low spending and high step counts.
                </div>
              </div>
              <div className="trevor-msg trevor-msg-user">
                <span className="trevor-msg-label">You</span>
                <div className="trevor-msg-bubble">What caused the spending spike in week 14?</div>
              </div>
              <div className="trevor-msg trevor-msg-ai">
                <span className="trevor-msg-label">Trevor</span>
                <div className="trevor-msg-bubble">
                  TravelNet flagged a 3.1σ spending outlier that week. Your entry from that day
                  mentions an unexpected flight rebook and two unplanned nights in a new city —
                  consistent with the transaction breakdown showing two large transport charges.
                </div>
              </div>
              <div className="trevor-msg trevor-msg-user">
                <span className="trevor-msg-label">You</span>
                <div className="trevor-msg-bubble">Did my step count drop in weeks I overspent?</div>
              </div>
              <div className="trevor-msg trevor-msg-ai">
                <span className="trevor-msg-label">Trevor</span>
                <div className="trevor-msg-bubble">
                  Yes — there's a moderate negative correlation (r = -0.41) between weekly spend
                  and average daily steps. High-spend weeks coincide with city stays where you logged
                  fewer walks. The journal entries from those weeks reference restaurants and museums,
                  not hiking or exploration.
                </div>
              </div>
            </div>
            <div className="trevor-chat-overlay">
              <span className="trevor-overlay-badge">
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-purple)', display: 'inline-block' }} />
                Coming soon
              </span>
              <a href={TREVOR_REPO} className="btn btn-secondary" style={{ fontSize: 13, padding: '10px var(--space-4)' }} target="_blank" rel="noopener">
                Follow the build on GitHub &rarr;
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* ── How it works ──────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Architecture</p>
          <h2 className="section-title reveal">Tool-calling retrieval.</h2>
          <p className="section-subtitle reveal">
            Instead of a hardcoded router, the LLM decides which tools to invoke based on what the
            question requires. Cross-stream queries trigger both in a single turn.
          </p>

          <div className="trevor-arch-steps">
            {ARCH_STEPS.map((step, i) => (
              <div key={step.label} className="trevor-arch-step reveal">
                <div className="trevor-arch-step-icon">{step.icon}</div>
                <div className="trevor-arch-step-label">{step.label}</div>
                <div className="trevor-arch-step-title">{step.title}</div>
                <p className="trevor-arch-step-text">{step.text}</p>
                {i < ARCH_STEPS.length - 1 && <div className="trevor-arch-connector" />}
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Multi-agent architecture ──────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">How It Works</p>
          <h2 className="section-title reveal">A RAG-Based Multi-Agent System</h2>
          <p className="section-subtitle reveal">
            Trevor is not a wrapper around an LLM. It is a multi-agent system: an orchestrator agent
            that retrieves relevant context from two sources &mdash; a Chroma vector store for journal
            entries, and TravelNet&apos;s SQLite database for structured telemetry &mdash; then
            synthesises across both to answer a question. The retrieval step is what makes it RAG
            (Retrieval-Augmented Generation): rather than asking the LLM to recall facts from
            training, Trevor fetches the actual data and puts it in context.
          </p>

          <div className="trevor-arch-steps" style={{ marginTop: 'var(--space-7)' }}>
            {RAG_CARDS.map(step => (
              <div key={step.label} className="trevor-arch-step reveal">
                <div className="trevor-arch-step-icon">{step.icon}</div>
                <div className="trevor-arch-step-label">{step.label}</div>
                <div className="trevor-arch-step-title">{step.title}</div>
                <p className="trevor-arch-step-text">{step.text}</p>
              </div>
            ))}
          </div>

          <div className="reveal">
            <AgentGraph lines={AGENT_DIAGRAM} />
          </div>

          <div className="reveal" style={{
            marginTop: 'var(--space-7)', paddingTop: 'var(--space-6)',
            borderTop: '1px solid var(--border)',
          }}>
            <p style={{
              fontSize: 11, color: 'var(--accent-purple)', fontWeight: 700,
              textTransform: 'uppercase', letterSpacing: '0.1em',
              marginBottom: 'var(--space-3)',
            }}>Planned Extension &mdash; Specialist Sub-Agents</p>
            <p style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.7, maxWidth: 680 }}>
              Trevor&apos;s tool list is designed to be extended with sub-agents for queries that
              require more than fetching a value. Each sub-agent is a LangGraph sub-graph that
              receives a natural language query, makes multiple tool calls within its domain, and
              returns a domain-level conclusion &mdash; for example, &ldquo;Melbourne was 1.4&times;
              your trip average spend, but 0.9&times; when adjusted for accommodation type and cost
              of living.&rdquo; Trevor&apos;s system prompt describes when to prefer a sub-agent over
              a direct SQL tool.
            </p>
            <p style={{ fontSize: 14, color: 'var(--text-secondary)', lineHeight: 1.7, maxWidth: 680, marginTop: 'var(--space-3)' }}>
              This is the agent-as-tool pattern: from Trevor&apos;s perspective, calling a sub-agent
              is identical to calling a SQL tool. The complexity is encapsulated inside the sub-graph.
            </p>
            <p style={{ fontSize: 13, color: 'var(--text-tertiary)', marginTop: 'var(--space-4)' }}>
              <Link to="/ai" style={{ color: 'var(--accent-purple)', textDecoration: 'none' }}>
                &rarr; See the AI Features page for implementation detail
              </Link>
            </p>
          </div>
        </div>
      </section>

      {/* ── Features ──────────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Capabilities</p>
          <h2 className="section-title reveal">What Trevor does.</h2>

          <div className="trevor-page-features">
            {FEATURES.map(({ icon, title, text }) => (
              <div key={title} className="trevor-feature-item reveal">
                <span className="trevor-feature-item-icon">{icon}</span>
                <div>
                  <span className="trevor-feature-item-title">{title}</span>
                  <span className="trevor-feature-item-text">{text}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── Tech stack ────────────────────────────────────── */}
      <section>
        <div className="section-inner">
          <p className="section-eyebrow reveal">Stack</p>
          <h2 className="section-title reveal">Built on.</h2>
          <p className="section-subtitle reveal">
            The components that make Trevor&apos;s retrieval and generation pipeline work.
          </p>
          <div className="reveal">
            <TechTable rows={TECH_STACK} />
          </div>
        </div>
      </section>

      {/* ── GitHub links ──────────────────────────────────── */}
      <section style={{ background: 'var(--bg-sunken)' }}>
        <div className="section-inner" style={{ textAlign: 'center' }}>
          <div className="trevor-page-avatar-lg reveal" style={{ margin: '0 auto var(--space-4)' }}>✦</div>
          <h2 className="section-title reveal" style={{ marginBottom: 'var(--space-3)' }}>Follow the build.</h2>
          <p className="section-subtitle reveal" style={{ marginBottom: 'var(--space-6)', marginLeft: 'auto', marginRight: 'auto' }}>
            Trevor is under active development alongside TravelNet. The data collection stage is
            underway — journal entries and telemetry are being gathered throughout the trip.
          </p>
          <div className="trevor-page-ctas reveal">
            <a href={TREVOR_REPO} className="btn btn-primary" target="_blank" rel="noopener">
              {GITHUB_SVG}
              Trevor Repository
            </a>
            <a href={GITHUB_REPO} className="btn btn-secondary" target="_blank" rel="noopener">
              {GITHUB_SVG}
              TravelNet Repository
            </a>
          </div>
        </div>
      </section>
    </>
  );
}
