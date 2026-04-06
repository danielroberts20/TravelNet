import { useState, useEffect } from 'react';
import { Outlet, NavLink, Link, useLocation } from 'react-router-dom';
import { useStats } from '../hooks/useStats';
import { GITHUB_REPO, TREVOR_REPO, DOCS_URL, PERSONAL_SITE } from '../data/travel';

const NAV_ITEMS = [
  { title: 'Journey',     path: '/journey'  },
  { title: 'Explorer',    path: '/explorer' },
  { title: 'ML Insights', path: '/ml'       },
  { title: 'Trevor',      path: '/trevor'   },
  { title: 'About',       path: '/about'    },
];

const GITHUB_SVG = (
  <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
    <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
  </svg>
);

function TrevorWidget() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      const target = e.target as Element;
      if (!target.closest('.trevor-widget')) {
        setOpen(false);
      }
    }
    document.addEventListener('click', handleClick);
    return () => document.removeEventListener('click', handleClick);
  }, []);

  return (
    <div className="trevor-widget">
      <div className={`trevor-panel${open ? ' open' : ''}`} aria-hidden={!open}>
        <div className="trevor-panel-header">
          <div className="trevor-avatar">✦</div>
          <div>
            <div className="trevor-panel-name">Trevor</div>
            <div className="trevor-panel-sub">TravelNet Assistant</div>
          </div>
        </div>
        <div className="trevor-panel-body">
          <span className="trevor-widget-badge">
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--accent-purple)', display: 'inline-block' }}></span>
            Coming soon
          </span>
          <p className="trevor-panel-desc">
            Trevor is a RAG-based AI assistant that lets you have a conversation with three years of personal travel data.
          </p>
          <div className="trevor-example-queries">
            <span className="trevor-example-label">Example queries</span>
            <div className="trevor-query">"Summarise how I felt in Vietnam"</div>
            <div className="trevor-query">"Did my step count drop in weeks I overspent?"</div>
            <div className="trevor-query">"What triggered the spending spike in week 14?"</div>
          </div>
        </div>
        <div className="trevor-panel-footer">
          <a href={TREVOR_REPO} className="btn btn-secondary" style={{ width: '100%', justifyContent: 'center', fontSize: 13, padding: '10px' }} target="_blank" rel="noopener">
            <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
            </svg>
            View on GitHub
          </a>
        </div>
      </div>
      <button
        className={`trevor-fab${open ? ' open' : ''}`}
        aria-label="Chat with Trevor"
        aria-expanded={open}
        onClick={e => { e.stopPropagation(); setOpen(v => !v); }}
      >
        <svg className="icon-chat" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
        </svg>
        <svg className="icon-close" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round">
          <line x1="18" y1="6" x2="6" y2="18"/>
          <line x1="6" y1="6" x2="18" y2="18"/>
        </svg>
      </button>
    </div>
  );
}

export default function Layout() {
  const stats = useStats();
  const location = useLocation();
  const isJourney = location.pathname === '/journey';

  const statusDotColor =
    stats?.status === 'travelling' ? 'var(--accent-teal)' :
    stats?.status === 'finished'   ? 'var(--accent-orange)' :
                                     'var(--accent)';

  return (
    <>
      <nav className="nav">
        <div className="nav-inner" style={{ display: 'grid', gridTemplateColumns: '1fr auto 1fr', alignItems: 'center' }}>
          <Link to="/" className="nav-logo">
            <span className="nav-logo-wordmark">Travel<span>Net</span></span>
          </Link>

          <ul className="nav-links" style={{ margin: '0 auto' }}>
            {NAV_ITEMS.map(item => (
              <li key={item.path}>
                <NavLink to={item.path}>{item.title}</NavLink>
              </li>
            ))}
          </ul>

          <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', justifyContent: 'flex-end' }}>
            <a href={DOCS_URL} className="nav-github">Docs</a>
            <a href={GITHUB_REPO} className="nav-github" target="_blank" rel="noopener">
              {GITHUB_SVG}
              GitHub
            </a>
          </div>
        </div>
      </nav>

      {/* Journey page is full-screen — no wrapper */}
      {isJourney ? (
        <Outlet />
      ) : (
        <div className="page-wrapper">
          <Outlet />
        </div>
      )}

      <footer className="footer">
        <div className="footer-inner">
          <p className="footer-copy">
            &copy; {new Date().getFullYear()} <a href={PERSONAL_SITE}>Dan Roberts</a>.
            Built for the road.
          </p>
          <ul className="footer-links">
            <li><Link to="/about">About</Link></li>
            <li><a href={GITHUB_REPO} target="_blank" rel="noopener">GitHub</a></li>
          </ul>
          <span className="footer-mono">
            {stats?.status === 'travelling' ? '⬤\u00a0LIVE' :
             stats?.status === 'finished' ? '◌\u00a0ARCHIVED' :
             '◌\u00a0PRE-DEPARTURE'}
          </span>
        </div>
      </footer>

      <TrevorWidget />

      {/* Keep status dot colour in sync */}
      <style>{`.nav-status-dot { background: ${statusDotColor} !important; }`}</style>
    </>
  );
}
