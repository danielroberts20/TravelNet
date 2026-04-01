import { Link } from 'react-router-dom';

export default function Explorer() {
  return (
    <div className="page-wrapper">
      <div className="coming-soon">
        <span className="coming-soon-badge">◌&nbsp;&nbsp;Coming 2026</span>
        <h1>Explorer</h1>
        <p>Live database statistics, interactive charts, and spending breakdowns by country, currency, and category — updated as I travel.</p>
        <p className="mono" style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>Expected: Late 2026</p>
        <Link to="/" className="btn btn-secondary" style={{ marginTop: 'var(--space-4)' }}>← Back to home</Link>
      </div>
    </div>
  );
}
