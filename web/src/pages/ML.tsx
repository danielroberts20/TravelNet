import { Link } from 'react-router-dom';

export default function ML() {
  return (
    <div className="page-wrapper">
      <div className="coming-soon">
        <span className="coming-soon-badge">◌&nbsp;&nbsp;Coming 2027</span>
        <h1>ML Insights</h1>
        <p>Travel leg segmentation, fitness trend decomposition, spending pattern detection — machine learning models trained on two years of personal data.</p>
        <p className="mono" style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>Expected: 2027</p>
        <Link to="/" className="btn btn-secondary" style={{ marginTop: 'var(--space-4)' }}>← Back to home</Link>
      </div>
    </div>
  );
}
