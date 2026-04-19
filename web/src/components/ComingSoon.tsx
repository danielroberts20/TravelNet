import { Link } from 'react-router-dom';

interface ComingSoonProps {
  badge: string;
  title: string;
  description: string;
  eta: string;
}

export default function ComingSoon({ badge, title, description, eta }: ComingSoonProps) {
  return (
    <div className="page-wrapper">
      <div className="coming-soon">
        <span className="coming-soon-badge">◌&nbsp;&nbsp;{badge}</span>
        <h1>{title}</h1>
        <p>{description}</p>
        <p className="mono" style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>Expected: {eta}</p>
        <Link to="/" className="btn btn-secondary" style={{ marginTop: 'var(--space-4)' }}>← Back to home</Link>
      </div>
    </div>
  );
}
