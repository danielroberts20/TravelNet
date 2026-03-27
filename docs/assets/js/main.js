/* TravelNet — main.js */

/* --------------------------------------------------------
   Countdown Timer
   -------------------------------------------------------- */
function initCountdown() {
  const el = document.getElementById('countdown');
  if (!el) return;

  const departure = new Date(el.dataset.date);

  function tick() {
    const now = new Date();
    const diff = departure - now;

    if (diff <= 0) {
      el.innerHTML = '<span class="countdown-value mono" style="font-size:20px;color:var(--accent-teal)">Travelling now ✈</span>';
      return;
    }

    const d = Math.floor(diff / 86400000);
    const h = Math.floor((diff % 86400000) / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    const s = Math.floor((diff % 60000) / 1000);

    const fmt = (n) => String(n).padStart(2, '0');

    el.innerHTML = `
      <div class="countdown-unit">
        <span class="countdown-value">${d}</span>
        <span class="countdown-label">days</span>
      </div>
      <span class="countdown-sep">:</span>
      <div class="countdown-unit">
        <span class="countdown-value">${fmt(h)}</span>
        <span class="countdown-label">hrs</span>
      </div>
      <span class="countdown-sep">:</span>
      <div class="countdown-unit">
        <span class="countdown-value">${fmt(m)}</span>
        <span class="countdown-label">min</span>
      </div>
      <span class="countdown-sep">:</span>
      <div class="countdown-unit">
        <span class="countdown-value">${fmt(s)}</span>
        <span class="countdown-label">sec</span>
      </div>
    `;
  }

  tick();
  setInterval(tick, 1000);
}

/* --------------------------------------------------------
   Scroll Reveal
   -------------------------------------------------------- */
function initReveal() {
  const els = document.querySelectorAll('.reveal');
  if (!els.length) return;

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        e.target.classList.add('visible');
        observer.unobserve(e.target);
      }
    });
  }, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

  els.forEach(el => observer.observe(el));
}

/* --------------------------------------------------------
   Animated GPS Trace Canvas
   Placeholder for Kepler.gl embed — draws a flight path
   arc animation across a simplified world outline.
   -------------------------------------------------------- */
function initGPSCanvas() {
  const canvas = document.getElementById('gps-canvas');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');
  let w, h, animFrame;

  // Route waypoints [x%, y%] relative to canvas — approximate
  // world positions for the trip legs
  const waypoints = [
    { label: 'Philadelphia',  x: 0.21, y: 0.35 },
    { label: 'Seattle',       x: 0.10, y: 0.30 },
    { label: 'Fiji',          x: 0.08, y: 0.65 },
    { label: 'Australia',     x: 0.13, y: 0.72 },
    { label: 'New Zealand',   x: 0.18, y: 0.78 },
    { label: 'SE Asia',       x: 0.28, y: 0.55 },
    { label: 'Canada',        x: 0.12, y: 0.28 },
  ];

  let progress = 0;  // 0 → waypoints.length-1
  const SPEED = 0.004;
  let isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

  function resize() {
    const rect = canvas.parentElement.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width  = rect.width  * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width  = rect.width  + 'px';
    canvas.style.height = rect.height + 'px';
    ctx.scale(dpr, dpr);
    w = rect.width;
    h = rect.height;
  }

  // Map waypoints to canvas coords (leave room for nav bar)
  function pt(wp) {
    return {
      x: wp.x * w,
      y: (wp.y * (h - 44)) + 44
    };
  }

  // Quadratic bezier midpoint (arc upward between two points)
  function arcMid(p1, p2, lift = 0.3) {
    const mx = (p1.x + p2.x) / 2;
    const my = (p1.y + p2.y) / 2 - Math.abs(p2.x - p1.x) * lift;
    return { x: mx, y: my };
  }

  // Point along a quadratic bezier at t
  function bezierPt(p0, p1, p2, t) {
    const x = (1-t)*(1-t)*p0.x + 2*(1-t)*t*p1.x + t*t*p2.x;
    const y = (1-t)*(1-t)*p0.y + 2*(1-t)*t*p1.y + t*t*p2.y;
    return { x, y };
  }

  function getColour(key) {
    const style = getComputedStyle(document.documentElement);
    return style.getPropertyValue(key).trim();
  }

  function drawGrid() {
    ctx.save();
    ctx.strokeStyle = isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)';
    ctx.lineWidth = 1;
    const cols = 12, rows = 6;
    for (let i = 0; i <= cols; i++) {
      ctx.beginPath();
      ctx.moveTo((i / cols) * w, 44);
      ctx.lineTo((i / cols) * w, h);
      ctx.stroke();
    }
    for (let j = 0; j <= rows; j++) {
      ctx.beginPath();
      ctx.moveTo(0, 44 + (j / rows) * (h - 44));
      ctx.lineTo(w, 44 + (j / rows) * (h - 44));
      ctx.stroke();
    }
    ctx.restore();
  }

  // Draw completed arc segments
  function drawCompletedArcs(upTo) {
    for (let i = 0; i < upTo; i++) {
      const p1 = pt(waypoints[i]);
      const p2 = pt(waypoints[i + 1]);
      const mid = arcMid(p1, p2);

      ctx.save();
      ctx.beginPath();
      ctx.moveTo(p1.x, p1.y);
      ctx.quadraticCurveTo(mid.x, mid.y, p2.x, p2.y);
      ctx.strokeStyle = isDark ? 'rgba(10,132,255,0.25)' : 'rgba(10,132,255,0.20)';
      ctx.lineWidth = 1.5;
      ctx.setLineDash([4, 4]);
      ctx.stroke();
      ctx.restore();
    }
  }

  // Draw in-progress arc (current segment)
  function drawActiveArc(segIdx, t) {
    const p1  = pt(waypoints[segIdx]);
    const p2  = pt(waypoints[segIdx + 1]);
    const mid = arcMid(p1, p2);

    // Trace the arc up to t using many small segments
    const steps = 60;
    const pts = [];
    for (let i = 0; i <= Math.round(t * steps); i++) {
      pts.push(bezierPt(p1, mid, p2, i / steps));
    }

    if (pts.length < 2) return;

    ctx.save();
    ctx.beginPath();
    ctx.moveTo(pts[0].x, pts[0].y);
    for (let i = 1; i < pts.length; i++) {
      ctx.lineTo(pts[i].x, pts[i].y);
    }
    ctx.strokeStyle = '#0A84FF';
    ctx.lineWidth = 2;
    ctx.setLineDash([]);
    ctx.shadowColor = '#0A84FF';
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.restore();

    // Dot at leading edge
    const tip = pts[pts.length - 1];
    ctx.save();
    ctx.beginPath();
    ctx.arc(tip.x, tip.y, 5, 0, Math.PI * 2);
    ctx.fillStyle = '#0A84FF';
    ctx.shadowColor = '#0A84FF';
    ctx.shadowBlur = 16;
    ctx.fill();
    ctx.restore();

    // Pulse ring
    const pulse = (Math.sin(Date.now() / 400) + 1) / 2;
    ctx.save();
    ctx.beginPath();
    ctx.arc(tip.x, tip.y, 5 + pulse * 10, 0, Math.PI * 2);
    ctx.strokeStyle = `rgba(10,132,255,${0.4 * (1 - pulse)})`;
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.restore();
  }

  // Draw visited city dots
  function drawDots(upTo, activeIdx, t) {
    waypoints.forEach((wp, i) => {
      if (i > activeIdx + 1) return;
      const p = pt(wp);
      const isActive = (i === activeIdx);
      const isTip    = (i === activeIdx + 1) && t > 0.98;

      ctx.save();
      ctx.beginPath();
      ctx.arc(p.x, p.y, isActive ? 5 : 4, 0, Math.PI * 2);
      ctx.fillStyle = (i <= upTo || isTip)
        ? (isActive ? '#0A84FF' : '#30D158')
        : (isDark ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.15)');
      ctx.fill();

      // Label
      if (i <= activeIdx || isTip) {
        ctx.font = `500 11px -apple-system,"SF Pro Text","Helvetica Neue",sans-serif`;
        ctx.fillStyle = isDark ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.5)';
        ctx.fillText(wp.label, p.x + 8, p.y + 4);
      }
      ctx.restore();
    });
  }

  function draw() {
    ctx.clearRect(0, 0, w, h);
    drawGrid();

    const segIdx = Math.min(Math.floor(progress), waypoints.length - 2);
    const t      = progress - segIdx;

    drawCompletedArcs(segIdx);
    drawActiveArc(segIdx, t);
    drawDots(segIdx, segIdx, t);

    progress += SPEED;
    if (progress >= waypoints.length - 1) progress = 0;

    animFrame = requestAnimationFrame(draw);
  }

  resize();
  draw();

  window.addEventListener('resize', () => {
    cancelAnimationFrame(animFrame);
    resize();
    draw();
  });

  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', e => {
    isDark = e.matches;
  });
}

/* --------------------------------------------------------
   Nav active state
   -------------------------------------------------------- */
function initNav() {
  const links = document.querySelectorAll('.nav-links a');
  const current = window.location.pathname;
  links.forEach(a => {
    if (a.getAttribute('href') === current ||
        (current !== '/' && current.startsWith(a.getAttribute('href')))) {
      a.classList.add('active');
    }
  });
}

/* --------------------------------------------------------
   Boot
   -------------------------------------------------------- */
document.addEventListener('DOMContentLoaded', () => {
  initNav();
  initCountdown();
  initReveal();
  initGPSCanvas();
});
