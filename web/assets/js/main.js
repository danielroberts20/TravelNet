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
  /* ============================================================
   TravelNet — GPS Canvas Map
   Pacific-focused equirectangular projection with simplified
   landmass outlines. Replaces the generic grid version.
   ============================================================ */

  (function () {

    const canvas = document.getElementById('gps-canvas');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    let w, h, animFrame, isDark;

    // ----------------------------------------------------------
    // Projection config — Pacific-centred
    // Longitude range: 100°E → 240°E (= 120°W)
    // Latitude range:  55°N  → 55°S
    // ----------------------------------------------------------
    const LON_MIN = 100, LON_MAX = 240;  // 240 = -120 (wraps Pacific)
    const LAT_MIN = -55, LAT_MAX = 55;

    function project(lon, lat) {
      // Normalise lon to 0-360 for Pacific-centred view
      if (lon < 0) lon += 360;
      const x = ((lon - LON_MIN) / (LON_MAX - LON_MIN)) * w;
      const y = ((LAT_MAX - lat) / (LAT_MAX - LAT_MIN)) * (h - 44) + 44;
      return { x, y };
    }

    // ----------------------------------------------------------
    // Waypoints [lon, lat] for the trip route
    // ----------------------------------------------------------
    const WAYPOINTS = [
      { label: 'Philadelphia', lon: -75.2,  lat: 39.9  },
      { label: 'Seattle',      lon: -122.3, lat: 47.6  },
      { label: 'Fiji',         lon: 178.4,  lat: -18.1 },
      { label: 'Sydney',       lon: 151.2,  lat: -33.9 },
      { label: 'Auckland',     lon: 174.8,  lat: -36.9 },
      { label: 'Bangkok',      lon: 100.5,  lat: 13.7  },
      { label: 'Vancouver',    lon: -123.1, lat: 49.3  },
    ];

    // ----------------------------------------------------------
    // Simplified landmass polygons [lon, lat] pairs
    // Focused on: North America west coast, Pacific islands,
    // Australia, New Zealand, SE Asia
    // ----------------------------------------------------------
    const LANDMASSES = [
      // Continental USA / Canada west coast (simplified)
      {
        name: 'north_america',
        coords: [
          [-168, 72], [-140, 72], [-130, 58], [-124, 48], [-124, 37],
          [-117, 32], [-105, 20], [-90, 15], [-83, 10], [-78, 8],
          [-75, 10], [-78, 15], [-85, 22], [-90, 28], [-97, 26],
          [-105, 23], [-110, 28], [-117, 35], [-120, 38], [-122, 37],
          [-124, 40], [-124, 48], [-126, 50], [-128, 54], [-136, 58],
          [-145, 62], [-152, 60], [-158, 58], [-162, 60], [-165, 64],
          [-168, 66], [-168, 72]
        ]
      },
      // Australia
      {
        name: 'australia',
        coords: [
          [114, -22], [118, -20], [122, -18], [128, -15], [132, -12],
          [136, -12], [140, -15], [142, -11], [145, -15], [148, -20],
          [150, -23], [152, -25], [154, -28], [153, -30], [152, -33],
          [151, -34], [150, -37], [148, -38], [146, -39], [144, -38],
          [141, -38], [138, -36], [135, -35], [132, -34], [129, -34],
          [126, -34], [122, -34], [118, -32], [115, -30], [114, -27],
          [113, -24], [114, -22]
        ]
      },
      // New Zealand - North Island
      {
        name: 'nz_north',
        coords: [
          [172, -34], [173, -35], [174, -36], [175, -37], [176, -38],
          [177, -39], [178, -38], [178, -37], [177, -36], [176, -35],
          [175, -36], [174, -37], [173, -36], [172, -35], [172, -34]
        ]
      },
      // New Zealand - South Island
      {
        name: 'nz_south',
        coords: [
          [166, -46], [167, -46], [168, -45], [169, -44], [170, -43],
          [171, -42], [172, -41], [173, -41], [174, -42], [173, -43],
          [172, -44], [171, -45], [170, -46], [169, -47], [168, -46],
          [167, -46], [166, -46]
        ]
      },
      // SE Asia mainland (simplified)
      {
        name: 'se_asia',
        coords: [
          [100, 5], [102, 2], [104, 1], [104, 3], [103, 4],
          [102, 6], [100, 6], [99, 8], [98, 10], [99, 12],
          [100, 14], [101, 16], [102, 18], [104, 20], [106, 22],
          [108, 22], [110, 20], [111, 18], [110, 16], [108, 14],
          [106, 12], [105, 10], [106, 8], [107, 10], [108, 12],
          [109, 14], [110, 16], [112, 18], [114, 20], [116, 20],
          [118, 18], [118, 15], [116, 12], [114, 10], [112, 8],
          [110, 6], [108, 4], [106, 2], [104, 0], [102, -2],
          [104, -4], [106, -6], [108, -7], [110, -8], [112, -8],
          [114, -8], [116, -8], [118, -8], [116, -6], [114, -4],
          [112, -2], [110, 0], [108, 2], [106, 4], [104, 6],
          [102, 8], [100, 8], [100, 5]
        ]
      },
      // Fiji (simplified dot-island)
      {
        name: 'fiji',
        coords: [
          [177, -17], [178, -17], [178, -18], [177, -18], [177, -17]
        ]
      },
      // Japan (simplified — visible in upper frame)
      {
        name: 'japan',
        coords: [
          [130, 31], [131, 33], [132, 34], [133, 35], [135, 35],
          [136, 36], [137, 37], [138, 38], [139, 39], [140, 40],
          [141, 41], [142, 42], [141, 40], [140, 38], [139, 36],
          [138, 35], [137, 34], [136, 33], [134, 33], [132, 32],
          [130, 31]
        ]
      },
    ];

    // ----------------------------------------------------------
    // Resize
    // ----------------------------------------------------------
    function resize() {
      const rect = canvas.parentElement.getBoundingClientRect();
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      w = rect.width;
      h = rect.height;
      canvas.width  = w * dpr;
      canvas.height = h * dpr;
      canvas.style.width  = w + 'px';
      canvas.style.height = h + 'px';
      ctx.scale(dpr, dpr);
    }

    // ----------------------------------------------------------
    // Draw landmasses
    // ----------------------------------------------------------
    function drawLandmasses() {
      console.log('drawing landmasses, w:', w, 'h:', h, 'isDark:', isDark);
      isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      const fillColour   = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.07)';
      const strokeColour = isDark ? 'rgba(255,255,255,0.10)' : 'rgba(0,0,0,0.12)';

      LANDMASSES.forEach(land => {
        const pts = land.coords.map(([lon, lat]) => project(lon, lat));
        if (!pts.length) return;

        ctx.beginPath();
        ctx.moveTo(pts[0].x, pts[0].y);
        pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
        ctx.closePath();
        ctx.fillStyle = fillColour;
        ctx.fill();
        ctx.strokeStyle = strokeColour;
        ctx.lineWidth = 1;
        ctx.stroke();
      });
    }

    // ----------------------------------------------------------
    // Draw latitude / longitude grid (subtle)
    // ----------------------------------------------------------
    function drawGrid() {
      const lineColour = isDark ? 'rgba(255,255,255,0.03)' : 'rgba(0,0,0,0.03)';
      ctx.strokeStyle = lineColour;
      ctx.lineWidth = 1;

      // Lat lines every 15°
      for (let lat = -45; lat <= 45; lat += 15) {
        const p = project(LON_MIN, lat);
        const p2 = project(LON_MAX, lat);
        ctx.beginPath();
        ctx.moveTo(p.x, p.y);
        ctx.lineTo(p2.x, p2.y);
        ctx.stroke();
      }

      // Lon lines every 15°
      for (let lon = 105; lon <= 240; lon += 15) {
        const p  = project(lon, LAT_MAX);
        const p2 = project(lon, LAT_MIN);
        ctx.beginPath();
        ctx.moveTo(p.x, p.y + 44);
        ctx.lineTo(p2.x, p2.y);
        ctx.stroke();
      }
    }

    // ----------------------------------------------------------
    // Arc animation
    // ----------------------------------------------------------
    let progress = 0;
    const SPEED = 0.003;

    function bezierPt(p0, cp, p2, t) {
      return {
        x: (1-t)*(1-t)*p0.x + 2*(1-t)*t*cp.x + t*t*p2.x,
        y: (1-t)*(1-t)*p0.y + 2*(1-t)*t*cp.y + t*t*p2.y,
      };
    }

    function arcControl(p1, p2) {
      const mx = (p1.x + p2.x) / 2;
      const my = (p1.y + p2.y) / 2;
      const dx = Math.abs(p2.x - p1.x);
      const dy = Math.abs(p2.y - p1.y);
      const lift = Math.sqrt(dx*dx + dy*dy) * 0.25;
      return { x: mx, y: my - lift };
    }

    function drawCompletedArcs(upTo) {
      for (let i = 0; i < upTo; i++) {
        const p1 = project(WAYPOINTS[i].lon, WAYPOINTS[i].lat);
        const p2 = project(WAYPOINTS[i+1].lon, WAYPOINTS[i+1].lat);
        const cp = arcControl(p1, p2);

        ctx.beginPath();
        ctx.moveTo(p1.x, p1.y);
        ctx.quadraticCurveTo(cp.x, cp.y, p2.x, p2.y);
        ctx.strokeStyle = isDark ? 'rgba(10,132,255,0.2)' : 'rgba(10,132,255,0.15)';
        ctx.lineWidth = 1.5;
        ctx.setLineDash([4, 5]);
        ctx.stroke();
        ctx.setLineDash([]);
      }
    }

    function drawActiveArc(segIdx, t) {
      const wp1 = WAYPOINTS[segIdx];
      const wp2 = WAYPOINTS[segIdx + 1];
      const p1 = project(wp1.lon, wp1.lat);
      const p2 = project(wp2.lon, wp2.lat);
      const cp = arcControl(p1, p2);

      const steps = 80;
      const count = Math.max(1, Math.round(t * steps));
      const pts = [];
      for (let i = 0; i <= count; i++) {
        pts.push(bezierPt(p1, cp, p2, i / steps));
      }

      if (pts.length < 2) return;

      ctx.beginPath();
      ctx.moveTo(pts[0].x, pts[0].y);
      pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
      ctx.strokeStyle = '#0A84FF';
      ctx.lineWidth = 2;
      ctx.setLineDash([]);
      ctx.shadowColor = '#0A84FF';
      ctx.shadowBlur = 10;
      ctx.stroke();
      ctx.shadowBlur = 0;

      // Leading dot
      const tip = pts[pts.length - 1];
      ctx.beginPath();
      ctx.arc(tip.x, tip.y, 4, 0, Math.PI * 2);
      ctx.fillStyle = '#0A84FF';
      ctx.shadowColor = '#0A84FF';
      ctx.shadowBlur = 14;
      ctx.fill();
      ctx.shadowBlur = 0;

      // Pulse ring
      const pulse = (Math.sin(Date.now() / 400) + 1) / 2;
      ctx.beginPath();
      ctx.arc(tip.x, tip.y, 4 + pulse * 10, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(10,132,255,${0.4 * (1 - pulse)})`;
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }

    function drawDots(activeIdx, t) {
      WAYPOINTS.forEach((wp, i) => {
        if (i > activeIdx + 1) return;
        const p = project(wp.lon, wp.lat);
        const done = i <= activeIdx;
        const isTip = i === activeIdx + 1 && t > 0.95;

        // Dot
        ctx.beginPath();
        ctx.arc(p.x, p.y, done || isTip ? 4 : 3, 0, Math.PI * 2);
        ctx.fillStyle = done || isTip
          ? (i === activeIdx ? '#0A84FF' : '#30D158')
          : (isDark ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.2)');
        ctx.fill();

        // Label — only for reached waypoints, hide on very small screens
        if ((done || isTip) && w > 400) {
          ctx.font = `500 ${w < 600 ? 9 : 11}px -apple-system,"SF Pro Text",sans-serif`;
          ctx.fillStyle = isDark ? 'rgba(255,255,255,0.55)' : 'rgba(0,0,0,0.5)';
          ctx.fillText(wp.label, p.x + 6, p.y + 4);
        }
      });
    }

    // ----------------------------------------------------------
    // Main draw loop
    // ----------------------------------------------------------
    function draw() {
      isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      ctx.clearRect(0, 0, w, h);

      drawGrid();
      drawLandmasses();

      const segIdx = Math.min(Math.floor(progress), WAYPOINTS.length - 2);
      const t = progress - segIdx;

      drawCompletedArcs(segIdx);
      drawActiveArc(segIdx, t);
      drawDots(segIdx, t);

      progress += SPEED;
      if (progress >= WAYPOINTS.length - 1) progress = 0;

      animFrame = requestAnimationFrame(draw);
    }

    // ----------------------------------------------------------
    // Boot
    // ----------------------------------------------------------
    function init() {
      resize();
      draw();
    }

    window.addEventListener('resize', () => {
      cancelAnimationFrame(animFrame);
      resize();
    });

    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
      // Colour picked up per-frame, no action needed
    });

    // Wait for layout to settle before measuring
    if (document.readyState === 'complete') {
      init();
    } else {
      window.addEventListener('load', init);
    }

  })();
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
