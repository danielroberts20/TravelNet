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
/* ============================================================
   TravelNet — D3 SVG Map
   Pacific-centred equirectangular projection using Natural Earth
   110m TopoJSON from jsDelivr. Animated route arcs via SVG
   stroke-dashoffset.
   ============================================================ */

function initGPSCanvas() {
  const container = document.getElementById('gps-map');
  if (!container) return;

  // Load D3 and TopoJSON from CDN then initialise
  Promise.all([
    loadScript('https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js'),
    loadScript('https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js'),
  ]).then(() => {
    fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json')
      .then(r => r.json())
      .then(world => initMap(world));
  });

  function loadScript(src) {
    return new Promise((resolve, reject) => {
      if (document.querySelector(`script[src="${src}"]`)) return resolve();
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      s.onerror = reject;
      document.head.appendChild(s);
    });
  }

  // ----------------------------------------------------------
  // Waypoints
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

  function initMap(world) {
    const svg = d3.select('#gps-map');
    svg.selectAll('*').remove();

    const rect   = container.getBoundingClientRect();
    const width  = rect.width  || 960;
    const height = rect.height || 400;
    const navH   = 44; // chrome bar height

    svg
      .attr('width',  width)
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`)
      .style('display', 'block');

    const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    // ----------------------------------------------------------
    // Projection — Pacific centred
    // rotate([150, 0]) shifts the central meridian to 150°W,
    // putting the Pacific in the middle of the view
    // ----------------------------------------------------------
    const projection = d3.geoEquirectangular()
      .rotate([-150, 0])
      .fitExtent([[0, navH], [width, height]], {
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [[
            [100, 55],   // top-left:  100°E, 55°N (SE Asia top)
            [210, 55],   // top-right: 210°E = 150°W, 55°N (Canada)
            [210, -50],  // bot-right: 150°W, 50°S
            [100, -50],  // bot-left:  100°E, 50°S
            [100, 55]
          ]]
        }
      });

    const path = d3.geoPath().projection(projection);

    // ----------------------------------------------------------
    // Defs — clip path so content stays inside map bounds
    // ----------------------------------------------------------
    const defs = svg.append('defs');
    defs.append('clipPath')
      .attr('id', 'map-clip')
      .append('rect')
      .attr('x', 0).attr('y', navH)
      .attr('width', width).attr('height', height - navH);

    const mapGroup = svg.append('g').attr('clip-path', 'url(#map-clip)');

    // ----------------------------------------------------------
    // Grid lines (graticule)
    // ----------------------------------------------------------
    const graticule = d3.geoGraticule().step([15, 15]);
    mapGroup.append('path')
      .datum(graticule())
      .attr('d', path)
      .attr('fill', 'none')
      .attr('stroke', isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)')
      .attr('stroke-width', 0.5);

    // ----------------------------------------------------------
    // Landmasses
    // ----------------------------------------------------------
    const countries = topojson.feature(world, world.objects.countries);
    mapGroup.append('g')
      .selectAll('path')
      .data(countries.features)
      .join('path')
      .attr('d', path)
      .attr('fill', isDark ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.08)')
      .attr('stroke', isDark ? 'rgba(255,255,255,0.12)' : 'rgba(0,0,0,0.14)')
      .attr('stroke-width', 0.5);

    // ----------------------------------------------------------
    // Route arcs — great circle paths between waypoints
    // ----------------------------------------------------------
    const arcGroup = mapGroup.append('g');
    const dotGroup = mapGroup.append('g');
    const labelGroup = mapGroup.append('g');

    const arcs = [];
    for (let i = 0; i < WAYPOINTS.length - 1; i++) {
      const wp1 = WAYPOINTS[i];
      const wp2 = WAYPOINTS[i + 1];

      // Great circle arc
      const arcData = {
        type: 'LineString',
        coordinates: interpolateGreatCircle(
          [wp1.lon, wp1.lat],
          [wp2.lon, wp2.lat],
          80
        )
      };

      const arcPath = arcGroup.append('path')
        .datum(arcData)
        .attr('d', path)
        .attr('fill', 'none')
        .attr('stroke', '#0A84FF')
        .attr('stroke-width', 2)
        .attr('stroke-linecap', 'round')
        .style('filter', 'drop-shadow(0 0 4px rgba(10,132,255,0.6))');

      // Animate via stroke-dashoffset
      const totalLength = arcPath.node().getTotalLength();
      arcPath
        .attr('stroke-dasharray', totalLength)
        .attr('stroke-dashoffset', totalLength);

      arcs.push({ path: arcPath, length: totalLength, wp1, wp2 });
    }

    // ----------------------------------------------------------
    // Dots & labels
    // ----------------------------------------------------------
    WAYPOINTS.forEach((wp, i) => {
      const [px, py] = projection([wp.lon, wp.lat]) || [null, null];
      if (!px) return;

      const dot = dotGroup.append('circle')
        .attr('cx', px).attr('cy', py)
        .attr('r', 4)
        .attr('fill', '#30D158')
        .attr('stroke', isDark ? '#000' : '#fff')
        .attr('stroke-width', 1.5)
        .style('opacity', 0);

      // Pulse ring
      const pulse = dotGroup.append('circle')
        .attr('cx', px).attr('cy', py)
        .attr('r', 4)
        .attr('fill', 'none')
        .attr('stroke', 'rgba(48,209,88,0.4)')
        .attr('stroke-width', 1.5)
        .style('opacity', 0);

      // Label
      const isMobile = width < 500;
      const label = labelGroup.append('text')
        .attr('x', px + 7).attr('y', py + 4)
        .attr('font-size', isMobile ? 9 : 11)
        .attr('font-family', '-apple-system,"SF Pro Text","Helvetica Neue",sans-serif')
        .attr('font-weight', 500)
        .attr('fill', isDark ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.55)')
        .style('opacity', 0)
        .text(isMobile ? '' : wp.label);

      wp._dot   = dot;
      wp._pulse = pulse;
      wp._label = label;
    });

    // ----------------------------------------------------------
    // Leading dot (animated position along current arc)
    // ----------------------------------------------------------
    const leadDot = mapGroup.append('circle')
      .attr('r', 5)
      .attr('fill', '#0A84FF')
      .style('filter', 'drop-shadow(0 0 6px rgba(10,132,255,0.8))')
      .style('opacity', 0);

    const leadPulse = mapGroup.append('circle')
      .attr('r', 5)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(10,132,255,0.4)')
      .attr('stroke-width', 1.5)
      .style('opacity', 0);

    // ----------------------------------------------------------
    // Animation sequence
    // ----------------------------------------------------------
    const ARC_DURATION  = 2000;  // ms per arc
    const PAUSE_BETWEEN = 400;   // ms pause at each waypoint
    const RESET_PAUSE   = 1500;  // ms pause before restarting

    function animateSequence() {
      // Reset all
      arcs.forEach(a => {
        a.path
          .attr('stroke-dashoffset', a.length)
          .attr('stroke', '#0A84FF');
      });
      WAYPOINTS.forEach(wp => {
        if (wp._dot)   wp._dot.style('opacity', 0);
        if (wp._pulse) wp._pulse.style('opacity', 0);
        if (wp._label) wp._label.style('opacity', 0);
      });
      leadDot.style('opacity', 0);
      leadPulse.style('opacity', 0);

      // Show first waypoint dot
      showWaypoint(0, () => animateArc(0));
    }

    function showWaypoint(i, cb) {
      const wp = WAYPOINTS[i];
      if (!wp || !wp._dot) { if (cb) cb(); return; }

      wp._dot.transition().duration(300).style('opacity', 1);
      wp._label.transition().duration(300).style('opacity', 1);

      // Pulse animation
      animatePulse(wp._pulse);

      if (cb) setTimeout(cb, 300);
    }

    function animatePulse(ring) {
      if (!ring || ring.style('opacity') === '0') return;
      ring.style('opacity', 1)
        .attr('r', 4)
        .attr('stroke-opacity', 0.6);

      ring.transition()
        .duration(1200)
        .attr('r', 14)
        .attr('stroke-opacity', 0)
        .on('end', () => {
          ring.attr('r', 4).attr('stroke-opacity', 0.6);
          animatePulse(ring);
        });
    }

    function animateArc(i) {
      if (i >= arcs.length) {
        // All done — pause then restart
        leadDot.style('opacity', 0);
        leadPulse.style('opacity', 0);
        setTimeout(animateSequence, RESET_PAUSE);
        return;
      }

      const arc = arcs[i];
      const pathNode = arc.path.node();

      leadDot.style('opacity', 1);
      leadPulse.style('opacity', 1);

      // Animate the arc stroke
      arc.path.transition()
        .duration(ARC_DURATION)
        .ease(d3.easeLinear)
        .attr('stroke-dashoffset', 0)
        .on('end', () => {
          // Show destination waypoint
          showWaypoint(i + 1, () => {
            setTimeout(() => animateArc(i + 1), PAUSE_BETWEEN);
          });
        });

      // Animate leading dot along the arc path
      const startTime = performance.now();
      function moveDot(now) {
        const elapsed = now - startTime;
        const t = Math.min(elapsed / ARC_DURATION, 1);
        const len = arc.length * t;
        try {
          const pt = pathNode.getPointAtLength(arc.length - (arc.length - len));
          leadDot.attr('cx', pt.x).attr('cy', pt.y);
          leadPulse.attr('cx', pt.x).attr('cy', pt.y);

          // Pulse the ring
          const pulse = (Math.sin(now / 300) + 1) / 2;
          leadPulse.attr('r', 5 + pulse * 8).attr('stroke-opacity', 0.4 * (1 - pulse));
        } catch(e) {}

        if (t < 1) requestAnimationFrame(moveDot);
      }
      requestAnimationFrame(moveDot);
    }

    animateSequence();

    // ----------------------------------------------------------
    // Resize handler
    // ----------------------------------------------------------
    let resizeTimer;
    window.addEventListener('resize', () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => initMap(world), 200);
    });
  }

  // ----------------------------------------------------------
  // Interpolate great circle between two lon/lat points
  // Returns array of [lon, lat] coordinates
  // ----------------------------------------------------------
  function interpolateGreatCircle([lon1, lat1], [lon2, lat2], steps) {
    const coords = [];
    for (let i = 0; i <= steps; i++) {
      const t = i / steps;
      coords.push(d3.geoInterpolate([lon1, lat1], [lon2, lat2])(t));
    }
    return coords;
  }
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
