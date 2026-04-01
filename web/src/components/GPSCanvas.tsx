import { useEffect, useRef } from 'react';
import * as d3 from 'd3';
import * as topojson from 'topojson-client';
import type { Topology } from 'topojson-specification';
import { MAP_WAYPOINTS } from '../data/travel';

const WORLD_ATLAS_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json';
const ARC_DURATION = 2000;
const PAUSE_BETWEEN = 400;
const RESET_PAUSE = 1500;

// Interpolate great circle between two lon/lat points
function interpolateGreatCircle(
  from: [number, number],
  to: [number, number],
  steps: number,
): [number, number][] {
  const coords: [number, number][] = [];
  for (let i = 0; i <= steps; i++) {
    coords.push(d3.geoInterpolate(from, to)(i / steps) as [number, number]);
  }
  return coords;
}

export default function GPSCanvas() {
  const svgRef = useRef<SVGSVGElement>(null);
  const worldRef = useRef<Topology | null>(null);
  const animFrameRef = useRef<number>(0);

  function initMap(world: Topology) {
    const svg = d3.select(svgRef.current!);
    svg.selectAll('*').remove();

    const container = svgRef.current!.parentElement!;
    const rect = container.getBoundingClientRect();
    const width = rect.width || 960;
    const height = rect.height || 400;
    const navH = 44;

    svg
      .attr('width', width)
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`)
      .style('display', 'block');

    const isDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    const projection = d3.geoEquirectangular()
      .rotate([-150, 0])
      .fitExtent([[0, navH], [width, height]], {
        type: 'Feature',
        geometry: {
          type: 'Polygon',
          coordinates: [[
            [100, 55], [210, 55], [210, -50], [100, -50], [100, 55],
          ]],
        },
      } as d3.GeoPermissibleObjects);

    const path = d3.geoPath().projection(projection);

    const defs = svg.append('defs');
    defs.append('clipPath').attr('id', 'map-clip')
      .append('rect').attr('x', 0).attr('y', navH).attr('width', width).attr('height', height - navH);

    const mapGroup = svg.append('g').attr('clip-path', 'url(#map-clip)');

    const graticule = d3.geoGraticule().step([15, 15]);
    mapGroup.append('path')
      .datum(graticule())
      .attr('d', path)
      .attr('fill', 'none')
      .attr('stroke', isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)')
      .attr('stroke-width', 0.5);

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const countries = topojson.feature(world, (world.objects as Record<string, any>).countries);
    mapGroup.append('g')
      .selectAll('path')
      .data((countries as unknown as GeoJSON.FeatureCollection).features)
      .join('path')
      .attr('d', path)
      .attr('fill', isDark ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.08)')
      .attr('stroke', isDark ? 'rgba(255,255,255,0.12)' : 'rgba(0,0,0,0.14)')
      .attr('stroke-width', 0.5);

    const arcGroup = mapGroup.append('g');
    const dotGroup = mapGroup.append('g');
    const labelGroup = mapGroup.append('g');

    type ArcEntry = {
      path: d3.Selection<SVGPathElement, GeoJSON.LineString, null, undefined>;
      length: number;
    };

    const arcs: ArcEntry[] = [];

    for (let i = 0; i < MAP_WAYPOINTS.length - 1; i++) {
      const wp1 = MAP_WAYPOINTS[i];
      const wp2 = MAP_WAYPOINTS[i + 1];
      const arcData: GeoJSON.LineString = {
        type: 'LineString',
        coordinates: interpolateGreatCircle([wp1.lon, wp1.lat], [wp2.lon, wp2.lat], 80),
      };

      const arcPath = arcGroup.append<SVGPathElement>('path')
        .datum(arcData)
        .attr('d', path)
        .attr('fill', 'none')
        .attr('stroke', '#0A84FF')
        .attr('stroke-width', 2)
        .attr('stroke-linecap', 'round')
        .style('filter', 'drop-shadow(0 0 4px rgba(10,132,255,0.6))');

      const totalLength = arcPath.node()!.getTotalLength();
      arcPath.attr('stroke-dasharray', totalLength).attr('stroke-dashoffset', totalLength);
      arcs.push({ path: arcPath as unknown as d3.Selection<SVGPathElement, GeoJSON.LineString, null, undefined>, length: totalLength });
    }

    type WpWithDom = typeof MAP_WAYPOINTS[0] & {
      _dot?: d3.Selection<SVGCircleElement, unknown, null, undefined>;
      _pulse?: d3.Selection<SVGCircleElement, unknown, null, undefined>;
      _label?: d3.Selection<SVGTextElement, unknown, null, undefined>;
    };

    const wps: WpWithDom[] = MAP_WAYPOINTS.map(wp => ({ ...wp }));
    const isMobile = width < 500;

    wps.forEach(wp => {
      const projected = projection([wp.lon, wp.lat]);
      if (!projected) return;
      const [px, py] = projected;

      wp._dot = dotGroup.append('circle')
        .attr('cx', px).attr('cy', py).attr('r', 4)
        .attr('fill', '#30D158').attr('stroke', isDark ? '#000' : '#fff').attr('stroke-width', 1.5)
        .style('opacity', 0);

      wp._pulse = dotGroup.append('circle')
        .attr('cx', px).attr('cy', py).attr('r', 4)
        .attr('fill', 'none').attr('stroke', 'rgba(48,209,88,0.4)').attr('stroke-width', 1.5)
        .style('opacity', 0);

      wp._label = labelGroup.append('text')
        .attr('x', px + 7).attr('y', py + 4)
        .attr('font-size', isMobile ? 9 : 11)
        .attr('font-family', '-apple-system,"SF Pro Text","Helvetica Neue",sans-serif')
        .attr('font-weight', 500)
        .attr('fill', isDark ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.55)')
        .style('opacity', 0)
        .text(isMobile ? '' : wp.label);
    });

    const leadDot = mapGroup.append('circle')
      .attr('r', 5).attr('fill', '#0A84FF')
      .style('filter', 'drop-shadow(0 0 6px rgba(10,132,255,0.8))').style('opacity', 0);
    const leadPulse = mapGroup.append('circle')
      .attr('r', 5).attr('fill', 'none')
      .attr('stroke', 'rgba(10,132,255,0.4)').attr('stroke-width', 1.5).style('opacity', 0);

    function animatePulse(ring: WpWithDom['_pulse']) {
      if (!ring || ring.style('opacity') === '0') return;
      ring.style('opacity', 1).attr('r', 4).attr('stroke-opacity', 0.6);
      ring.transition().duration(1200).attr('r', 14).attr('stroke-opacity', 0)
        .on('end', () => { ring.attr('r', 4).attr('stroke-opacity', 0.6); animatePulse(ring); });
    }

    function showWaypoint(i: number, cb?: () => void) {
      const wp = wps[i];
      if (!wp || !wp._dot) { cb?.(); return; }
      wp._dot.transition().duration(300).style('opacity', 1);
      wp._label?.transition().duration(300).style('opacity', 1);
      animatePulse(wp._pulse);
      if (cb) setTimeout(cb, 300);
    }

    function animateArc(i: number) {
      if (i >= arcs.length) {
        leadDot.style('opacity', 0);
        leadPulse.style('opacity', 0);
        setTimeout(animateSequence, RESET_PAUSE);
        return;
      }
      const arc = arcs[i];
      const pathNode = (arc.path.node() as SVGPathElement);

      leadDot.style('opacity', 1);
      leadPulse.style('opacity', 1);

      arc.path.transition().duration(ARC_DURATION).ease(d3.easeLinear)
        .attr('stroke-dashoffset', 0)
        .on('end', () => showWaypoint(i + 1, () => setTimeout(() => animateArc(i + 1), PAUSE_BETWEEN)));

      const startTime = performance.now();
      function moveDot(now: number) {
        const t = Math.min((now - startTime) / ARC_DURATION, 1);
        try {
          const pt = pathNode.getPointAtLength(arc.length * t);
          leadDot.attr('cx', pt.x).attr('cy', pt.y);
          leadPulse.attr('cx', pt.x).attr('cy', pt.y);
          const pulse = (Math.sin(now / 300) + 1) / 2;
          leadPulse.attr('r', 5 + pulse * 8).attr('stroke-opacity', 0.4 * (1 - pulse));
        } catch { /* ignore */ }
        if (t < 1) animFrameRef.current = requestAnimationFrame(moveDot);
      }
      animFrameRef.current = requestAnimationFrame(moveDot);
    }

    function animateSequence() {
      arcs.forEach(a => a.path.attr('stroke-dashoffset', a.length).attr('stroke', '#0A84FF'));
      wps.forEach(wp => {
        wp._dot?.style('opacity', 0);
        wp._pulse?.style('opacity', 0);
        wp._label?.style('opacity', 0);
      });
      leadDot.style('opacity', 0);
      leadPulse.style('opacity', 0);
      showWaypoint(0, () => animateArc(0));
    }

    animateSequence();
  }

  useEffect(() => {
    let resizeTimer: ReturnType<typeof setTimeout>;

    function handleResize() {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        if (worldRef.current) initMap(worldRef.current);
      }, 200);
    }

    fetch(WORLD_ATLAS_URL)
      .then(r => r.json())
      .then((world: Topology) => {
        worldRef.current = world;
        initMap(world);
        window.addEventListener('resize', handleResize);
      })
      .catch(e => console.warn('[GPSCanvas] Failed to load world atlas:', e));

    return () => {
      window.removeEventListener('resize', handleResize);
      cancelAnimationFrame(animFrameRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return <svg ref={svgRef} id="gps-map" className="hero-map-canvas" style={{ width: '100%', height: '100%', display: 'block' }} />;
}
