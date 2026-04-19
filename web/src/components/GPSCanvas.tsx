/**
 * GPSCanvas — animated great-circle arc route map.
 *
 * Replaces the original D3 SVG implementation with the same deck.gl +
 * maplibre stack used by FogOfWarMap, keeping identical animation behaviour:
 *
 *   1. Origin waypoint dot appears immediately.
 *   2. Each arc "draws itself" over ARC_DURATION ms.
 *   3. On arc completion the destination dot settles with a pulse ring.
 *   4. Brief pause, then the next arc starts.
 *   5. After the final arc, RESET_PAUSE then full restart.
 *
 * Waypoints are sourced from travel.yml via the virtual:travel-yaml module
 * (see vite.config.ts) — edit travel.yml to change the route, not this file.
 */

import { useEffect, useRef, useState, useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { PathLayer, ScatterplotLayer, TextLayer } from '@deck.gl/layers';
import type { MapViewState } from '@deck.gl/core';
import Map from 'react-map-gl';
import maplibregl from 'maplibre-gl';
import * as d3 from 'd3';
import { MAP_WAYPOINTS, type Waypoint } from '../data/travel';

const DARK_MATTER_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const ARC_DURATION  = 2000; // ms to draw each arc
const PAUSE_BETWEEN = 400;  // ms pause after each arc completes
const RESET_PAUSE   = 1500; // ms before restarting the whole sequence
const PATH_STEPS    = 80;   // great-circle interpolation steps per arc

// Pacific-centred view so the full Pacific route sits comfortably on screen
const VIEW_STATE: MapViewState = {
  longitude: 150,
  latitude: 10,
  zoom: 1.0,
  pitch: 0,
  bearing: 0,
};

// Arc colour: iOS blue
const BLUE:  [number, number, number, number] = [10, 132, 255, 220];
// Waypoint dot colour: iOS green
const GREEN: [number, number, number, number] = [48, 209, 88, 255];
// Label colour: white, slightly muted
const LABEL: [number, number, number, number] = [255, 255, 255, 153];

type Pos = [number, number];

// Compute a great-circle path between two [lon, lat] points.
// Normalises each point so longitudes are continuous (no ±180 jump),
// which prevents deck.gl PathLayer from drawing the arc the wrong way
// around the globe when crossing the antimeridian.
function greatCircle(from: Pos, to: Pos, steps: number): Pos[] {
  const interp = d3.geoInterpolate(from, to);
  const path: Pos[] = [];
  for (let i = 0; i <= steps; i++) {
    const [lon, lat] = interp(i / steps) as Pos;
    if (path.length === 0) {
      path.push([lon, lat]);
    } else {
      const prevLon = path[path.length - 1][0];
      let adjustedLon = lon;
      while (adjustedLon - prevLon >  180) adjustedLon -= 360;
      while (prevLon - adjustedLon >  180) adjustedLon += 360;
      path.push([adjustedLon, lat]);
    }
  }
  return path;
}

interface ArcDef {
  fullPath: Pos[]; // PATH_STEPS + 1 points
}

function buildArcs(waypoints: Waypoint[]): ArcDef[] {
  const arcs: ArcDef[] = [];
  for (let i = 0; i < waypoints.length - 1; i++) {
    const a = waypoints[i];
    const b = waypoints[i + 1];
    arcs.push({ fullPath: greatCircle([a.lon, a.lat], [b.lon, b.lat], PATH_STEPS) });
  }
  return arcs;
}

interface AnimState {
  phase: 'drawing' | 'pausing';
  arcIdx: number;
  fraction: number;      // 0–1 progress of current arc (only in 'drawing')
  settledCount: number;  // how many waypoints have a solid dot + label
  elapsed: number;       // ms elapsed in current phase (only in 'pausing')
}

export default function GPSCanvas() {
  // frame is incremented on every RAF tick to drive re-renders
  const [frame, setFrame] = useState(0);
  const animRef  = useRef<AnimState>({ phase: 'pausing', arcIdx: 0, fraction: 0, settledCount: 1, elapsed: 0 });
  const lastTime = useRef(0);
  const rafId    = useRef(0);

  // Pre-compute arc paths once — they only change when waypoints change
  const arcs = useMemo(() => buildArcs(MAP_WAYPOINTS), []);

  useEffect(() => {
    function tick(now: number) {
      const dt = Math.min(now - (lastTime.current || now), 50); // cap to 50 ms
      lastTime.current = now;
      const s = animRef.current;

      if (s.phase === 'drawing') {
        s.fraction = Math.min(1, s.fraction + dt / ARC_DURATION);
        if (s.fraction >= 1) {
          // Arc complete — settle the destination waypoint
          s.settledCount = s.arcIdx + 2;
          s.phase = 'pausing';
          s.elapsed = 0;
        }
      } else {
        // pausing
        s.elapsed += dt;
        const isLast = s.arcIdx === arcs.length - 1;
        const pauseEnd = (s.fraction >= 1 && isLast) ? RESET_PAUSE : PAUSE_BETWEEN;

        if (s.elapsed >= pauseEnd) {
          s.elapsed = 0;
          if (s.fraction >= 1 && isLast) {
            // Full reset
            s.arcIdx = 0;
            s.fraction = 0;
            s.settledCount = 1;
            // stay in 'pausing' for one PAUSE_BETWEEN before restarting
          } else {
            if (s.fraction >= 1) s.arcIdx++; // advance after a completed arc
            s.fraction = 0;
            s.phase = 'drawing';
          }
        }
      }

      setFrame(f => (f + 1) & 0x7fff);
      rafId.current = requestAnimationFrame(tick);
    }

    rafId.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(rafId.current);
  }, [arcs]);

  // ── Layer computation (runs every frame) ─────────────────────────────────
  const s = animRef.current;

  // Settled arcs: all arcs before the current one (fully drawn)
  const settledPaths: Pos[][] = arcs.slice(0, s.arcIdx).map(a => a.fullPath);

  // Current arc: partial or full path depending on phase
  let currentPath: Pos[] = [];
  const arc = arcs[s.arcIdx];
  if (arc) {
    if (s.phase === 'pausing' && s.fraction >= 1) {
      // Arc completed — move it into settled and leave currentPath empty
      settledPaths.push(arc.fullPath);
    } else if (s.fraction > 0) {
      const steps = Math.min(PATH_STEPS, Math.floor(s.fraction * PATH_STEPS));
      currentPath = arc.fullPath.slice(0, steps + 1);
    }
  }

  // Lead dot: tip of the currently-drawing arc
  const leadPos: Pos | null = (s.phase === 'drawing' && currentPath.length > 1)
    ? currentPath[currentPath.length - 1]
    : null;

  // Settled waypoint dots + labels
  const settledDots = MAP_WAYPOINTS.slice(0, s.settledCount);

  // Pulse ring: animates using wall-clock time so it runs independently of arc timing
  const pt = performance.now() / 1200;
  const pulsePhase = (Math.sin(pt * Math.PI * 2) + 1) / 2; // 0–1, ~0.83 Hz
  const pulseR     = 4 + pulsePhase * 10;                    // 4–14 px
  const pulseAlpha = Math.round((1 - pulsePhase) * 0.6 * 255); // 153–0

  const layers = [
    // Settled arcs (fully drawn)
    settledPaths.length > 0 && new PathLayer<Pos[]>({
      id: 'arcs-settled',
      data: settledPaths,
      getPath: d => d,
      getColor: BLUE,
      getWidth: 2,
      widthUnits: 'pixels',
      widthMinPixels: 1,
      pickable: false,
    }),

    // Currently-drawing arc (partial)
    currentPath.length > 1 && new PathLayer<Pos[]>({
      id: 'arc-current',
      data: [currentPath],
      getPath: d => d,
      getColor: BLUE,
      getWidth: 2,
      widthUnits: 'pixels',
      widthMinPixels: 1,
      pickable: false,
    }),

    // Pulse rings around settled waypoints
    settledDots.length > 0 && new ScatterplotLayer<Waypoint>({
      id: 'dots-pulse',
      data: settledDots,
      getPosition: d => [d.lon, d.lat],
      getRadius: pulseR,
      radiusUnits: 'pixels',
      getFillColor: [48, 209, 88, pulseAlpha],
      pickable: false,
    }),

    // Settled waypoint solid dots
    settledDots.length > 0 && new ScatterplotLayer<Waypoint>({
      id: 'dots-settled',
      data: settledDots,
      getPosition: d => [d.lon, d.lat],
      getRadius: 4,
      radiusUnits: 'pixels',
      getFillColor: GREEN,
      pickable: false,
    }),

    // Lead dot (moving tip of the current arc)
    leadPos && new ScatterplotLayer<Pos>({
      id: 'dot-lead',
      data: [leadPos],
      getPosition: d => d,
      getRadius: 5,
      radiusUnits: 'pixels',
      getFillColor: BLUE,
      pickable: false,
    }),

    // Labels for settled waypoints
    settledDots.length > 0 && new TextLayer<Waypoint>({
      id: 'labels',
      data: settledDots,
      getPosition: d => [d.lon, d.lat],
      getText: d => d.label,
      getSize: 11,
      getColor: LABEL,
      fontFamily: '-apple-system, "Helvetica Neue", sans-serif',
      fontWeight: '500',
      getTextAnchor: 'start',
      getAlignmentBaseline: 'center',
      getPixelOffset: d => [8, d.above ? -10 : 10],
      pickable: false,
    }),
  ];

  // Suppress unused-variable warning — frame is consumed by reading animRef
  void frame;

  return (
    <DeckGL
      viewState={VIEW_STATE}
      controller={false}
      layers={layers}
      style={{ width: '100%', height: '100%' }}
    >
      <Map
        mapLib={maplibregl as unknown as Parameters<typeof Map>[0]['mapLib']}
        mapStyle={DARK_MATTER_STYLE}
        reuseMaps
      />
    </DeckGL>
  );
}
