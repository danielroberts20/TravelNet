/**
 * FogOfWarMap — interactive GPS fog-of-war built on deck.gl + maplibre-gl.
 *
 * Basemap: Carto Positron (light, clean, Apple Maps-style).
 *
 * Zoom ≤ 5 (country view):
 *   Every country gets a fog overlay. Visited ones receive a subtle warm
 *   tint; unvisited stay dark. Country detection uses d3-geo geoContains
 *   against world-atlas 50m TopoJSON, subsampled to ≤500 GPS points.
 *
 * Zoom > 5 (track view):
 *   Full-world SolidPolygonLayer fog with holes cut at GPS cluster centres
 *   (~9 km radius per cluster). A ScatterplotLayer rendered above the fog
 *   with additive blending adds a soft glow/halo at each hole edge.
 *
 * Transitions: both layer groups are always present; opacity crossfades
 * over 400 ms when crossing the zoom threshold so there is no hard swap.
 */

import { useState, useEffect, useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { SolidPolygonLayer, ScatterplotLayer } from '@deck.gl/layers';
import type { MapViewState } from '@deck.gl/core';
import Map from 'react-map-gl';
import maplibregl from 'maplibre-gl';
import * as topojson from 'topojson-client';
import type { Topology, Objects } from 'topojson-specification';
import { geoContains } from 'd3-geo';

const FOG_OF_WAR_URL = 'https://api.travelnet.dev/public/fog-of-war';
const WORLD_ATLAS_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-50m.json';
const POSITRON_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json';
const ZOOM_THRESHOLD = 5;

const INITIAL_VIEW_STATE: MapViewState = {
  longitude: 175,
  latitude: 5,
  zoom: 3,
  pitch: 0,
  bearing: 0,
};

// Zoom-in fog: dark semi-transparent overlay covering the entire world
const FOG_FILL: [number, number, number, number] = [0, 0, 0, 165];
// Zoom-out visited country: subtle warm tint on the light basemap
const VISITED_TINT: [number, number, number, number] = [255, 210, 80, 60];
// Zoom-out unvisited country: dark fog tile on the light basemap
const UNVISITED_FOG: [number, number, number, number] = [0, 0, 0, 150];
// Glow halo colour: warm, very low opacity — rendered additively to soften hole edges
const GLOW: [number, number, number, number] = [255, 210, 120, 35];

type Position2 = [number, number];

// World bounding box as CCW outer ring (GeoJSON exterior convention)
const WORLD_OUTER: Position2[] = [
  [-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90],
];

// CW-wound circle — used as a hole ring inside the world polygon
function circleHole(lon: number, lat: number, r: number, n = 32): Position2[] {
  const coords: Position2[] = [];
  for (let i = 0; i <= n; i++) {
    const a = (i / n) * Math.PI * 2;
    // CW: east → south → west → north
    coords.push([lon + r * Math.cos(a), lat - r * Math.sin(a)]);
  }
  return coords;
}

// Grid-cluster GPS points to reduce fog-hole count while preserving coverage
function gridCluster(points: Position2[], cellDeg: number): Position2[] {
  const cells: Record<string, { sumLon: number; sumLat: number; count: number }> = {};
  for (const [lon, lat] of points) {
    const key = `${Math.floor(lon / cellDeg)},${Math.floor(lat / cellDeg)}`;
    const c = cells[key];
    if (c) { c.sumLon += lon; c.sumLat += lat; c.count++; }
    else cells[key] = { sumLon: lon, sumLat: lat, count: 1 };
  }
  return Object.values(cells).map(c => [c.sumLon / c.count, c.sumLat / c.count] as Position2);
}

interface FlatPoly {
  polygon: Position2[][];
  featureIdx: number;
}

function flattenCountries(features: GeoJSON.Feature[]): FlatPoly[] {
  return features.flatMap((f, idx) => {
    if (!f.geometry) return [];
    if (f.geometry.type === 'Polygon') {
      return [{ polygon: f.geometry.coordinates as Position2[][], featureIdx: idx }];
    }
    if (f.geometry.type === 'MultiPolygon') {
      return f.geometry.coordinates.map(poly => ({ polygon: poly as Position2[][], featureIdx: idx }));
    }
    return [];
  });
}

export default function FogOfWarMap() {
  const [viewState, setViewState] = useState<MapViewState>(INITIAL_VIEW_STATE);
  const [gpsPoints, setGpsPoints] = useState<Position2[]>([]);
  const [worldFeatures, setWorldFeatures] = useState<GeoJSON.Feature[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    Promise.all([
      fetch(FOG_OF_WAR_URL)
        .then(r => { if (!r.ok) throw new Error(`${r.status}`); return r.json(); })
        .catch(() => { setError(true); return { features: [] }; }),
      fetch(WORLD_ATLAS_URL).then(r => r.json()),
    ]).then(([fogData, worldData]) => {
      const pts: Position2[] = ((fogData as GeoJSON.FeatureCollection).features ?? [])
        .map((f: GeoJSON.Feature) => f.geometry?.type === 'Point'
          ? (f.geometry as GeoJSON.Point).coordinates as Position2
          : null)
        .filter((p): p is Position2 => p !== null);

      setGpsPoints(pts);

      const countries = topojson.feature(
        worldData as Topology<Objects>,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (worldData as any).objects.countries,
      ) as unknown as GeoJSON.FeatureCollection;

      setWorldFeatures(countries.features);
      setLoading(false);
    });
  }, []);

  // Determine visited country indices (expensive — memoized, only recomputes on new data)
  const visitedSet = useMemo(() => {
    if (!gpsPoints.length || !worldFeatures.length) return new Set<number>();
    const step = Math.max(1, Math.floor(gpsPoints.length / 500));
    const sample = gpsPoints.filter((_, i) => i % step === 0);
    const visited = new Set<number>();
    worldFeatures.forEach((feature, idx) => {
      if (visited.has(idx)) return;
      for (const pt of sample) {
        if (geoContains(feature as Parameters<typeof geoContains>[0], pt)) {
          visited.add(idx);
          break;
        }
      }
    });
    return visited;
  }, [gpsPoints, worldFeatures]);

  const flatCountries = useMemo(() => flattenCountries(worldFeatures), [worldFeatures]);
  const fogClusters = useMemo(() => gridCluster(gpsPoints, 0.1), [gpsPoints]);

  const zoom = viewState.zoom;
  const isZoomedIn = zoom > ZOOM_THRESHOLD;

  const layers = useMemo(() => {
    if (loading) return [];

    // Holes for the fog polygon: CW circles at each GPS cluster centre
    const HOLE_RADIUS = 0.08; // ~9 km per cluster
    const holes = fogClusters.map(([lon, lat]) => circleHole(lon, lat, HOLE_RADIUS));

    // Both layer groups live in the array at all times; opacity crossfades when
    // isZoomedIn changes so there is no hard visual swap at the threshold.
    const countryOpacity = isZoomedIn ? 0 : 1;
    const fogOpacity = isZoomedIn ? 1 : 0;

    return [
      // --- Country view (zoom-out) ---
      new SolidPolygonLayer<FlatPoly>({
        id: 'countries',
        data: flatCountries,
        getPolygon: d => d.polygon,
        getFillColor: d => visitedSet.has(d.featureIdx) ? VISITED_TINT : UNVISITED_FOG,
        filled: true,
        pickable: false,
        opacity: countryOpacity,
        transitions: { opacity: 400 },
        updateTriggers: { getFillColor: [visitedSet] },
      }),

      // --- Track view (zoom-in): fog mask with holes ---
      new SolidPolygonLayer({
        id: 'fog',
        data: [{ polygon: [WORLD_OUTER, ...holes] }],
        getPolygon: d => d.polygon,
        getFillColor: FOG_FILL,
        filled: true,
        pickable: false,
        opacity: fogOpacity,
        transitions: { opacity: 400 },
      }),

      // --- Soft glow halos (zoom-in) ---
      // Additive blending (SRC_ALPHA + ONE) lightens the dark fog at each hole
      // edge, creating a smooth vignette-style transition instead of a hard cutout.
      new ScatterplotLayer<Position2>({
        id: 'fog-glow',
        data: fogClusters,
        getPosition: d => d,
        getRadius: 12000,      // 12 km geographic radius
        radiusUnits: 'meters',
        radiusMinPixels: 60,   // always at least 60 px wide in screen space
        radiusMaxPixels: 180,
        getFillColor: GLOW,
        pickable: false,
        opacity: fogOpacity,
        transitions: { opacity: 400 },
        // Additive blending (src_alpha * src + 1 * dst): brightens fog at hole edges.
        // Uses luma.gl v9 string-based DeviceParameters (blendFunc[] is luma.gl v8).
        parameters: {
          blend: true,
          blendColorOperation: 'add',
          blendColorSrcFactor: 'src-alpha',
          blendColorDstFactor: 'one',
        } as object,
      }),
    ];
  }, [loading, isZoomedIn, flatCountries, visitedSet, fogClusters]);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <DeckGL
        viewState={viewState}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        onViewStateChange={(p: any) => {
          const next = p.viewState as MapViewState;
          console.log('[FogOfWarMap] zoom:', next.zoom.toFixed(2), '| track view:', next.zoom > ZOOM_THRESHOLD);
          setViewState(next);
        }}
        controller={true}
        layers={layers}
      >
        <Map
          mapLib={maplibregl as unknown as Parameters<typeof Map>[0]['mapLib']}
          mapStyle={POSITRON_STYLE}
          reuseMaps
        />
      </DeckGL>

      {loading && (
        <div style={{
          position: 'absolute', inset: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: 'rgba(255,255,255,0.7)',
          color: 'rgba(0,0,0,0.4)',
          fontFamily: 'var(--font-mono)',
          fontSize: 12,
          letterSpacing: '0.08em',
          pointerEvents: 'none',
        }}>
          Loading GPS data...
        </div>
      )}

      {!loading && error && (
        <div style={{
          position: 'absolute', bottom: 16, left: '50%', transform: 'translateX(-50%)',
          background: 'rgba(255,255,255,0.85)',
          color: 'rgba(0,0,0,0.35)',
          fontFamily: 'var(--font-mono)',
          fontSize: 11,
          padding: '6px 12px',
          borderRadius: 6,
          pointerEvents: 'none',
        }}>
          GPS data unavailable — map showing basemap only
        </div>
      )}

      <div style={{
        position: 'absolute', bottom: 16, right: 16,
        background: 'rgba(255,255,255,0.75)',
        color: 'rgba(0,0,0,0.4)',
        fontFamily: 'var(--font-mono)',
        fontSize: 10,
        padding: '4px 10px',
        borderRadius: 4,
        letterSpacing: '0.06em',
        pointerEvents: 'none',
      }}>
        {isZoomedIn ? 'GPS track view' : 'Country view'} · {zoom.toFixed(1)}z
      </div>
    </div>
  );
}
