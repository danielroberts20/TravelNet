/**
 * FogOfWarMap — interactive GPS fog-of-war built on deck.gl + maplibre-gl.
 *
 * This is the Kepler.gl tech stack (deck.gl layers + maplibre basemap) used
 * directly for precise rendering control. The project uses Kepler.gl as its
 * canonical mapping foundation; future pages will add the KeplerGl component
 * for exploratory data viz (Explorer, ML pages).
 *
 * Zoom ≤ 5 (world view):
 *   Visited countries are lit amber/gold. Rest of world is dark.
 *   Country detection: point-in-polygon via d3-geo geoContains against
 *   world-atlas 110m TopoJSON, subsampled to ≤500 GPS points.
 *
 * Zoom > 5 (track view):
 *   Dark SolidPolygonLayer covering the whole world with holes cut around
 *   GPS track clusters. Only areas physically visited are revealed.
 */

import { useState, useEffect, useMemo } from 'react';
import DeckGL from '@deck.gl/react';
import { SolidPolygonLayer } from '@deck.gl/layers';
import type { MapViewState } from '@deck.gl/core';
import Map from 'react-map-gl';
import maplibregl from 'maplibre-gl';
import * as topojson from 'topojson-client';
import type { Topology, Objects } from 'topojson-specification';
import { geoContains } from 'd3-geo';

const FOG_OF_WAR_URL = 'https://api.travelnet.dev/public/fog-of-war';
const WORLD_ATLAS_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json';
const DARK_MATTER_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

const INITIAL_VIEW_STATE: MapViewState = {
  longitude: 175,
  latitude: 5,
  zoom: 3,
  pitch: 0,
  bearing: 0,
};

// Visited country fill: warm amber/gold
const AMBER: [number, number, number, number] = [255, 159, 10, 185];
// Unvisited country fill: very dark, slightly blue
const DARK_COUNTRY: [number, number, number, number] = [12, 12, 18, 235];
// Fog fill: almost-black with slight opacity to let basemap labels through at edges
const FOG_FILL: [number, number, number, number] = [8, 8, 14, 218];

type Position2 = [number, number];

// World bounding box as CCW outer ring (GeoJSON convention: CCW = exterior)
const WORLD_OUTER: Position2[] = [
  [-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90],
];

// Circle polygon — CW winding (GeoJSON convention: CW = hole / interior ring)
function circleHole(lon: number, lat: number, r: number, n = 24): Position2[] {
  const coords: Position2[] = [];
  for (let i = 0; i <= n; i++) {
    const a = (i / n) * Math.PI * 2;
    // CW: cos(a), -sin(a) — right → south → left → north
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

    // Subsample to ≤500 points for country detection performance
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

  // Pre-flatten country features for SolidPolygonLayer (memoized)
  const flatCountries = useMemo(() => flattenCountries(worldFeatures), [worldFeatures]);

  // GPS clusters for track-level fog holes (memoized)
  const fogClusters = useMemo(() => gridCluster(gpsPoints, 0.1), [gpsPoints]);

  const zoom = viewState.zoom;
  const isZoomedIn = zoom > 5;

  const layers = useMemo(() => {
    if (loading) return [];

    if (!isZoomedIn) {
      // Country-level: single SolidPolygonLayer, amber for visited, dark for rest
      return [
        new SolidPolygonLayer<FlatPoly>({
          id: 'countries',
          data: flatCountries,
          getPolygon: d => d.polygon,
          getFillColor: d => visitedSet.has(d.featureIdx) ? AMBER : DARK_COUNTRY,
          filled: true,
          pickable: false,
          updateTriggers: { getFillColor: [visitedSet] },
        }),
      ];
    }

    // Track-level: dark world polygon with GPS cluster holes
    const HOLE_RADIUS = 0.08; // degrees (~9 km) — reveals a corridor around each track cluster
    const holes = fogClusters.map(([lon, lat]) => circleHole(lon, lat, HOLE_RADIUS));

    return [
      new SolidPolygonLayer({
        id: 'fog',
        data: [{ polygon: [WORLD_OUTER, ...holes] }],
        getPolygon: d => d.polygon,
        getFillColor: FOG_FILL,
        filled: true,
        pickable: false,
      }),
    ];
  }, [loading, isZoomedIn, flatCountries, visitedSet, fogClusters]);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <DeckGL
        viewState={viewState}
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        onViewStateChange={(p: any) => setViewState(p.viewState as MapViewState)}
        controller={true}
        layers={layers}
      >
        <Map
          mapLib={maplibregl as unknown as Parameters<typeof Map>[0]['mapLib']}
          mapStyle={DARK_MATTER_STYLE}
          reuseMaps
        />
      </DeckGL>

      {loading && (
        <div style={{
          position: 'absolute', inset: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          background: 'rgba(8,8,14,0.75)',
          color: 'rgba(255,255,255,0.45)',
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
          background: 'rgba(8,8,14,0.85)',
          color: 'rgba(255,255,255,0.4)',
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
        background: 'rgba(8,8,14,0.75)',
        color: 'rgba(255,255,255,0.35)',
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
