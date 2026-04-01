/**
 * Single source of truth for trip data — values are derived from travel.yml
 * at build time via the travelYamlPlugin in vite.config.ts.  Do not
 * hard-code trip metadata here; edit travel.yml instead.
 */

import travelData from 'virtual:travel-yaml';

export const TRIP_START = travelData.meta.trip_start + 'T06:00:00Z';

export const GITHUB_REPO = 'https://github.com/danielroberts20/TravelNet';
export const TREVOR_REPO = 'https://github.com/danielroberts20/Trevor-For-TravelNet';
export const DOCS_URL = 'https://docs.travelnet.dev';
export const PERSONAL_SITE = 'https://danielroberts20.github.io';

export interface Leg {
  id: string;
  name: string;
  emoji: string;
  stopover: boolean;
}

export const LEGS: Leg[] = travelData.legs.map(l => ({
  id: l.id,
  name: l.name,
  emoji: l.emoji,
  stopover: l.stopover,
}));

export interface Waypoint {
  lon: number;
  lat: number;
  label: string;
  above: boolean; // true = label above dot, false = below
}

// Ordered route stops for the homepage arc animation.
// Sourced from map_route in travel.yml.
export const MAP_WAYPOINTS: Waypoint[] = travelData.map_route.map(s => ({
  lon: s.lon,
  lat: s.lat,
  label: s.label,
  above: s.above,
}));
