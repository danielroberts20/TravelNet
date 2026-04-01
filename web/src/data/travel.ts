// Single source of truth for trip data — mirrors travel.yml
// (travel.yml is copied to _data/ at Jekyll build time; this is the React equivalent)

export const TRIP_START = '2026-06-11T06:00:00Z';

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

export const LEGS: Leg[] = [
  { id: 'usa',         name: 'USA',         emoji: '🇺🇸', stopover: false },
  { id: 'fiji',        name: 'Fiji',        emoji: '🇫🇯', stopover: true  },
  { id: 'australia',   name: 'Australia',   emoji: '🇦🇺', stopover: false },
  { id: 'new_zealand', name: 'New Zealand', emoji: '🇳🇿', stopover: false },
  { id: 'se_asia',     name: 'SE Asia',     emoji: '🌏',  stopover: false },
  { id: 'canada',      name: 'Canada',      emoji: '🇨🇦', stopover: false },
];

// Waypoints for the animated GPS canvas on the homepage.
// Ordered Philadelphia → Seattle → Fiji → Sydney → Auckland → Bangkok → Vancouver
// to trace the Pacific route visually.
export const MAP_WAYPOINTS = [
  { label: 'Philadelphia', lon: -75.2,  lat: 39.9  },
  { label: 'Seattle',      lon: -122.3, lat: 47.6  },
  { label: 'Fiji',         lon: 178.4,  lat: -18.1 },
  { label: 'Sydney',       lon: 151.2,  lat: -33.9 },
  { label: 'Auckland',     lon: 174.8,  lat: -36.9 },
  { label: 'Bangkok',      lon: 100.5,  lat: 13.7  },
  { label: 'Vancouver',    lon: -123.1, lat: 49.3  },
];
