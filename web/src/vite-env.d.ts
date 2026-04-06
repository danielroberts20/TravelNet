/// <reference types="vite/client" />

// Allow CSS side-effect imports
declare module '*.css';

// Virtual module: travel.yml parsed by the travelYamlPlugin in vite.config.ts
declare module 'virtual:travel-yaml' {
  interface MapRouteStop {
    leg_id: string;
    lon: number;
    lat: number;
    label: string;
    above: boolean;
  }

  interface TravelLeg {
    id: string;
    name: string;
    emoji: string;
    stopover: boolean;
  }

  interface TravelMeta {
    trip_start: string; // 'YYYY-MM-DD'
  }

  interface TravelData {
    meta: TravelMeta;
    map_route: MapRouteStop[];
    legs: TravelLeg[];
  }

  const data: TravelData;
  export default data;
}
