import Map from 'react-map-gl';
import maplibregl from 'maplibre-gl';

const DARK_MATTER_STYLE = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';

export default function GPSCanvas() {
  return (
    <Map
      mapLib={maplibregl as unknown as Parameters<typeof Map>[0]['mapLib']}
      mapStyle={DARK_MATTER_STYLE}
      initialViewState={{ longitude: 0, latitude: 20, zoom: 1.5 }}
      style={{ width: '100%', height: '100%' }}
      interactive={false}
    />
  );
}
