import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          'deck': ['@deck.gl/core', '@deck.gl/layers', '@deck.gl/react'],
          'maplibre': ['maplibre-gl', 'react-map-gl'],
          'geo': ['d3', 'topojson-client'],
        },
      },
    },
  },
});
