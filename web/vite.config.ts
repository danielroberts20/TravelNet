import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';

const __dirname = dirname(fileURLToPath(import.meta.url));
const TRAVEL_YML = resolve(__dirname, '../travel.yml');

/**
 * Vite virtual module plugin — exposes travel.yml as a typed import.
 *
 * Usage in source files:
 *   import travelData from 'virtual:travel-yaml';
 *
 * The file is parsed at build time (and during dev HMR).  travel.ts is the
 * bridge between this raw parsed object and typed TypeScript exports.
 */
function travelYamlPlugin() {
  const VIRTUAL_ID = 'virtual:travel-yaml';
  const RESOLVED_ID = '\0' + VIRTUAL_ID;

  return {
    name: 'travel-yaml',
    resolveId(id: string) {
      if (id === VIRTUAL_ID) return RESOLVED_ID;
    },
    load(this: { addWatchFile: (path: string) => void }, id: string) {
      if (id === RESOLVED_ID) {
        this.addWatchFile(TRAVEL_YML); // re-run on travel.yml changes
        const raw = readFileSync(TRAVEL_YML, 'utf-8');
        const data = yaml.load(raw);
        return `export default ${JSON.stringify(data)};`;
      }
    },
  };
}

export default defineConfig({
  plugins: [react(), travelYamlPlugin()],
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          'deck': ['@deck.gl/core', '@deck.gl/layers', '@deck.gl/react'],
          'maplibre': ['maplibre-gl', 'react-map-gl'],
          'geo': ['d3'],
        },
      },
    },
  },
});
