import { useState, useEffect } from 'react';

const LIVE_URL = 'https://api.travelnet.dev/public/stats';
const FALLBACK_URL = '/public_stats.json';
const TIMEOUT_MS = 5000;

export interface CurrentLeg {
  id: string;
  name: string;
  emoji: string;
  stopover: boolean;
}

export interface Stats {
  status: 'pre_departure' | 'travelling' | 'finished' | string;
  days_travelling: number;
  countries_visited: number;
  total_countries: number;
  current_leg: CurrentLeg | null;
  gps_points: number;
  health_records: number;
  transactions: number;
  last_synced: string | null;
  generated_at: string | null;
}

async function fetchWithTimeout(url: string, ms: number): Promise<Stats> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    const resp = await fetch(url, { signal: controller.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json() as Stats;
  } finally {
    clearTimeout(timer);
  }
}

export function useStats() {
  const [stats, setStats] = useState<Stats | null>(null);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      let data: Stats | null = null;
      try {
        data = await fetchWithTimeout(LIVE_URL, TIMEOUT_MS);
      } catch {
        try {
          data = await fetchWithTimeout(FALLBACK_URL, TIMEOUT_MS);
        } catch {
          // Stats unavailable — UI stays in dash state
        }
      }
      if (!cancelled && data) setStats(data);
    })();

    return () => { cancelled = true; };
  }, []);

  return stats;
}

export function fmt(value: number | null | undefined): string {
  if (value === null || value === undefined) return '—';
  return value.toLocaleString();
}

export function fmtDate(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleDateString('en-GB', {
      day: 'numeric', month: 'short', year: 'numeric',
      hour: '2-digit', minute: '2-digit', timeZone: 'UTC',
    }) + ' UTC';
  } catch {
    return iso;
  }
}
