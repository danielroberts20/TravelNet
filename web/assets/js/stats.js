/* TravelNet — stats.js
 * Fetches live stats from the public API, falls back to the committed
 * public_stats.json if the endpoint is unreachable.
 *
 * Stat card elements use data-stat attributes to map to payload fields.
 * e.g. <span data-stat="gps_points"> gets populated with payload.gps_points
 */

const LIVE_URL      = "https://api.travelnet.dev/public/stats";
const FALLBACK_URL  = "/public_stats.json";
const TIMEOUT_MS    = 5000;

/* --------------------------------------------------------
   Fetch with timeout
   -------------------------------------------------------- */
async function fetchWithTimeout(url, ms) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  try {
    const resp = await fetch(url, { signal: controller.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}

/* --------------------------------------------------------
   Load stats — live first, fallback second
   -------------------------------------------------------- */
async function loadStats() {
  let data = null;
  let source = null;

  try {
    data = await fetchWithTimeout(LIVE_URL, TIMEOUT_MS);
    source = "live";
  } catch (e) {
    console.info(`[TravelNet] Live endpoint unreachable (${e.message}), trying fallback.`);
    try {
      data = await fetchWithTimeout(FALLBACK_URL, TIMEOUT_MS);
      source = "fallback";
    } catch (e2) {
      console.warn(`[TravelNet] Fallback also failed (${e2.message}). Stats unavailable.`);
    }
  }

  if (data) {
    console.info(`[TravelNet] Stats loaded from ${source}.`);
    applyStats(data);
  }
}

/* --------------------------------------------------------
   Format helpers
   -------------------------------------------------------- */
function fmt(value) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return value.toLocaleString();
  return value;
}

function fmtDate(iso) {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleDateString("en-GB", {
      day: "numeric", month: "short", year: "numeric",
      hour: "2-digit", minute: "2-digit", timeZone: "UTC"
    }) + " UTC";
  } catch {
    return iso;
  }
}

/* --------------------------------------------------------
   Apply stats to DOM
   -------------------------------------------------------- */
function applyStats(data) {
  // Generic data-stat elements
  const map = {
    "gps_points":         fmt(data.gps_points),
    "health_records":     fmt(data.health_records),
    "transactions":       fmt(data.transactions),
    "days_travelling":    fmt(data.days_travelling),
    "countries_visited":  fmt(data.countries_visited),
    "total_countries":    fmt(data.total_countries),
    "last_synced":        fmtDate(data.last_synced),
    "generated_at":       fmtDate(data.generated_at),
    "status":             fmtStatus(data.status),
  };

  Object.entries(map).forEach(([key, value]) => {
    document.querySelectorAll(`[data-stat="${key}"]`).forEach(el => {
      el.textContent = value;
    });
  });

  // Current leg
  applyCurrentLeg(data.current_leg);

  // Nav status dot + eyebrow
  applyTripStatus(data.status, data.current_leg);

  // Sync info line
  const syncEl = document.getElementById("stats-sync-info");
  if (syncEl && data.last_synced) {
    syncEl.textContent = `Last synced: ${fmtDate(data.last_synced)}`;
    syncEl.style.display = "";
  }

  // Timeline legs
  applyTimeline(data);
}

function fmtStatus(status) {
  const labels = {
    pre_departure: "Pre-departure",
    travelling:    "Travelling",
    finished:      "Trip complete",
  };
  return labels[status] || status || "—";
}

function applyCurrentLeg(leg) {
  const nameEl  = document.getElementById("current-leg-name");
  const emojEl  = document.getElementById("current-leg-emoji");
  const subEl   = document.querySelectorAll("[data-stat='current_leg_name']");

  if (leg) {
    if (nameEl)  nameEl.textContent  = leg.name;
    if (emojEl)  emojEl.textContent  = leg.emoji;
    subEl.forEach(el => el.textContent = leg.name);

    // Sub-label on the days_travelling stat card
    const daysCard = document.getElementById("stat-days-sublabel");
    if (daysCard) daysCard.textContent = `${leg.emoji} ${leg.name}${leg.stopover ? " (stopover)" : ""}`;
  }
}

function applyTripStatus(status, leg) {
  // Nav eyebrow dot colour
  const dot = document.querySelector(".nav-status-dot");
  if (dot) {
    if (status === "travelling") {
      dot.style.background = "var(--accent-teal)";
      dot.title = leg ? `Currently in: ${leg.name}` : "Travelling";
    } else if (status === "finished") {
      dot.style.background = "var(--accent-orange)";
      dot.title = "Trip complete";
    } else {
      dot.style.background = "var(--accent)";
      dot.title = "Pre-departure";
    }
  }

  // Hero eyebrow text
  const eyebrow = document.getElementById("hero-status-text");
  if (eyebrow) {
    if (status === "travelling" && leg) {
      eyebrow.textContent = `Live — currently in ${leg.name} ${leg.emoji}`;
    } else if (status === "finished") {
      eyebrow.textContent = "Trip complete — full dataset available";
    } else {
      eyebrow.textContent = "Pre-departure — launching June 2026";
    }
  }
}

function applyTimeline(data) {
  // Mark timeline legs as done/active based on status in payload
  // Timeline dots have data-leg-id attributes matching travel.yml leg ids
  const legs = document.querySelectorAll("[data-leg-id]");
  if (!legs.length) return;

  legs.forEach(el => {
    const id = el.dataset.legId;
    el.classList.remove("done", "active");

    if (data.current_leg && data.current_leg.id === id) {
      el.classList.add("active");
    }
    // A leg is "done" if it appears before the current leg in the DOM
    // and is not the current leg itself
  });

  // Mark all legs before the active one as done
  let foundActive = false;
  Array.from(legs).reverse().forEach(el => {
    if (data.current_leg && el.dataset.legId === data.current_leg.id) {
      foundActive = true;
      return;
    }
    if (foundActive) el.classList.add("done");
  });
}

/* --------------------------------------------------------
   Boot — set dashes immediately, then fetch
   -------------------------------------------------------- */
document.addEventListener("DOMContentLoaded", () => {
  // Set all data-stat elements to dash immediately while loading
  document.querySelectorAll("[data-stat]").forEach(el => {
    if (el.textContent.trim() === "" || el.textContent.trim() === "0") {
      el.textContent = "—";
    }
  });

  loadStats();
});