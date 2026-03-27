/* TravelNet Docs — search.js
 * Client-side full-text search using Lunr.js.
 * Fetches /search.json (built by Jekyll), indexes it with Lunr,
 * and renders results in the sidebar dropdown.
 */

(function () {
  const input      = document.getElementById('docs-search-input');
  const resultsEl  = document.getElementById('search-results');

  if (!input || !resultsEl) return;

  let index  = null;
  let pages  = [];
  let loaded = false;

  // Load Lunr from CDN
  const script = document.createElement('script');
  script.src = 'https://cdnjs.cloudflare.com/ajax/libs/lunr.js/2.3.9/lunr.min.js';
  script.onload = loadIndex;
  document.head.appendChild(script);

  async function loadIndex() {
    try {
      const resp = await fetch('/search.json');
      pages = await resp.json();

      index = lunr(function () {
        this.ref('url');
        this.field('title', { boost: 10 });
        this.field('section', { boost: 5 });
        this.field('content');

        pages.forEach(page => this.add(page));
      });

      loaded = true;
    } catch (e) {
      console.warn('[TravelNet] Search index failed to load:', e);
    }
  }

  function search(query) {
    if (!loaded || !query.trim()) return [];
    try {
      const results = index.search(query + '~1'); // fuzzy match
      return results.slice(0, 8).map(r => pages.find(p => p.url === r.ref)).filter(Boolean);
    } catch {
      // Lunr throws on invalid query syntax
      return [];
    }
  }

  function renderResults(results, query) {
    if (!query.trim()) {
      resultsEl.classList.remove('visible');
      return;
    }

    if (!results.length) {
      resultsEl.innerHTML = `<p class="search-no-results">No results for "<strong>${escHtml(query)}</strong>"</p>`;
      resultsEl.classList.add('visible');
      return;
    }

    resultsEl.innerHTML = results.map(page => `
      <a href="${page.url}" class="search-result-item">
        <div class="search-result-section">${escHtml(page.section || 'Docs')}</div>
        <div class="search-result-title">${escHtml(page.title)}</div>
        <div class="search-result-excerpt">${escHtml(truncate(page.content, 120))}</div>
      </a>
    `).join('');

    resultsEl.classList.add('visible');
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function truncate(str, n) {
    if (!str) return '';
    return str.length > n ? str.slice(0, n) + '…' : str;
  }

  let debounceTimer;
  input.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      renderResults(search(input.value), input.value);
    }, 150);
  });

  input.addEventListener('focus', () => {
    if (input.value.trim()) renderResults(search(input.value), input.value);
  });

  // Close on outside click
  document.addEventListener('click', e => {
    if (!input.contains(e.target) && !resultsEl.contains(e.target)) {
      resultsEl.classList.remove('visible');
    }
  });

  // Keyboard nav
  input.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      resultsEl.classList.remove('visible');
      input.blur();
    }
  });
})();
