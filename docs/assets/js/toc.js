/* TravelNet Docs — toc.js
 * Auto-generates the right-sidebar TOC from h2/h3 headings
 * in .docs-prose, and highlights the active heading on scroll.
 */

(function () {
  const prose   = document.querySelector('.docs-prose');
  const tocList = document.getElementById('toc-list');
  const toc     = document.getElementById('docs-toc');

  if (!prose || !tocList) return;

  const headings = Array.from(prose.querySelectorAll('h2, h3'));

  if (headings.length < 2) {
    // Not enough headings to warrant a TOC
    if (toc) toc.style.display = 'none';
    return;
  }

  // Build TOC items
  headings.forEach((heading, i) => {
    // Ensure heading has an id
    if (!heading.id) {
      heading.id = heading.textContent
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-|-$/g, '');
    }

    const li = document.createElement('li');
    li.className = `toc-item ${heading.tagName === 'H3' ? 'h3' : ''}`;
    li.dataset.id = heading.id;

    const a = document.createElement('a');
    a.href = `#${heading.id}`;
    a.textContent = heading.textContent;
    a.addEventListener('click', e => {
      e.preventDefault();
      heading.scrollIntoView({ behavior: 'smooth', block: 'start' });
      history.pushState(null, '', `#${heading.id}`);
    });

    li.appendChild(a);
    tocList.appendChild(li);
  });

  // Highlight active heading on scroll
  const items = Array.from(tocList.querySelectorAll('.toc-item'));

  const observer = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        items.forEach(item => item.classList.remove('active'));
        const active = items.find(item => item.dataset.id === entry.target.id);
        if (active) {
          active.classList.add('active');
          // Scroll TOC to keep active item visible
          active.scrollIntoView({ block: 'nearest' });
        }
      }
    });
  }, {
    rootMargin: '-52px 0px -60% 0px',
    threshold: 0,
  });

  headings.forEach(h => observer.observe(h));
})();
