/* TravelNet Docs — sidebar toggle (add to main.js or keep separate) */

function toggleSection(titleEl) {
  const section = titleEl.closest('.sidebar-section');
  section.classList.toggle('open');
}
