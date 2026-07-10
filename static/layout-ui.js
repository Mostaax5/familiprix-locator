// ── Layout config helpers ─────────────────────────────────────────────────────
function defaultLayoutConfig(maxSection=0, maxShelf=0, maxPosition=0) {
  const sCount = Math.max(0, Number(maxSection) || 0);
  const shCount = Math.max(0, Number(maxShelf) || 0);
  const pCount = Math.max(0, Number(maxPosition) || 0);
  const makeSections = () => Array.from({length: sCount}, () => ({
    shelves: Array.from({length: shCount}, () => pCount),
    labels:  Array.from({length: shCount}, () => '')
  }));
  return {
    sides: {Gauche: {sections: makeSections()}, Droite: {sections: makeSections()}},
    facade_a: {shelves: [], labels: []},   // end cap at entrance of aisle
    facade_b: {shelves: [], labels: []},   // end cap at far end of aisle
    presentoirs: [],                       // freestanding displays in corridor
  };
}

function buildLayoutWithSideCounts(leftSections=0, rightSections=0, shelfCount=0, positionCount=0) {
  const makeSections = count => Array.from({length: Math.max(0, Number(count) || 0)}, () => ({
    shelves: Array.from({length: Math.max(0, Number(shelfCount) || 0)}, () => Math.max(0, Number(positionCount) || 0))
  }));
  return {sides: {Gauche: {sections: makeSections(leftSections)}, Droite: {sections: makeSections(rightSections)}}};
}

function getLayoutMetrics(config) {
  const sides = config?.sides || {};
  const leftSections = Array.isArray(sides.Gauche?.sections) ? sides.Gauche.sections : [];
  const rightSections = Array.isArray(sides.Droite?.sections) ? sides.Droite.sections : [];
  const allSections = [...leftSections, ...rightSections];
  const maxSection = Math.max(leftSections.length, rightSections.length, 0);
  const maxShelf = Math.max(0, ...allSections.map(s => Array.isArray(s?.shelves) ? s.shelves.length : 0));
  const maxPosition = Math.max(0, ...allSections.flatMap(s => Array.isArray(s?.shelves) ? s.shelves : [0]));
  return {max_section: String(maxSection), max_shelf: String(maxShelf), max_position: String(maxPosition)};
}

function normalizeLayoutConfig(config, maxSection=0, maxShelf=0, maxPosition=0) {
  const base = defaultLayoutConfig(maxSection, maxShelf, maxPosition);
  const source = (config && typeof config === 'object') ? config : {};
  const sourceSides = (source.sides && typeof source.sides === 'object') ? source.sides : {};
  ['Gauche', 'Droite'].forEach(side => {
    const sections = Array.isArray(sourceSides[side]?.sections) ? sourceSides[side].sections : [];
    base.sides[side].sections = sections
      .map(section => {
        if (!Array.isArray(section?.shelves)) return null;
        const shelves = section.shelves.map(v => Math.max(0, Number(v) || 0));
        const rawLabels = Array.isArray(section.labels) ? section.labels : [];
        const labels = shelves.map((_, i) => String(rawLabels[i] || ''));
        return {shelves, labels};
      })
      .filter(Boolean);
  });
  // Normalize a simple fixture: {shelves: [...], labels: [...]}
  const normFixture = fd => {
    const rawSh = Array.isArray(fd?.shelves) ? fd.shelves : [];
    const shelves = rawSh.map(v => Math.max(0, Number(v) || 0));
    const rawL = Array.isArray(fd?.labels) ? fd.labels : [];
    return {shelves, labels: shelves.map((_, i) => String(rawL[i] || ''))};
  };
  // Façades: two end caps per aisle
  base.facade_a = normFixture(source.facade_a);
  base.facade_b = normFixture(source.facade_b);
  // Présentoirs: freestanding displays in corridor
  const normPresentoirFacade = (f, fallbackName) => {
    const fname = String(f?.name || fallbackName).trim() || fallbackName;
    const sh = Array.isArray(f?.shelves) ? f.shelves.map(v => Math.max(0, Number(v)||0)) : [];
    const rawL = Array.isArray(f?.labels) ? f.labels : [];
    return {name: fname, shelves: sh, labels: sh.map((_, i) => String(rawL[i]||''))};
  };
  base.presentoirs = (Array.isArray(source.presentoirs) ? source.presentoirs : [])
    .map(p => {
      if (!p || typeof p !== 'object') return null;
      const name = String(p.name || 'Présentoir').trim() || 'Présentoir';
      // Support both facades array (new) and flat shelves/labels (legacy)
      let facades;
      if (Array.isArray(p.facades) && p.facades.length > 0) {
        facades = p.facades.map((f, i) => normPresentoirFacade(f, `Façade ${i+1}`));
      } else {
        facades = [normPresentoirFacade(p, 'Façade 1')];
      }
      return {name, facades};
    }).filter(Boolean);
  return base;
}

// Per-aisle / per-side / home-brand product counts, built once per cache
// version and reused. Layout-only edits (e.g. +/- a position) don't touch the
// product cache, so repeated plan renders reuse these without rescanning.
let _planCountsVersion = -1;
let _planCounts = null;
function planSummaryCounts() {
  if (_planCounts === null || _planCountsVersion !== lastProductsRefreshAt) {
    const aisle = new Map(), home = new Map(), side = new Map();
    const section = new Map(), sectionHome = new Map();   // keyed aisle|side|sectionNumber
    for (const p of allProductsCache) {
      const a = String(p.aisle);
      const isHome = isHomeBrand(p.brand);
      aisle.set(a, (aisle.get(a) || 0) + 1);
      if (isHome) home.set(a, (home.get(a) || 0) + 1);
      const sk = a + '|' + p.side;
      side.set(sk, (side.get(sk) || 0) + 1);
      const ck = sk + '|' + String(p.section);
      section.set(ck, (section.get(ck) || 0) + 1);
      if (isHome) sectionHome.set(ck, (sectionHome.get(ck) || 0) + 1);
    }
    _planCounts = {aisle, home, side, section, sectionHome};
    _planCountsVersion = lastProductsRefreshAt;
  }
  return _planCounts;
}

function countProductsInAisle(aisle) {
  return planSummaryCounts().aisle.get(String(aisle)) || 0;
}

// Count scannable slots without allocating a slot object for each one (matches
// buildSlotsFromConfig with no side filter: façades + both sides, no présentoirs).
function countSlotsFromConfig(config) {
  let n = 0;
  const sum = fx => { for (const c of (fx?.shelves || [])) n += Math.max(0, Number(c) || 0); };
  sum(config.facade_a);
  for (const sideName of ['Gauche', 'Droite']) {
    for (const sec of (config.sides?.[sideName]?.sections || [])) sum(sec);
  }
  sum(config.facade_b);
  return n;
}

function sortMapLayouts() {
  mapLayouts.sort((a, b) => (Number(a.aisle) || 0) - (Number(b.aisle) || 0) || String(a.aisle).localeCompare(String(b.aisle)));
}

function syncLayoutRecord(layout) {
  if (!layout) return null;
  layout.config = normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position);
  const metrics = getLayoutMetrics(layout.config);
  layout.max_section = metrics.max_section;
  layout.max_shelf = metrics.max_shelf;
  layout.max_position = metrics.max_position;
  layout.product_count = countProductsInAisle(layout.aisle);
  return layout;
}

function markLayoutDirty(aisle) { dirtyLayoutAisles.add(String(aisle)); }
function clearLayoutDirty(aisle) { dirtyLayoutAisles.delete(String(aisle)); }
function hasDirtyLayouts() { return dirtyLayoutAisles.size > 0; }

// Normalizing a layout config is a deep rebuild. The rayon fields call this 3×
// per keystroke, so memoize the last result by (aisle, config-reference). When a
// layout mutates, syncLayoutRecord swaps in a new config object → reference
// changes → cache misses and re-normalizes once. All callers are read-only.
let _alcAisle = null, _alcRaw = null, _alcNorm = null;
function getAisleLayoutConfig(aisle) {
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  const raw = layout ? layout.config : null;
  if (_alcNorm && String(aisle) === _alcAisle && raw === _alcRaw) return _alcNorm;
  _alcNorm = normalizeLayoutConfig(raw, layout?.max_section, layout?.max_shelf, layout?.max_position);
  _alcAisle = String(aisle);
  _alcRaw = raw;
  return _alcNorm;
}

function getConfiguredLayoutForAisle(aisle) {
  return getAisleLayoutConfig(aisle);
}

// ── Slot helpers ──────────────────────────────────────────────────────────────
function buildSlotsFromConfig(aisle, config, sideFilter=null) {
  const slots = [];
  const addFixture = (sideName, fixture) => {
    if (sideFilter && sideName !== sideFilter) return;
    (fixture?.shelves || []).forEach((posCount, shi) => {
      for (let p = 1; p <= Math.max(0, Number(posCount) || 0); p++) {
        slots.push({aisle: String(aisle), side: sideName, section: '1', shelf: String(shi+1), position: String(p)});
      }
    });
  };
  // Scan order: Façade A → Côté A → Côté B → Façade B
  addFixture('Façade A', config.facade_a);
  ['Gauche', 'Droite'].forEach(side => {
    if (sideFilter && side !== sideFilter) return;
    const sections = config.sides[side].sections || [];
    sections.forEach((section, si) => {
      (section.shelves || []).forEach((positionCount, shi) => {
        for (let p = 1; p <= Math.max(0, Number(positionCount) || 0); p++) {
          slots.push({aisle: String(aisle), side, section: String(si + 1), shelf: String(shi + 1), position: String(p)});
        }
      });
    });
  });
  addFixture('Façade B', config.facade_b);
  // Présentoirs: standalone — each façade is a separate side, only when filtering
  (config.presentoirs || []).forEach(pres => {
    (pres.facades || []).forEach(facade => {
      const sideName = `${pres.name} - ${facade.name}`;
      if (sideFilter && sideName !== sideFilter) return;
      if (!sideFilter) return; // exclude from sequential scan
      (facade.shelves || []).forEach((posCount, shi) => {
        for (let p = 1; p <= Math.max(0, Number(posCount) || 0); p++) {
          slots.push({aisle: String(aisle), side: sideName, section: '1', shelf: String(shi+1), position: String(p)});
        }
      });
    });
  });
  return slots;
}

function compareSlotLocations(a, b) {
  // Façade A → Côté A → Côté B → Façade B → (presentoirs excluded from scan)
  const sideOrder = {'Façade A': 0, 'Gauche': 1, 'Droite': 2, 'Façade B': 3};
  const checks = [
    (Number(a.aisle) || 0) - (Number(b.aisle) || 0),
    (sideOrder[a.side] ?? 99) - (sideOrder[b.side] ?? 99),
    (Number(a.section) || 1) - (Number(b.section) || 1),
    (Number(a.shelf) || 1) - (Number(b.shelf) || 1),
    (Number(a.position) || 1) - (Number(b.position) || 1)
  ];
  return checks.find(v => v !== 0) || 0;
}

function getAllScanSlots() {
  const layouts = (mapLayouts.length ? mapLayouts : [{
    aisle: String(cursor.aisle),
    config: defaultLayoutConfig(cursor.maxSection, cursor.maxShelf, cursor.maxPosition),
    max_section: String(cursor.maxSection),
    max_shelf: String(cursor.maxShelf),
    max_position: String(cursor.maxPosition)
  }]).slice().sort((a, b) => Number(a.aisle) - Number(b.aisle) || String(a.aisle).localeCompare(String(b.aisle)));
  return layouts.flatMap(layout => {
    // Reuse the already-normalized config (the common case) instead of a fresh
    // deep rebuild every call; slot building only reads numeric shelf counts.
    const cfg = (layout.config && layout.config.sides && layout.config.facade_a)
      ? layout.config
      : normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position);
    return buildSlotsFromConfig(layout.aisle, cfg);
  });
}

function syncCursorLimitsForAisle(aisle) {
  const config = getAisleLayoutConfig(aisle);
  cursor.maxSection = Math.max(config.sides.Gauche.sections.length, config.sides.Droite.sections.length, 0);
  cursor.maxShelf = Math.max(
    0,
    ...config.sides.Gauche.sections.map(s => s.shelves.length),
    ...config.sides.Droite.sections.map(s => s.shelves.length)
  );
  cursor.maxPosition = Math.max(
    0,
    ...config.sides.Gauche.sections.flatMap(s => s.shelves),
    ...config.sides.Droite.sections.flatMap(s => s.shelves)
  );
}

function setCursorFromSlot(slot) {
  cursor.aisle = Number(slot.aisle) || cursor.aisle;
  cursor.side = slot.side || cursor.side;
  cursor.section = Number(slot.section) || 1;
  cursor.shelf = Number(slot.shelf) || 1;
  cursor.position = Number(slot.position) || 1;
  syncCursorLimitsForAisle(cursor.aisle);
}

function findBestCursorSlot() {
  const slots = getAllScanSlots();
  if (!slots.length) return null;
  const current = {aisle: cursor.aisle, side: cursor.side, section: cursor.section, shelf: cursor.shelf, position: cursor.position};
  return slots.find(slot => compareSlotLocations(slot, current) >= 0) || slots[slots.length - 1];
}

function normalizeCursorToLayout() {
  const slot = findBestCursorSlot();
  if (slot) setCursorFromSlot(slot);
}

function countProductsOutsideLayout(aisle, config) {
  return allProductsCache.filter(product => {
    if (String(product.aisle) !== String(aisle)) return false;
    const sideConfig = config.sides[String(product.side)] || {sections: []};
    const si = Math.max(0, Number(product.section || 1) - 1);
    const shi = Math.max(0, Number(product.shelf || 1) - 1);
    const pos = Number(product.position || 0);
    if (si >= sideConfig.sections.length) return true;
    const shelves = sideConfig.sections[si]?.shelves || [];
    if (shi >= shelves.length) return true;
    return pos < 1 || pos > Number(shelves[shi] || 0);
  }).length;
}

function confirmLayoutReduction(aisle, nextConfig, subjectLabel) {
  const removed = countProductsOutsideLayout(aisle, nextConfig);
  if (!removed) return confirm(`${subjectLabel} ?`);
  return confirm(`${subjectLabel} ?\n\nCela supprimera aussi ${removed} produit(s) qui ne rentreront plus dans cette structure.`);
}

// ── Cache refresh ─────────────────────────────────────────────────────────────
async function refreshProductsCache(force=false) {
  if (!force && allProductsCache.length && (Date.now() - lastProductsRefreshAt) < 30000) return allProductsCache;
  try {
    allProductsCache = await apiGetProducts();
    mapLayouts.forEach(layout => syncLayoutRecord(layout));
    lastProductsRefreshAt = Date.now();
  } catch (e) {}
  return allProductsCache;
}

async function refreshLayoutsCache(force=false) {
  if (!force && hasDirtyLayouts()) return mapLayouts;
  if (!force && mapLayouts.length && (Date.now() - lastLayoutsRefreshAt) < 30000) return mapLayouts;
  try {
    mapLayouts = await apiGetLayoutAisles();
    mapLayouts = mapLayouts.map(layout => syncLayoutRecord({
      ...layout,
      config: normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position)
    }));
    sortMapLayouts();
    lastLayoutsRefreshAt = Date.now();
  } catch (e) {}
  return mapLayouts;
}

// ── Cursor (used by the Plan-tab "point de départ" editor) ────────────────────
function aiProviderLabel() {
  return backendInfo.ai_provider_label || 'IA';
}

function cursorLabel() {
  return `Allée ${cursor.aisle} - ${sideDisplayLabel(cursor.side)} - Section ${cursor.section} - Tablette ${cursor.shelf} - Pos. ${cursor.position}`;
}

function updateCursorUi() {
  // #cursorText was removed when the scan tab moved to the rayon picker.
  const el = document.getElementById('cursorText');
  if (el) el.textContent = cursorLabel();
  localStorage.setItem(STORAGE_KEYS.cursor, JSON.stringify(cursor));
}

function loadCursor() {
  const saved = localStorage.getItem(STORAGE_KEYS.cursor);
  if (saved) {
    try { Object.assign(cursor, JSON.parse(saved)); }
    catch (e) { localStorage.removeItem(STORAGE_KEYS.cursor); }
  }
  updateCursorUi();
}

// ── Draft persistence ─────────────────────────────────────────────────────────
function persistScanDraft() {
  localStorage.setItem(STORAGE_KEYS.scanDraft, JSON.stringify({
    barcode: document.getElementById('scanInput').value.trim()
  }));
}

function loadScanDraft() {
  const saved = localStorage.getItem(STORAGE_KEYS.scanDraft);
  if (!saved) return;
  try {
    const draft = JSON.parse(saved);
    if (draft.barcode) document.getElementById('scanInput').value = draft.barcode;
  } catch (e) {}
}

function persistAddDraft() {
  localStorage.setItem(STORAGE_KEYS.addDraft, JSON.stringify({
    aisle: document.getElementById('mapAisle').value.trim(),
    leftSections: document.getElementById('mapLeftSections').value.trim(),
    rightSections: document.getElementById('mapRightSections').value.trim(),
    initialShelves: document.getElementById('mapInitialShelves').value.trim(),
    initialPositions: document.getElementById('mapInitialPositions').value.trim()
  }));
}

function loadAddDraft() {
  const saved = localStorage.getItem(STORAGE_KEYS.addDraft);
  if (!saved) return;
  try {
    const draft = JSON.parse(saved);
    document.getElementById('mapAisle').value = draft.aisle || '';
    document.getElementById('mapLeftSections').value = draft.leftSections ?? '0';
    document.getElementById('mapRightSections').value = draft.rightSections ?? '0';
    document.getElementById('mapInitialShelves').value = draft.initialShelves ?? '0';
    document.getElementById('mapInitialPositions').value = draft.initialPositions ?? '0';
  } catch (e) {}
}

function persistClientDraft() {
  localStorage.setItem(STORAGE_KEYS.clientDraft, JSON.stringify({
    question: document.getElementById('clientQuestion')?.value.trim() || ''
  }));
}

function loadClientDraft() {
  const saved = localStorage.getItem(STORAGE_KEYS.clientDraft);
  if (!saved) return;
  try {
    const draft = JSON.parse(saved);
    document.getElementById('clientQuestion').value = draft.question || '';
  } catch (e) {}
}

// ── App shell state ───────────────────────────────────────────────────────────
function updateBackendStatusInfo() {
  const target = document.getElementById('backendStatusInfo');
  if (!target) return;
  const msgs = [];
  if (backendInfo.shared_sync) {
    msgs.push('Base partagee active: PostgreSQL. Les appareils connectes lisent la meme base.');
  } else {
    msgs.push('Base locale active: SQLite. Pour une vraie synchronisation entre appareils, connectez Render Postgres et reglez DATABASE_URL.');
  }
  msgs.push(backendInfo.ai_enabled
    ? `Aide client IA active via ${aiProviderLabel()}.`
    : 'Aide client IA inactive tant que GEMINI_API_KEY n’est pas configurée.');
  if (Number(backendInfo.duplicate_slots || 0) > 0) msgs.push(`Attention: ${backendInfo.duplicate_slots} position(s) contiennent déjà plusieurs produits.`);
  if (Number(backendInfo.duplicate_barcodes || 0) > 0) msgs.push(`Attention: ${backendInfo.duplicate_barcodes} code(s)-barres sont dupliques dans la base.`);
  target.textContent = msgs.join(' ');
}

function updateAppShellState() {
  const standalone = isStandaloneApp();
  document.getElementById('appModeLabel').textContent = `${standalone ? 'Mode app' : 'Mode web'} - ${backendInfo.label || 'Base serveur'}`;
  document.getElementById('installHint').textContent = standalone
    ? 'L app garde votre position et vos formulaires meme si Safari se ferme.'
    : 'Sur mobile: ouvrez le site puis ajoutez-le a l ecran d accueil pour un acces rapide.';
  const session = loadEditorSession();
  document.getElementById('editorSessionInfo').textContent = session?.username && session.username !== 'appareil'
    ? `Cet appareil signe les changements avec le nom ${session.username}.`
    : 'Aucun mot de passe. Si vous entrez un nom, il sera garde sur cet appareil pour suivre qui a change quoi.';
  document.getElementById('editorName').value = session?.username === 'appareil' ? '' : (session?.username || '');
  updateBackendStatusInfo();
}

function updateNetworkStatus() {
  const online = navigator.onLine;
  document.getElementById('networkStatus').innerHTML = online
    ? '<span class="status-dot"></span>En ligne'
    : '<span class="status-dot offline"></span>Hors ligne';
}

// ── Plan start editor ─────────────────────────────────────────────────────────
function setSelectOptions(elementId, values, selectedValue) {
  const select = document.getElementById(elementId);
  if (!select) return;
  const items = Array.isArray(values) ? values : [];
  if (!items.length) {
    select.innerHTML = '<option value="">Aucune</option>';
    select.value = '';
    select.disabled = true;
    return;
  }
  select.disabled = false;
  select.innerHTML = items.map(v => `<option value="${esc(v)}">${esc(v)}</option>`).join('');
  const next = items.includes(String(selectedValue)) ? String(selectedValue) : String(items[0]);
  select.value = next;
}

function getCursorSelection() {
  return {
    aisle: String(cursor.aisle),
    facing: String(cursor.facing || 'Avant'),
    side: String(cursor.side || 'Gauche'),
    section: String(cursor.section || 1),
    shelf: String(cursor.shelf || 1),
    position: String(cursor.position || 1)
  };
}

function getPlanStartDraft() {
  return planStartDraft ? {...planStartDraft} : getCursorSelection();
}

function readPlanStartSelectionFromDom() {
  const aisleEl = document.getElementById('planScanAisle');
  const sideEl = document.getElementById('planScanSide');
  const sectionEl = document.getElementById('planScanSection');
  const shelfEl = document.getElementById('planScanShelf');
  const positionEl = document.getElementById('planScanPosition');
  const facingEl = document.getElementById('planScanFacing');
  if (!aisleEl || !sideEl || !sectionEl || !shelfEl || !positionEl || !facingEl) return getPlanStartDraft();
  return {
    aisle: String(aisleEl.value || ''),
    facing: String(facingEl.value || 'Avant'),
    side: String(sideEl.value || ''),
    section: String(sectionEl.value || ''),
    shelf: String(shelfEl.value || ''),
    position: String(positionEl.value || '')
  };
}

function getPlanPreviewSelection() {
  if (document.getElementById('add')?.classList.contains('active')) {
    const draft = readPlanStartSelectionFromDom();
    if (draft.aisle && draft.side && draft.section && draft.shelf && draft.position) return draft;
  }
  return getCursorSelection();
}

function slotValueKey(slot) {
  return [slot.side, slot.section, slot.shelf, slot.position].join('|');
}

function setPlanSlotOptions(aisle, facing, selectedSlot) {
  const select = document.getElementById('planScanSlot');
  if (!select) return;
  const config = getConfiguredLayoutForAisle(aisle);
  const slots = buildSlotsFromConfig(aisle, config);
  if (!slots.length) {
    select.innerHTML = '<option value="">Aucun slot</option>';
    select.value = ''; select.disabled = true; return;
  }
  select.disabled = false;
  select.innerHTML = slots.map(slot => {
    const value = slotValueKey(slot);
    const label = `${sideDisplayLabel(slot.side)} - Section ${slot.section} - Tablette ${slot.shelf} - Position ${slot.position}`;
    return `<option value="${esc(value)}">${esc(label)}</option>`;
  }).join('');
  const currentValue = slotValueKey(selectedSlot);
  const available = slots.map(slotValueKey);
  select.value = available.includes(currentValue) ? currentValue : available[0];
}

function applySelectedPlanSlot() {
  const rawValue = document.getElementById('planScanSlot')?.value || '';
  const [side, section, shelf, position] = rawValue.split('|');
  if (!side || !section || !shelf || !position) return;
  document.getElementById('planScanSide').value = side;
  document.getElementById('planScanSection').value = section;
  document.getElementById('planScanShelf').value = shelf;
  document.getElementById('planScanPosition').value = position;
  planStartDraft = {...readPlanStartSelectionFromDom(), side, section, shelf, position};
  renderScanPathPreview();
}

function slotLabel(slot) {
  return `A${slot.aisle} ${sideDisplayLabel(slot.side)} S${slot.section} T${slot.shelf} P${slot.position}`;
}

function renderScanPathPreview() {
  const div = document.getElementById('scanPathPreview');
  if (!div) return;
  const slots = getAllScanSlots();
  if (!slots.length) {
    div.innerHTML = '<div class="empty" style="padding:1rem 0">Aucun slot de scan pour le moment. Commencez par créer une allée dans le plan.</div>';
    return;
  }
  const previewSelection = getPlanPreviewSelection();
  const currentIndex = slots.findIndex(slot => (
    Number(slot.aisle) === Number(previewSelection.aisle) &&
    slot.side === previewSelection.side &&
    Number(slot.section) === Number(previewSelection.section) &&
    Number(slot.shelf) === Number(previewSelection.shelf) &&
    Number(slot.position) === Number(previewSelection.position)
  ));
  const startIndex = currentIndex >= 0 ? currentIndex : 0;
  const preview = Array.from({length: Math.min(12, slots.length)}, (_, offset) => slots[(startIndex + offset) % slots.length]);
  const isOnPlanTab = document.getElementById('add')?.classList.contains('active');
  div.innerHTML = `
    <div class="small">${isOnPlanTab ? 'Depart selectionne' : 'Position actuelle'}: <strong>${esc(`Allée ${previewSelection.aisle} - ${sideDisplayLabel(previewSelection.side)} - Section ${previewSelection.section} - Tablette ${previewSelection.shelf} - Pos. ${previewSelection.position}`)}</strong></div>
    <div class="small">Total de slots dans le plan: <strong>${slots.length}</strong></div>
    <div class="slot-preview">${preview.map((slot, index) => `<span class="slot-chip ${index === 0 ? 'active' : ''}">${index === 0 ? 'Maintenant' : `Puis ${index}`} ${esc(slotLabel(slot))}</span>`).join('')}</div>
  `;
}

function buildUniformSections(sectionCount, shelfCount, positionCount) {
  const sc = Math.max(0, Number(shelfCount) || 0);
  return Array.from({length: Math.max(0, Number(sectionCount) || 0)}, () => ({
    shelves: Array.from({length: sc}, () => Math.max(0, Number(positionCount) || 0)),
    labels:  Array.from({length: sc}, () => '')
  }));
}

function applySideTemplate(aisle, side) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const sectionCount = document.getElementById(`sideTemplateSections-${aisle}-${side}`).value;
  const shelfCount = document.getElementById(`sideTemplateShelves-${aisle}-${side}`).value;
  const positionCount = document.getElementById(`sideTemplatePositions-${aisle}-${side}`).value;
  const newSections = buildUniformSections(sectionCount, shelfCount, positionCount);
  // Preserve accroche labels where the shelf still exists — accroches stay.
  const oldSections = layout.config.sides[side]?.sections || [];
  newSections.forEach((sec, si) => {
    sec.labels = sec.shelves.map((_, shi) => (oldSections[si] && oldSections[si].labels && oldSections[si].labels[shi]) || '');
  });
  const nextConfig = normalizeLayoutConfig({
    sides: {...layout.config.sides, [side]: {sections: newSections}}
  }, layout.max_section, layout.max_shelf, layout.max_position);
  if (!confirmLayoutReduction(aisle, nextConfig, `Appliquer ce modèle uniforme au ${sideDisplayLabel(side)}`)) return;
  layout.config = nextConfig;
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSide(aisle, side);   // whole side rebuilt from the template
}

function renderPlanStartEditor() {
  const draft = getPlanStartDraft();
  const aisleOptions = mapLayouts.map(layout => String(layout.aisle)).sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
  setSelectOptions('planScanAisle', aisleOptions, draft.aisle);
  document.getElementById('planScanFacing').value = draft.facing || 'Avant';
  updatePlanStartSelectors();
  renderScanPathPreview();
}

function updatePlanStartSelectors(changedLevel='') {
  const currentDraft = readPlanStartSelectionFromDom();
  const draft = {...getPlanStartDraft(), ...currentDraft};
  const aisle = document.getElementById('planScanAisle').value;
  const config = getConfiguredLayoutForAisle(aisle);

  // Build ordered side list: Façade A → Côté A → Côté B → Façade B
  const sideOpts = [];
  if ((config.facade_a?.shelves || []).length) sideOpts.push({v:'Façade A', l:'🔲 Façade A'});
  ['Gauche','Droite'].forEach(s => { if ((config.sides[s]?.sections||[]).length) sideOpts.push({v:s, l:sideDisplayLabel(s)}); });
  if ((config.facade_b?.shelves || []).length) sideOpts.push({v:'Façade B', l:'🔲 Façade B'});

  const sideSelected = changedLevel === 'aisle' ? sideOpts[0]?.v : draft.side;
  {
    const el = document.getElementById('planScanSide');
    if (!sideOpts.length) { el.innerHTML='<option value="">Aucun</option>'; el.value=''; el.disabled=true; }
    else {
      el.disabled = false;
      el.innerHTML = sideOpts.map(s=>`<option value="${esc(s.v)}">${esc(s.l)}</option>`).join('');
      el.value = sideOpts.some(s=>s.v===sideSelected) ? sideSelected : sideOpts[0].v;
    }
  }
  const side = document.getElementById('planScanSide').value;
  const isFacade = side === 'Façade A' || side === 'Façade B';
  const fixture  = side === 'Façade A' ? config.facade_a : side === 'Façade B' ? config.facade_b : null;

  // Sections
  const sections = isFacade ? ['1'] : ((config.sides[side]?.sections)||[]).map((_,i)=>String(i+1));
  const sectionSelected = ['aisle','side'].includes(changedLevel) ? sections[0] : draft.section;
  setSelectOptions('planScanSection', sections, sectionSelected);
  document.getElementById('planScanSection').disabled = isFacade;

  // Shelves
  const sectionIndex = Math.max(0, Number(document.getElementById('planScanSection').value||'1') - 1);
  const shelfCount = isFacade ? (fixture?.shelves||[]).length : (config.sides[side]?.sections?.[sectionIndex]?.shelves?.length||0);
  const shelves = Array.from({length:shelfCount},(_,i)=>String(i+1));
  const shelfSelected = ['aisle','side','section'].includes(changedLevel) ? shelves[0] : draft.shelf;
  setSelectOptions('planScanShelf', shelves, shelfSelected);

  // Positions
  const shelfIndex = Math.max(0, Number(document.getElementById('planScanShelf').value||'1')-1);
  const posCount = isFacade
    ? Number((fixture?.shelves||[])[shelfIndex]||0)
    : Number(config.sides[side]?.sections?.[sectionIndex]?.shelves?.[shelfIndex]||0);
  const positions = Array.from({length:posCount},(_,i)=>String(i+1));
  const posSelected = ['aisle','side','section','shelf'].includes(changedLevel) ? positions[0] : draft.position;
  setSelectOptions('planScanPosition', positions, posSelected);

  planStartDraft = {
    aisle: document.getElementById('planScanAisle').value,
    facing: document.getElementById('planScanFacing').value || 'Avant',
    side:     document.getElementById('planScanSide').value,
    section:  document.getElementById('planScanSection').value,
    shelf:    document.getElementById('planScanShelf').value,
    position: document.getElementById('planScanPosition').value
  };
  setPlanSlotOptions(planStartDraft.aisle, planStartDraft.facing, planStartDraft);
  renderScanPathPreview();
}

function savePlanScanStart() {
  const {aisle, side, section, shelf, position, facing} = readPlanStartSelectionFromDom();
  const msg = document.getElementById('planScanStartMsg');
  if (!aisle || !side || !section || !shelf || !position) {
    msg.className = 'msg error';
    msg.textContent = 'Le plan n offre encore aucune position valide pour le scan.';
    return;
  }
  cursor.facing = facing;
  setCursorFromSlot({aisle, side, section, shelf, position});
  planStartDraft = getCursorSelection();
  updateCursorUi();
  msg.className = 'msg success';
  msg.textContent = `Le scan commencera maintenant a ${cursorLabel()}.`;
  renderScanPathPreview();
}

function slotExistsInCurrentPlan(selection) {
  return getAllScanSlots().some(slot => (
    String(slot.aisle) === String(selection.aisle) &&
    String(slot.side) === String(selection.side) &&
    String(slot.section) === String(selection.section) &&
    String(slot.shelf) === String(selection.shelf) &&
    String(slot.position) === String(selection.position)
  ));
}

function ensureCursorStillValid() {
  if (hasDirtyLayouts()) return;
  if (!slotExistsInCurrentPlan(getCursorSelection())) {
    normalizeCursorToLayout();
    updateCursorUi();
  }
}

// Coalesce bursts of mutations (rapid +/- taps, a loop of edits) into a single
// rebuild on the next frame instead of N synchronous full rebuilds — far less
// CPU/heat. renderMapEditor() is still called directly for one-off renders.
let _planUiPending = false;
function refreshPlanUi() {
  if (_planUiPending) return;
  _planUiPending = true;
  window.requestAnimationFrame(() => {
    _planUiPending = false;
    ensureCursorStillValid();
    renderMapEditor();
    renderPlanStartEditor();
  });
}

function captureOpenPlanNodesFromDom() {
  const next = new Set();
  document.querySelectorAll('#mapContent details[data-node-id]').forEach(node => {
    const id = node.dataset.nodeId;
    if (!id) return;
    if (node.open) {
      next.add(id);
    } else if (id.startsWith('planAisle-')) {
      next.add('--closed--' + id); // track explicitly closed aisles
    }
  });
  openPlanNodes = next;
}

function detailsOpenAttr(nodeId) {
  if (openPlanNodes.has('--closed--' + nodeId)) return '';          // explicitly closed
  if (openPlanNodes.has(nodeId)) return ' open';                    // explicitly open
  return nodeId.startsWith('planAisle-') ? ' open' : '';            // default: aisles open
}

function selectNumericField(input) {
  if (!input || input.dataset.autoSelectDone === '1') return;
  input.dataset.autoSelectDone = '1';
  window.requestAnimationFrame(() => {
    try { input.select(); } catch (e) {}
    window.setTimeout(() => { delete input.dataset.autoSelectDone; }, 150);
  });
}

function startScanFromSection(aisle, side, sectionIndex) {
  const config = getAisleLayoutConfig(aisle);
  const section = (config?.sides?.[side]?.sections || [])[sectionIndex];
  if (!section || !(section.shelves || []).length) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Aucune tablette dans cette section pour le moment.</div>';
    return;
  }
  startScanAt(aisle, side, sectionIndex + 1, 1);   // section's first tablette
}

// ── Product card ──────────────────────────────────────────────────────────────
function productCard(p, showDelete=true, showAiButton=true) {
  // Catalog-only products come from the imported planograms and have no shelf yet.
  const catalogOnly = p.catalog_only || !String(p.aisle || '').trim();
  const locationHtml = catalogOnly
    ? `<div class="location" style="color:#64748b">📦 En magasin — position à confirmer</div>`
    : `<div class="location">${[
        `Allée ${esc(p.aisle)}`,
        esc(sideStaffLabel(p.side)),
        `Section ${esc(p.section || '1')}`,
        `Tablette ${esc(p.shelf)}`,
        `Pos. ${esc(p.position)}`
      ].join('<span class="loc-sep"> · </span>')}</div>`;
  const outOfStock = p.in_stock === 0;
  const flipped = p.flipped_label === 1;
  const planoBadge = (p.is_plano ? `<span style="display:inline-block;background:#eef2ff;color:#4338ca;border-radius:6px;padding:1px 7px;font-size:10px;font-weight:700;margin-right:4px">📋 PLANO</span>` : `<span style="display:inline-block;background:#f1f5f9;color:#64748b;border-radius:6px;padding:1px 7px;font-size:10px;font-weight:700;margin-right:4px">HORS-PLANO</span>`)
    + (p.id ? `<button title="Basculer plano / hors-plano" onclick="toggleIsPlano(${p.id},${p.is_plano?'false':'true'})" style="background:none;border:1px solid #cbd5e1;border-radius:5px;color:#475569;cursor:pointer;font-size:9px;padding:1px 5px;font-weight:700">↔ ${p.is_plano?'hors-plano':'plano'}</button>` : '');
  // For hors-plano products: flag a flipped plano étiquette + name the product hidden underneath.
  const flippedRow = (p.id && !p.is_plano) ? `<div class="tool-row" style="margin-top:6px;align-items:center;gap:8px;flex-wrap:wrap">
      <button class="btn btn-outline btn-inline" style="font-size:12px;${flipped?'background:#fef3c7;border-color:#fbbf24;color:#92400e':''}" onclick="toggleFlippedLabel(${p.id},${flipped?'false':'true'})">🔄 Étiquette plano flippée dessous : ${flipped?'OUI':'non'}</button>
      ${flipped ? `<button class="btn btn-outline btn-inline" style="font-size:12px" onclick="editUnderneath(${p.id})">${p.underneath_label ? '✎ '+esc(p.underneath_label) : '+ Produit plano dessous'}</button>` : ''}
    </div>` : '';
  const stockRow = (p.id ? `<div class="tool-row" style="margin-top:6px;align-items:center;gap:8px">
      ${outOfStock
        ? `<span style="color:#c8102e;font-size:12px;font-weight:700">⚠ RUPTURE${p.is_plano ? ' — retirer l étiquette plano' : ''}</span>
           <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="toggleProductStock(${p.id},true)">Remettre en stock</button>`
        : `<button class="btn btn-outline btn-inline" style="font-size:12px;color:#c8102e;border-color:#f1b8c2" onclick="toggleProductStock(${p.id},false)">⚠ Marquer rupture</button>`}
    </div>${flippedRow}` : '');
  return `<div class="card"${outOfStock ? ' style="border-left:4px solid #c8102e"' : ''}>
    ${showDelete && p.id && !catalogOnly ? `<button class="delete-btn" onclick="deleteProduct(${p.id})" title="Supprimer">✕</button>` : ''}
    ${isHomeBrand(p.brand) ? `<div class="home-badge">★ Marque maison Familiprix</div>` : ''}
    <div class="product-layout">
      ${p.image_url ? `<img class="product-thumb" src="${esc(p.image_url)}" alt="Image produit">` : ''}
      <div class="product-info">
        <div style="margin-bottom:3px">${planoBadge}</div>
        <div class="name">${esc(p.name)}</div>
        ${p.brand ? `<div class="product-brand">${esc(p.brand)}</div>` : ''}
        ${locationHtml}
      </div>
    </div>
    <div class="product-footer">
      ${stockRow}
      ${p.barcode ? `<div class="meta-row"><span class="meta-label">Code-barres</span><span class="barcode-text">${esc(p.barcode)}</span></div>` : ''}
      ${p.product_code ? `<div class="meta-row"><span class="meta-label">Code pharmacie</span><span class="barcode-text">${esc(p.product_code)}</span></div>` : ''}
      ${p.facings > 1 ? `<div class="meta-row"><span class="meta-label">Façades</span><span>${esc(p.facings)} positions</span></div>` : ''}
      ${p.last_change_by ? `<div class="meta-row"><span class="meta-label">Modifié par</span><span>${esc(p.last_change_by)}</span></div>` : ''}
      ${p.description ? `<div class="desc-text">${esc(p.description)}</div>` : ''}
      ${p.usage_notes ? `<div class="desc-text">${esc(p.usage_notes)}</div>` : ''}
      ${p.search_terms ? `<div class="meta-row"><span class="meta-label">Mots-clés</span><span class="small">${esc(p.search_terms)}</span></div>` : ''}
      ${p.alternative_suggestions ? `<div class="meta-row"><span class="meta-label">Alternatives</span><span class="small">${esc(p.alternative_suggestions)}</span></div>` : ''}
      ${showAiButton && p.id && backendInfo.ai_enabled ? `<div class="tool-row"><button class="btn btn-outline btn-inline" onclick="enrichStoredProductWithAi(${p.id})">Générer aide client (IA)</button></div>` : ''}
    </div>
  </div>`;
}

async function toggleProductStock(productId, inStock) {
  if (!requireEditorSession('changer le statut de stock')) return;
  const data = await apiSetProductStock(productId, inStock);
  if (data.success !== false && data.product) {
    upsertCachedProduct(normalizeProduct(data.product));
    // Re-render whatever view is visible
    if (document.getElementById('search')?.classList.contains('active')) doSearch();
    else if (document.getElementById('add')?.classList.contains('active')) refreshPlanUi();
    else if (document.getElementById('scan')?.classList.contains('active')) refreshRayonList();
  }
}

function _reRenderActiveView() {
  if (document.getElementById('search')?.classList.contains('active')) doSearch();
  else if (document.getElementById('add')?.classList.contains('active')) refreshPlanUi();
  else if (document.getElementById('scan')?.classList.contains('active')) refreshRayonList();
}

async function toggleFlippedLabel(productId, flipped) {
  if (!requireEditorSession('changer l étiquette')) return;
  const data = await apiSetFlippedLabel(productId, flipped);
  if (data.success !== false && data.product) {
    upsertCachedProduct(normalizeProduct(data.product));
    _reRenderActiveView();
  }
}

async function toggleIsPlano(productId, makePlano) {
  if (!requireEditorSession('changer le statut plano')) return;
  const data = await apiSetIsPlano(productId, makePlano);
  if (data.success !== false && data.product) {
    upsertCachedProduct(normalizeProduct(data.product));
    _reRenderActiveView();
  }
}

async function editUnderneath(productId) {
  if (!requireEditorSession('modifier le produit plano dessous')) return;
  const p = allProductsCache.find(x => Number(x.id) === Number(productId));
  const val = prompt('Produit plano caché dessous (UPC ou nom). Vide = aucun :', p?.underneath_label || '');
  if (val === null) return;
  const data = await apiSetFlippedLabel(productId, true, val.trim());
  if (data.success !== false && data.product) {
    upsertCachedProduct(normalizeProduct(data.product));
    _reRenderActiveView();
  }
}

async function enrichStoredProductWithAi(productId) {
  const cached = allProductsCache.length ? allProductsCache : await refreshProductsCache(true);
  const product = cached.find(item => Number(item.id) === Number(productId)) || {};
  if (!product.id) return;
  const searchDiv = document.getElementById('searchResults');
  searchDiv.insertAdjacentHTML('afterbegin', '<div class="msg info">Generation de l aide client...</div>');
  const assistResult = await apiGenerateProductAssist(product);
  if (!assistResult.success || !assistResult.assist) {
    await doSearch();
    searchDiv.insertAdjacentHTML('afterbegin', `<div class="msg error">${esc(assistResult.error || 'Aide client indisponible.')}</div>`);
    return;
  }
  const nextProduct = {
    ...product,
    description: product.description || assistResult.assist.usage_notes || '',
    search_terms: assistResult.assist.search_terms || product.search_terms || '',
    usage_notes: assistResult.assist.usage_notes || product.usage_notes || '',
    alternative_suggestions: assistResult.assist.alternative_suggestions || product.alternative_suggestions || ''
  };
  const saveResult = await apiUpdateProduct(nextProduct);
  if (!saveResult.success) {
    await doSearch();
    searchDiv.insertAdjacentHTML('afterbegin', `<div class="msg error">${esc(saveResult.error || 'Sauvegarde impossible.')}</div>`);
    return;
  }
  if (saveResult.product) upsertCachedProduct(normalizeProduct(saveResult.product));  // local update, no full refetch
  await doSearch();
  searchDiv.insertAdjacentHTML('afterbegin', '<div class="msg success">Aide client ajoutée au produit.</div>');
}

// ── Plan editor ───────────────────────────────────────────────────────────────

// Products grouped by exact shelf, built once per cache version. Rendering a
// plan asks for dozens of shelves; without this each shelf scanned the whole
// product list (O(shelves × products)). The index auto-rebuilds whenever the
// cache changes (lastProductsRefreshAt moves on every upsert/remove/refresh).
let _shelfIndexVersion = -1;
let _shelfIndex = null;
function productsAtShelf(aisle, side, section, shelf) {
  if (_shelfIndex === null || _shelfIndexVersion !== lastProductsRefreshAt) {
    _shelfIndex = new Map();
    for (const p of allProductsCache) {
      const k = `${p.aisle}|${p.side}|${p.section}|${p.shelf}`;
      let arr = _shelfIndex.get(k);
      if (!arr) { arr = []; _shelfIndex.set(k, arr); }
      arr.push(p);
    }
    _shelfIndexVersion = lastProductsRefreshAt;
  }
  return _shelfIndex.get(`${aisle}|${side}|${section}|${shelf}`) || [];
}

// Move a product to ANY allée/côté/section/tablette/position. The server
// validates the target slot and frees the old one — works across allées.
function openMoveProduct(id) {
  if (!requireEditorSession('déplacer un produit')) return;
  const p = allProductsCache.find(x => Number(x.id) === Number(id));
  if (!p) return;
  const aisles = mapLayouts.map(l => String(l.aisle)).sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
  const overlay = document.createElement('div');
  overlay.className = 'move-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
  overlay.innerHTML = `<div style="background:#fff;border-radius:12px;padding:18px;max-width:380px;width:100%;box-shadow:0 12px 44px rgba(0,0,0,.3)">
    <div style="font-weight:800;margin-bottom:2px">Déplacer « ${esc(p.name)} »</div>
    <div class="small" style="color:#64748b;margin-bottom:10px">Choisissez la nouvelle allée et la position.</div>
    <div class="field"><label class="label">Allée</label>
      <select id="mvAisle">${aisles.map(a => `<option value="${esc(a)}"${a === String(p.aisle) ? ' selected' : ''}>${esc(a)}</option>`).join('')}</select></div>
    <div class="field"><label class="label">Côté</label>
      <select id="mvSide"><option value="Gauche"${p.side === 'Gauche' ? ' selected' : ''}>Côté A</option><option value="Droite"${p.side === 'Droite' ? ' selected' : ''}>Côté B</option></select></div>
    <div class="row3">
      <div class="field"><label class="label">Section</label><input type="number" id="mvSection" min="1" value="${esc(p.section || '1')}"></div>
      <div class="field"><label class="label">Tablette</label><input type="number" id="mvShelf" min="1" value="${esc(p.shelf)}"></div>
      <div class="field"><label class="label">Position</label><input type="number" id="mvPosition" min="1" value="${esc(p.position)}"></div>
    </div>
    <div id="mvMsg" class="small" style="color:#c8102e;min-height:16px"></div>
    <div class="tool-row" style="margin-top:8px">
      <button class="btn btn-inline" onclick="confirmMoveProduct(${p.id})">Déplacer</button>
      <button class="btn btn-outline btn-inline" onclick="this.closest('.move-overlay').remove()">Annuler</button>
    </div>
  </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

async function confirmMoveProduct(id) {
  const p = allProductsCache.find(x => Number(x.id) === Number(id));
  if (!p) return;
  const overlay = document.querySelector('.move-overlay');
  const msg = document.getElementById('mvMsg');
  const payload = {...p,
    aisle: document.getElementById('mvAisle').value,
    side: document.getElementById('mvSide').value,
    section: document.getElementById('mvSection').value || '1',
    shelf: document.getElementById('mvShelf').value,
    position: document.getElementById('mvPosition').value};
  const data = await apiUpdateProduct(payload);
  if (data.success !== false && !data.error) {
    if (data.product) upsertCachedProduct(normalizeProduct(data.product));
    if (overlay) overlay.remove();
    refreshPlanUi();
  } else if (msg) {
    msg.textContent = data.error || 'Déplacement impossible (position occupée ou hors plan ?).';
  }
}

// Move a whole section (tablettes + products) to another allée/côté.
function openMoveSection(aisle, side, sectionIndex) {
  if (!requireEditorSession('déplacer une section')) return;
  const aisles = mapLayouts.map(l => String(l.aisle)).sort((a, b) => Number(a) - Number(b) || a.localeCompare(b));
  const overlay = document.createElement('div');
  overlay.className = 'move-overlay';
  overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px';
  overlay.innerHTML = `<div style="background:#fff;border-radius:12px;padding:18px;max-width:380px;width:100%;box-shadow:0 12px 44px rgba(0,0,0,.3)">
    <div style="font-weight:800;margin-bottom:2px">Déplacer la section ${sectionIndex + 1}</div>
    <div class="small" style="color:#64748b;margin-bottom:10px">Depuis Allée ${esc(aisle)} · ${esc(sideDisplayLabel(side))}. Choisissez l'allée, le côté et la position. Elle emporte ses tablettes et ses produits.</div>
    <div class="field"><label class="label">Allée de destination</label>
      <select id="msAisle">${aisles.map(a => `<option value="${esc(a)}"${a === String(aisle) ? ' selected' : ''}>${esc(a)}</option>`).join('')}</select></div>
    <div class="row2">
      <div class="field"><label class="label">Côté</label>
        <select id="msSide"><option value="Gauche"${side === 'Gauche' ? ' selected' : ''}>Côté A</option><option value="Droite"${side === 'Droite' ? ' selected' : ''}>Côté B</option></select></div>
      <div class="field"><label class="label" title="Position de la section (1 = première)">Position</label>
        <input type="number" id="msPosition" min="1" value="${sectionIndex + 1}"></div>
    </div>
    <div id="msMsg" class="small" style="color:#c8102e;min-height:16px"></div>
    <div class="tool-row" style="margin-top:8px">
      <button class="btn btn-inline" onclick="confirmMoveSection('${esc(aisle)}','${side}',${sectionIndex})">Déplacer</button>
      <button class="btn btn-outline btn-inline" onclick="this.closest('.move-overlay').remove()">Annuler</button>
    </div>
  </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

async function confirmMoveSection(aisle, side, sectionIndex) {
  const overlay = document.querySelector('.move-overlay');
  const msg = document.getElementById('msMsg');
  const target_aisle = document.getElementById('msAisle').value;
  const target_side  = document.getElementById('msSide').value;
  const target_position = document.getElementById('msPosition')?.value || '';
  try {
    const {res, data} = await apiFetch(`/api/layout/aisles/${encodeURIComponent(aisle)}/move-section-to-aisle`, {
      method: 'POST', headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({side, section_index: sectionIndex, target_aisle, target_side, target_position})
    });
    if (res.ok && data.success) {
      if (overlay) overlay.remove();
      await refreshProductsCache(true);   // products + 2 layouts changed
      await refreshLayoutsCache(true);
      renderMapEditor();
    } else if (msg) { msg.textContent = (data && data.error) || 'Déplacement impossible.'; }
  } catch (e) { if (msg) msg.textContent = 'Erreur réseau.'; }
}

function renderShelfProductList(aisle, side, section, shelf, positions) {
  const products = productsAtShelf(String(aisle), side, String(section), String(shelf))
    .slice().sort((a, b) => Number(a.position) - Number(b.position));
  const filled = products.length;
  const total = Number(positions) || 0;
  // "Scanner ici" makes EVERY tablette directly scannable — côté sections,
  // accroches, façades and présentoirs all render their products through here.
  const scanBtn = `<button class="btn btn-outline btn-inline" style="font-size:11px;padding:3px 9px;margin:0 0 5px;width:100%;color:#16a34a;border-color:#16a34a" onclick="startScanAt('${esc(String(aisle))}','${esc(String(side))}','${esc(String(section))}','${esc(String(shelf))}')">▶ Scanner ici</button>`;
  if (!total && !filled) return `<div class="plan-product-list">${scanBtn}</div>`;

  // Mode libre (positions = 0): show all scanned products without fixed slots
  if (!total) {
    return `<div class="plan-product-list">${scanBtn}
      <div style="font-size:10px;color:#8b5cf6;font-weight:600;padding:3px 0 4px">📦 ${filled} produit${filled!==1?'s':''} libre${filled!==1?'s':''}</div>
      ${products.map(p => `<div class="plan-product-item">
        <div class="plan-product-row1">
          <span class="plan-product-name">${esc(p.name)}${p.brand ? ` <span class="plan-product-brand">${esc(p.brand)}</span>` : ''}</span>
          <button title="Déplacer (autre allée/position)" onclick="openMoveProduct(${p.id})" style="margin-left:auto;flex-shrink:0;border:1px solid #cbd5e1;color:#334155;background:#f8fafc;border-radius:5px;padding:2px 7px;cursor:pointer;font-size:11px">⇄</button>
          <button title="Retirer ce produit" onclick="deleteProduct(${p.id})" style="flex-shrink:0;border:1px solid #f1b8c2;color:#c8102e;background:#fff;border-radius:5px;padding:2px 7px;cursor:pointer;font-size:11px">✕</button>
        </div>
        <div class="plan-product-row2">${p.barcode ? esc(p.barcode) : '—'}${p.product_code ? ` · code ${esc(p.product_code)}` : ''}</div>
      </div>`).join('')}
      ${!filled ? `<div class="plan-product-item"><span class="plan-slot-empty">Scannez les produits via le Scan tab</span></div>` : ''}
    </div>`;
  }
  const byPos = {};
  products.forEach(p => { byPos[Number(p.position)] = p; });
  const ae = s => esc(String(s));
  let html = `<div class="plan-product-list">${scanBtn}`;
  for (let pos = 1; pos <= total; pos++) {
    const p = byPos[pos];
    if (p) {
      const canUp   = pos > 1;
      const canDown = pos < total;
      const swapArgs = `'${ae(aisle)}','${ae(side)}','${ae(section)}','${ae(shelf)}'`;
      html += `<div class="plan-product-item">
        <div class="plan-product-row1">
          <span class="plan-pos-badge">${pos}</span>
          <span class="plan-product-name">${esc(p.name)}${p.brand ? ` <span class="plan-product-brand">${esc(p.brand)}</span>` : ''}</span>
          <span style="display:flex;gap:4px;margin-left:auto;flex-shrink:0">
            <button title="Échanger avec la position précédente" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:14px;line-height:1;${canUp?'':'opacity:.25;cursor:default'}" onclick="swapPositions(${swapArgs},${pos},${pos-1})" ${canUp?'':'disabled'}>↑</button>
            <button title="Échanger avec la position suivante" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:14px;line-height:1;${canDown?'':'opacity:.25;cursor:default'}" onclick="swapPositions(${swapArgs},${pos},${pos+1})" ${canDown?'':'disabled'}>↓</button>
            <button title="Déplacer (autre allée/position)" style="background:#f8fafc;border:1px solid #cbd5e1;color:#334155;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:13px;line-height:1" onclick="openMoveProduct(${p.id})">⇄</button>
            <button title="Retirer ce produit" style="background:#fff;border:1px solid #f1b8c2;color:#c8102e;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:13px;line-height:1" onclick="deleteProduct(${p.id})">✕</button>
          </span>
        </div>
        <div class="plan-product-row2">${p.barcode ? esc(p.barcode) : '—'}${p.product_code ? ` · code ${esc(p.product_code)}` : ''}</div>
      </div>`;
    } else {
      html += `<div class="plan-product-item">
        <div class="plan-product-row1">
          <span class="plan-pos-badge">${pos}</span>
          <span class="plan-slot-empty">vide</span>
        </div>
      </div>`;
    }
  }
  products.filter(p => Number(p.position) < 1 || Number(p.position) > total).forEach(p => {
    html += `<div class="plan-product-item" style="background:#fff5f5;border-radius:4px;padding:5px 4px">
      <div class="plan-product-row1">
        <span class="plan-pos-badge" style="color:#c8102e">${esc(String(p.position))}</span>
        <span class="plan-product-name" style="color:#c8102e">${esc(p.name)} <span class="plan-product-brand">hors limite</span></span>
      </div>
      <div class="plan-product-row2" style="color:#f87171">${p.barcode ? esc(p.barcode) : '—'}</div>
    </div>`;
  });
  html += `</div>`;
  return html;
}

// One shelf card (a tablette). Extracted so a single position change can
// re-render just this card instead of the whole plan tree. The id encodes its
// coordinates so rerenderShelfCard can find and replace it.
function renderShelfCard(aisle, side, sectionIndex, shelfIndex, positions, shelfLabel) {
  const shelfFilled = productsAtShelf(String(aisle), side, String(sectionIndex + 1), String(shelfIndex + 1)).length;
  const isLibre = positions === 0;
  const shelfTitle = shelfLabel ? `${isLibre ? '📦 ' : '📎 '}${esc(shelfLabel)}` : (isLibre ? `📦 T${shelfIndex + 1} Libre` : `T${shelfIndex + 1}`);
  const cardBg = isLibre ? 'background:#faf5ff;border-color:#a78bfa' : (shelfLabel ? 'background:#fffbf0;border-color:#fbbf24' : '');
  return `<div class="plan-shelf-card" id="shelfcard-${esc(aisle)}|${esc(side)}|${sectionIndex}|${shelfIndex}" style="${cardBg}">
    <div class="shelf-header" style="gap:4px">
      <span class="shelf-title">${shelfTitle}</span>
      ${isLibre
        ? `<span style="font-size:10px;color:#8b5cf6;font-weight:700">LIBRE · ${shelfFilled} prod.</span>
           <button title="Définir un nombre fixe de positions" style="background:none;border:1px solid #a78bfa;border-radius:4px;color:#8b5cf6;cursor:pointer;font-size:10px;padding:1px 5px"
                   onclick="setShelfPositionCount('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},prompt('Nombre de positions fixes ?','8')||0)">→ Positions fixes</button>`
        : `<button title="Retirer une position" style="background:none;border:1px solid #e2e8f0;border-radius:5px;cursor:pointer;font-size:14px;padding:1px 8px;line-height:1.3;${positions<=1?'opacity:.3;cursor:default':''}" onclick="setShelfPositionCount('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},${positions-1})" ${positions<=1?'disabled':''}>➖</button>
           <input type="number" min="1" value="${positions}" title="Positions"
                 style="width:46px;padding:2px 4px;border:1px solid #e2e8f0;border-radius:5px;font-size:12px;text-align:center"
                 onchange="setShelfPositionCount('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},this.value)"/>
           <button title="Ajouter une position" style="background:none;border:1px solid #e2e8f0;border-radius:5px;cursor:pointer;font-size:14px;padding:1px 8px;line-height:1.3" onclick="setShelfPositionCount('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},${positions+1})">➕</button>
           <span style="font-size:11px;color:#64748b">${shelfFilled} prod.</span>
           <button title="Passer en mode libre (cosmétiques, presentoirs...)" style="background:none;border:1px solid #e2e8f0;border-radius:4px;color:#8b5cf6;cursor:pointer;font-size:10px;padding:1px 5px"
                   onclick="setShelfPositionCount('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},0)">📦 Libre</button>`
      }
      <button onclick="removeShelf('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex})" style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5" title="Supprimer cette tablette">✕ Suppr.</button>
    </div>
    <div style="display:flex;gap:6px;padding:5px 0 4px;border-top:1px solid rgba(0,0,0,.06);margin-top:4px">
      <button class="btn btn-outline btn-inline" style="font-size:12px;flex:1" onclick="moveShelf('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},-1)">↑ Monter</button>
      <button class="btn btn-outline btn-inline" style="font-size:12px;flex:1" onclick="moveShelf('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},1)">↓ Descendre</button>
    </div>
    <details class="struct-details">
      <summary class="struct-toggle" style="font-size:11px">⚙ Nom / étiquette</summary>
      <div class="field" style="margin-top:6px">
        <input type="text" value="${esc(shelfLabel)}" placeholder="Laisser vide = Tablette ${shelfIndex + 1}"
               oninput="setShelfLabel('${esc(aisle)}','${side}',${sectionIndex},${shelfIndex},this.value)"/>
      </div>
    </details>
    ${renderShelfProductList(aisle, side, sectionIndex + 1, shelfIndex + 1, positions)}
  </div>`;
}

// Re-render only one shelf card (used by setShelfPositionCount) instead of the
// whole tree; also refreshes the aisle's slot total. Falls back to a full
// refresh if the card isn't in the DOM.
function rerenderShelfCard(aisle, side, sectionIndex, shelfIndex) {
  const layout = mapLayouts.find(l => String(l.aisle) === String(aisle));
  const section = layout && layout.config && layout.config.sides[side] && layout.config.sides[side].sections[sectionIndex];
  const el = document.getElementById(`shelfcard-${aisle}|${side}|${sectionIndex}|${shelfIndex}`);
  if (!layout || !section || !el) { refreshPlanUi(); return; }
  const positions = Number(section.shelves[shelfIndex]) || 0;
  el.outerHTML = renderShelfCard(aisle, side, sectionIndex, shelfIndex, positions, (section.labels || [])[shelfIndex] || '');
  _updateAisleSlotTotal(aisle);
}

// Keeps the aisle summary's "X slots" number in sync after a targeted update,
// without rebuilding the whole summary.
function _updateAisleSlotTotal(aisle) {
  const layout = mapLayouts.find(l => String(l.aisle) === String(aisle));
  const slotEl = document.getElementById(`aisleSlots-${aisle}`);
  if (layout && slotEl) slotEl.textContent = String(countSlotsFromConfig(layout.config));
}

// One section (its summary + action buttons + shelf grid). Extracted so adding
// a tablette/accroche re-renders just this section, not the whole tree.
function renderSection(aisle, side, sectionIndex, section) {
  const counts = planSummaryCounts();
  const ck = `${aisle}|${side}|${sectionIndex + 1}`;
  const sectionProducts = counts.section.get(ck) || 0;
  const sectionHome = counts.sectionHome.get(ck) || 0;
  const sectionNodeId = `planSection-${aisle}-${side}-${sectionIndex}`;
  return `<details class="tree-node plan-section" data-node-id="${sectionNodeId}"${detailsOpenAttr(sectionNodeId)}>
    <summary>
      <span>Section ${sectionIndex + 1}</span>
      <span class="tree-meta">${sectionProducts} prod. · ${section.shelves.length} T${sectionHome ? ` · <span style="color:#c8102e">★${sectionHome}</span>` : ''}</span>
    </summary>
    <div class="tree-body">
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin:6px 0 6px">
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addShelf('${esc(aisle)}','${side}',${sectionIndex})">➕ Tablette</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addAccrocheToSection('${esc(aisle)}','${side}',${sectionIndex})">📎 Accroche</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="startScanFromSection('${esc(aisle)}','${side}',${sectionIndex})">▶ Scanner ici</button>
      </div>
      <div style="display:flex;gap:6px;margin-bottom:10px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:6px 14px" onclick="moveSection('${esc(aisle)}','${side}',${sectionIndex},-1)">↑ Monter</button>
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:6px 14px" onclick="moveSection('${esc(aisle)}','${side}',${sectionIndex},1)">↓ Descendre</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="openMoveSection('${esc(aisle)}','${side}',${sectionIndex})">⇄ Autre allée</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px;color:#c8102e;border-color:#f1b8c2;margin-left:auto" onclick="removeSection('${esc(aisle)}','${side}',${sectionIndex})">✕ Supprimer section</button>
      </div>
      ${section.shelves.length ? '' : `<div class="small" style="padding:4px 0;color:#94a3b8">Aucune tablette — cliquez ➕ Tablette ci-dessus.</div>`}
      <div class="plan-shelf-grid">
        ${section.shelves.map((positions, shelfIndex) =>
          renderShelfCard(aisle, side, sectionIndex, shelfIndex, positions, (section.labels || [])[shelfIndex] || '')
        ).join('')}
      </div>
    </div>
  </details>`;
}

// One side (Côté A/B): its structure controls + all its sections. Extracted so
// adding a section / applying a template re-renders just this side.
function renderSide(aisle, side, config) {
  const sections = config.sides[side].sections;
  const sideNodeId = `planSide-${aisle}-${side}`;
  const sideLabel = sideDisplayLabel(side);
  const sideCount = planSummaryCounts().side.get(`${aisle}|${side}`) || 0;
  return `<details class="tree-node plan-side" data-node-id="${sideNodeId}"${detailsOpenAttr(sideNodeId)}>
    <summary>
      <span>${sideLabel}</span>
      <span class="tree-meta">${sections.length} section${sections.length !== 1 ? 's' : ''} · ${sideCount} produit${sideCount !== 1 ? 's' : ''}</span>
    </summary>
    <div class="tree-body">
      <details class="struct-details">
        <summary class="struct-toggle">⚙ Modifier la structure de ${sideLabel}</summary>
        <div style="margin-top:10px">
          <div class="row3">
            <div class="field">
              <label class="label" for="sideTemplateSections-${aisle}-${side}">Sections</label>
              <input id="sideTemplateSections-${aisle}-${side}" type="number" min="0" value="${sections.length}"/>
            </div>
            <div class="field">
              <label class="label" for="sideTemplateShelves-${aisle}-${side}">Tablettes / section</label>
              <input id="sideTemplateShelves-${aisle}-${side}" type="number" min="0" value="${sections[0]?.shelves?.length ?? 0}"/>
            </div>
            <div class="field">
              <label class="label" for="sideTemplatePositions-${aisle}-${side}">Positions / tablette</label>
              <input id="sideTemplatePositions-${aisle}-${side}" type="number" min="0" value="${sections[0]?.shelves?.[0] ?? 0}"/>
            </div>
          </div>
          <button class="btn btn-outline btn-inline" style="margin-top:8px" onclick="applySideTemplate('${esc(aisle)}','${side}')">Appliquer modèle uniforme a ${sideLabel}</button>
          <div class="field" style="margin-top:8px">
            <label class="label" for="sectionCount-${aisle}-${side}">Nombre de sections</label>
            <input id="sectionCount-${aisle}-${side}" type="number" min="0" value="${sections.length}" onchange="setSideSectionCount('${esc(aisle)}','${side}', this.value)"/>
          </div>
        </div>
      </details>
      ${sections.length ? '' : `<div class="small" style="padding:8px 0">Aucune section sur ${sideLabel}.</div>`}
      ${sections.map((section, sectionIndex) => renderSection(aisle, side, sectionIndex, section)).join('')}
      <button class="btn btn-outline btn-inline" style="margin-top:8px;font-size:12px;width:100%" onclick="addSection('${esc(aisle)}','${side}')">➕ Ajouter une section</button>
    </div>
  </details>`;
}

function rerenderSection(aisle, side, sectionIndex) {
  const layout = mapLayouts.find(l => String(l.aisle) === String(aisle));
  const section = layout && layout.config && layout.config.sides[side] && layout.config.sides[side].sections[sectionIndex];
  const el = document.querySelector(`#mapContent [data-node-id="planSection-${aisle}-${side}-${sectionIndex}"]`);
  if (!layout || !section || !el) { refreshPlanUi(); return; }
  captureOpenPlanNodesFromDom();
  el.outerHTML = renderSection(aisle, side, sectionIndex, section);
  _updateAisleSlotTotal(aisle);
}

function rerenderSide(aisle, side) {
  const layout = mapLayouts.find(l => String(l.aisle) === String(aisle));
  const el = document.querySelector(`#mapContent [data-node-id="planSide-${aisle}-${side}"]`);
  if (!layout || !layout.config || !el) { refreshPlanUi(); return; }
  captureOpenPlanNodesFromDom();
  el.outerHTML = renderSide(aisle, side, layout.config);
  _updateAisleSlotTotal(aisle);
}

function renderMapEditor() {
  captureOpenPlanNodesFromDom();
  const msgDiv = document.getElementById('addMsg');
  const div = document.getElementById('mapContent');
  const counts = planSummaryCounts();   // memoized per cache version (no rescan on layout-only edits)
  div.innerHTML = mapLayouts.length
    ? `<div class="tool-row" style="margin-bottom:12px">
        <button class="btn btn-outline btn-inline" onclick="setAllPlanTrees(true)">Ouvrir tout</button>
        <button class="btn btn-outline btn-inline" onclick="setAllPlanTrees(false)">Fermer tout</button>
      </div>` + mapLayouts.map(layout => {
        syncLayoutRecord(layout);          // normalizes layout.config + refreshes metrics/count (count is memoized)
        const config = layout.config;       // already normalized above — skip a 2nd deep rebuild
        const slotCount = countSlotsFromConfig(config);   // no per-slot object allocation
        const aisleNodeId = `planAisle-${layout.aisle}`;
        const homeCount = counts.home.get(String(layout.aisle)) || 0;
        const dirty = dirtyLayoutAisles.has(String(layout.aisle));
        return `<details class="tree-node plan-aisle-node" id="${aisleNodeId}" data-node-id="${aisleNodeId}"${detailsOpenAttr(aisleNodeId)}>
        <summary>
          <span>Allée ${esc(layout.aisle)}</span>
          <span class="tree-meta">${layout.product_count || 0} produit${Number(layout.product_count || 0) !== 1 ? 's' : ''} · <span id="aisleSlots-${esc(layout.aisle)}">${slotCount}</span> slots${homeCount ? ` · <span style="color:#c8102e">★${homeCount} maison</span>` : ''}${dirty ? ' · <span style="color:#d97706">non sauvegardé</span>' : ''}</span>
        </summary>
        <div class="tree-body">
        <div class="plan-actions" style="margin-top:8px">
          <button class="btn btn-inline" onclick="saveAisleLayout('${esc(layout.aisle)}')">Sauver</button>
          <button class="btn btn-outline btn-inline" onclick="applyAisleLayoutToCursor('${esc(layout.aisle)}')">Utiliser pour scan</button>
          <button class="btn btn-outline btn-inline" onclick="setPlanAisleTrees('${esc(layout.aisle)}', true)">Tout ouvrir</button>
          <button class="btn btn-outline btn-inline" onclick="setPlanAisleTrees('${esc(layout.aisle)}', false)">Tout fermer</button>
          <button class="btn btn-outline btn-inline" style="border-color:#f1b8c2;color:#c8102e" onclick="removeAisleLayout('${esc(layout.aisle)}')">Supprimer</button>
        </div>
        ${layout.modified_by ? `<div class="small" style="margin-top:6px">Modifie par: ${esc(layout.modified_by)}</div>` : ''}
        <div class="plan-sides">
        ${['Gauche','Droite'].map(side => renderSide(layout.aisle, side, config)).join('')}
        </div>
        ${renderFacadesSection(layout.aisle, config)}
        ${renderPresentoirSection(layout.aisle, config)}
        </div>
      </details>`;
      }).join('')
    : '<div class="empty">Aucune allée configurée.</div>';
  if (!msgDiv.textContent) msgDiv.innerHTML = '';
}

function setAllPlanTrees(open) {
  document.querySelectorAll('#mapContent details.tree-node').forEach(node => { node.open = open; });
  captureOpenPlanNodesFromDom();
}

function setPlanAisleTrees(aisle, open) {
  const root = document.getElementById(`planAisle-${aisle}`);
  if (!root) return;
  root.open = open;
  root.querySelectorAll('details.tree-node').forEach(node => { node.open = open; });
  captureOpenPlanNodesFromDom();
}

async function loadMapEditor(forceServer=false) {
  await Promise.allSettled([refreshProductsCache(forceServer), refreshLayoutsCache(forceServer)]);
  refreshPlanUi();
  loadPlanogramHistory();
  loadReferenceCount();
}

async function loadReferenceCount() {
  const el = document.getElementById('referenceCount');
  if (!el) return;
  try {
    const {res, data} = await apiFetch('/api/reference/count');
    el.textContent = res.ok ? `${Number(data.count || 0).toLocaleString('fr-CA')} produits au catalogue` : '—';
  } catch (e) { el.textContent = '—'; }
}

async function seedReference() {
  if (!requireEditorSession('remplir le catalogue')) return;
  const btn = document.getElementById('seedReferenceBtn');
  const msg = document.getElementById('referenceMsg');
  if (btn) btn.disabled = true;
  if (msg) { msg.style.color = '#64748b'; msg.textContent = 'Démarrage du remplissage…'; }
  try {
    const {res, data} = await apiFetch('/api/reference/seed', {
      method: 'POST', headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({pages: 25})
    });
    if (res.ok && data.success) {
      if (msg) { msg.style.color = '#16a34a'; msg.textContent = data.message || 'Remplissage en cours en arrière-plan.'; }
      let n = 0;                                   // poll the total as it grows
      const iv = window.setInterval(() => { loadReferenceCount(); if (++n >= 12) window.clearInterval(iv); }, 5000);
    } else if (msg) { msg.style.color = '#c8102e'; msg.textContent = (data && data.error) || 'Impossible de démarrer.'; }
  } catch (e) { if (msg) { msg.style.color = '#c8102e'; msg.textContent = 'Erreur réseau.'; } }
  if (btn) btn.disabled = false;
}

function getEditableLayout(aisle) {
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  if (!layout) return null;
  return normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position);
}

function getMutableLayout(aisle) {
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  if (!layout) return null;
  layout.config = normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position);
  return layout;
}

function readAisleLayoutConfig(aisle) {
  return getEditableLayout(aisle);
}

function setSideSectionCount(aisle, side, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const count = Math.max(0, Number(rawValue) || 0);
  const sections = layout.config.sides[side].sections;
  const currentCount = sections.length;
  const fallbackShelves = sections[sections.length - 1]?.shelves?.slice() || [];
  const nextConfig = normalizeLayoutConfig({
    sides: {
      ...layout.config.sides,
      [side]: {
        sections: [
          ...sections.slice(0, count),
          ...Array.from({length: Math.max(0, count - sections.length)}, () => ({shelves: fallbackShelves.slice()}))
        ]
      }
    }
  }, layout.max_section, layout.max_shelf, layout.max_position);
  if (count < currentCount && !confirmLayoutReduction(aisle, nextConfig, `Réduire le nombre de sections du ${sideDisplayLabel(side)} à ${count}`)) {
    refreshPlanUi(); return;
  }
  while (sections.length < count) sections.push({shelves: fallbackShelves.slice()});
  sections.length = count;
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSide(aisle, side);   // section count changed on this side only
}

function setShelfLabel(aisle, side, sectionIndex, shelfIndex, value) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side]?.sections[sectionIndex];
  if (!section) return;
  if (!section.labels) section.labels = section.shelves.map(() => '');
  while (section.labels.length < section.shelves.length) section.labels.push('');
  section.labels[shelfIndex] = value.trim();
  // Typed live (oninput): only capture the value + mark dirty. Do NOT rebuild
  // the whole plan tree on every keystroke — that pegged the CPU and heated the
  // phone (and fought the cursor). The input already shows what was typed.
  markLayoutDirty(aisle);
}

function addAccrocheToSection(aisle, side, sectionIndex) {
  const rawCount = prompt('Combien de produits sur cette accroche ?', '12');
  if (rawCount === null) return;
  const count = Math.max(1, parseInt(rawCount) || 12);
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side]?.sections[sectionIndex];
  if (!section) return;
  section.shelves.push(count);
  if (!section.labels) section.labels = section.shelves.map(() => '');
  while (section.labels.length < section.shelves.length) section.labels.push('');
  section.labels[section.shelves.length - 1] = 'Accroche';
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSection(aisle, side, sectionIndex);   // only this section changed
}

function _isLibreShelf(aisle, side, section, shelf) {
  const config = getAisleLayoutConfig(aisle);
  const si = parseInt(section) - 1;
  const ti = parseInt(shelf) - 1;
  if (side === 'Gauche' || side === 'Droite') {
    const positions = config?.sides?.[side]?.sections?.[si]?.shelves?.[ti];
    return positions === 0;
  }
  if (side === 'Façade A') return (config?.facade_a?.shelves?.[ti] ?? -1) === 0;
  if (side === 'Façade B') return (config?.facade_b?.shelves?.[ti] ?? -1) === 0;
  for (const pres of (config?.presentoirs || [])) {
    for (const f of (pres.facades || [])) {
      if (side === `${pres.name} - ${f.name}`) return (f.shelves?.[ti] ?? -1) === 0;
    }
  }
  return false;
}

function getShelfLabel(aisle, side, section, shelf) {
  const config = getAisleLayoutConfig(aisle);
  const ti = parseInt(shelf) - 1;
  if (side === 'Gauche' || side === 'Droite') {
    return config?.sides?.[side]?.sections?.[parseInt(section)-1]?.labels?.[ti] || '';
  }
  if (side === 'Façade A') return config?.facade_a?.labels?.[ti] || '';
  if (side === 'Façade B') return config?.facade_b?.labels?.[ti] || '';
  // Présentoir façade: side = "{pres.name} - {facade.name}"
  for (const pres of (config?.presentoirs || [])) {
    for (const f of (pres.facades || [])) {
      if (side === `${pres.name} - ${f.name}`) return f.labels?.[ti] || '';
    }
  }
  return '';
}

function setShelfPositionCount(aisle, side, sectionIndex, shelfIndex, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side].sections[sectionIndex];
  if (!section || section.shelves[shelfIndex] == null) return;
  const currentValue = Number(section.shelves[shelfIndex] || 0);
  const nextValue = Math.max(0, Number(rawValue) || 0);
  // nextValue === 0 means "mode libre" — no confirmation needed, products stay on shelf
  if (nextValue > 0 && nextValue < currentValue) {
    const nextConfig = normalizeLayoutConfig({
      sides: {
        ...layout.config.sides,
        [side]: {
          sections: layout.config.sides[side].sections.map((item, index) => index === sectionIndex
            ? {shelves: item.shelves.map((v, idx) => idx === shelfIndex ? nextValue : v)}
            : item)
        }
      }
    }, layout.max_section, layout.max_shelf, layout.max_position);
    if (!confirmLayoutReduction(aisle, nextConfig, `Réduire le nombre de positions de la tablette ${shelfIndex + 1} à ${nextValue}`)) {
      refreshPlanUi(); return;
    }
  }
  section.shelves[shelfIndex] = nextValue;
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  // Only this shelf card (and the aisle slot total) changed — re-render just
  // that, not the entire plan tree. Big win when tapping +/- on a large plan.
  rerenderShelfCard(aisle, side, sectionIndex, shelfIndex);
}

async function createAisleLayout() {
  if (!requireEditorSession('modifier le plan du magasin')) return;
  const aisle = document.getElementById('mapAisle').value.trim();
  const leftSections = document.getElementById('mapLeftSections').value.trim() || '0';
  const rightSections = document.getElementById('mapRightSections').value.trim() || '0';
  const max_shelf = document.getElementById('mapInitialShelves').value.trim() || '0';
  const max_position = document.getElementById('mapInitialPositions').value.trim() || '0';
  const msgDiv = document.getElementById('addMsg');
  const max_section = Math.max(Number(leftSections) || 0, Number(rightSections) || 0);
  const data = await apiCreateLayoutAisle({
    aisle, max_section, max_shelf, max_position,
    config: buildLayoutWithSideCounts(leftSections, rightSections, max_shelf, max_position)
  });
  msgDiv.className = data.success ? 'msg success' : 'msg error';
  msgDiv.textContent = data.success ? `✅ Allée ${aisle} créée.` : (data.error || 'Création impossible. Vérifiez le numéro d allée.');
  msgDiv.scrollIntoView({behavior: 'smooth', block: 'nearest'});
  if (!data.success) return;
  if (data.success) {
    const config = normalizeLayoutConfig(buildLayoutWithSideCounts(leftSections, rightSections, max_shelf, max_position), max_section, max_shelf, max_position);
    mapLayouts.push(syncLayoutRecord({
      aisle: String(aisle), config, enabled: true,
      modified_by: loadEditorSession().username || '',
      modified_at: nowIsoWithoutMs(), product_count: 0,
      ...getLayoutMetrics(config)
    }));
    sortMapLayouts();
    lastLayoutsRefreshAt = Date.now();
    clearLayoutDirty(aisle);
    openPlanNodes.add(`planAisle-${aisle}`);
    openPlanNodes.add(`planSide-${aisle}-Gauche`);
    openPlanNodes.add(`planSide-${aisle}-Droite`);
    planStartDraft = {...getPlanStartDraft(), aisle: String(aisle)};
    document.getElementById('mapAisle').value = '';
    document.getElementById('mapLeftSections').value = '0';
    document.getElementById('mapRightSections').value = '0';
    document.getElementById('mapInitialShelves').value = '0';
    document.getElementById('mapInitialPositions').value = '0';
    refreshPlanUi();
  }
}

async function saveAisleLayout(aisle) {
  if (!requireEditorSession('modifier le plan du magasin')) return;
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  syncLayoutRecord(layout);
  const config = readAisleLayoutConfig(aisle);
  const data = await apiUpdateLayoutAisle(aisle, {config, enabled: true});
  const msgDiv = document.getElementById('addMsg');
  msgDiv.className = data.success ? 'msg success' : 'msg error';
  msgDiv.textContent = data.success
    ? `Allée ${aisle} sauvee.${Number(data.removed_products || 0) ? ` ${data.removed_products} produit(s) supprime(s) car hors structure.` : ''}`
    : (data.error || 'Sauvegarde impossible.');
  if (data.success) {
    clearLayoutDirty(aisle);
    await refreshProductsCache(true);
    layout.modified_by = loadEditorSession().username || layout.modified_by || '';
    layout.modified_at = nowIsoWithoutMs();
    syncLayoutRecord(layout);
    lastLayoutsRefreshAt = Date.now();
    refreshPlanUi();
  }
}

function applyAisleLayoutToCursor(aisle) {
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  cursor.aisle = Number(aisle) || cursor.aisle;
  const config = getEditableLayout(aisle);
  if (layout && config) {
    syncCursorLimitsForAisle(aisle);
    const aisleSlots = buildSlotsFromConfig(aisle, config);
    if (aisleSlots.length) setCursorFromSlot(aisleSlots[0]);
    else normalizeCursorToLayout();
  }
  updateCursorUi();
  planStartDraft = getCursorSelection();
  renderPlanStartEditor();
  document.getElementById('addMsg').innerHTML = `<div class="msg success">${buildSlotsFromConfig(aisle, config || defaultLayoutConfig()).length ? `Le scan utilisera maintenant l’allée ${esc(aisle)}.` : `L’allée ${esc(aisle)} n’a aucune position de scan pour le moment.`}</div>`;
}

async function removeAisleLayout(aisle) {
  if (!requireEditorSession('modifier le plan du magasin')) return;
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  const productCount = Number(layout?.product_count || 0);
  const question = productCount
    ? `Supprimer l’allée ${aisle} ?\n\nCela supprimera aussi ${productCount} produit(s) qui sont dans cette allée.`
    : `Supprimer l’allée ${aisle} ?`;
  if (!confirm(question)) return;
  let data = null;
  try { data = await apiDeleteLayoutAisle(aisle); }
  catch (e) { data = {success: false, error: 'Suppression impossible pour le moment.'}; }
  const msgDiv = document.getElementById('addMsg');
  const success = Boolean(data && data.success);
  msgDiv.className = success ? 'msg success' : 'msg error';
  msgDiv.textContent = success
    ? (data.message || `Allée ${aisle} retirée.`)
    : ((data && data.error) || `Impossible de supprimer l’allée ${aisle}.`);
  if (success) {
    mapLayouts = mapLayouts.filter(item => String(item.aisle) !== String(aisle));
    clearLayoutDirty(aisle);
    await refreshProductsCache(true);
    lastLayoutsRefreshAt = Date.now();
    planStartDraft = getCursorSelection();
    refreshPlanUi();
  }
}

async function deleteProduct(id) {
  if (!requireEditorSession('supprimer un produit')) return;
  if (!confirm('Supprimer ce produit ?')) return;
  const data = await apiDeleteProduct(id);
  if (!data.success) {
    // Show the error in the tab the user is actually looking at — writing it to
    // the hidden search results made a failed delete look like a dead button.
    const target = document.getElementById('add')?.classList.contains('active')
      ? document.getElementById('addMsg')
      : document.getElementById('searchResults');
    if (target) target.innerHTML = `<div class="msg error">${esc(data.error || 'Suppression impossible.')}</div>`;
    return;
  }
  removeCachedProduct(id);   // local cache update — no server refetch
  // Refresh only the view the user is on. (Was: doSearch + loadMapEditor, which
  // rebuilt the whole plan AND did a planogram-history network fetch on every
  // single delete — a real heat source when cleaning up many products.)
  if (document.getElementById('add')?.classList.contains('active')) refreshPlanUi();
  else if (document.getElementById('scan')?.classList.contains('active')) refreshRayonList();
  else doSearch();
  const sr = document.getElementById('searchResults');
  if (sr && document.getElementById('search')?.classList.contains('active'))
    sr.insertAdjacentHTML('afterbegin', `<div class="msg success">${esc(data.message || 'Produit supprimé.')}</div>`);
}

// ── Database management ───────────────────────────────────────────────────────
async function exportDatabase() {
  const msg = document.getElementById('exportImportMsg');
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Preparation du fichier...'; }
  try {
    const res = await fetch('/api/export');
    if (!res.ok) throw new Error('Erreur serveur');
    const blob = await res.blob();
    const filename = `familiprix-backup-${new Date().toISOString().slice(0,10)}.json`;
    const file = new File([blob], filename, {type: 'application/json'});
    if (navigator.canShare && navigator.canShare({files: [file]})) {
      await navigator.share({files: [file], title: 'Sauvegarde Familiprix', text: 'Base de données Familiprix Localisateur'});
      if (msg) { msg.className = 'msg success'; msg.textContent = 'Fichier partage avec succes.'; }
    } else {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = filename;
      document.body.appendChild(a); a.click(); document.body.removeChild(a);
      URL.revokeObjectURL(url);
      if (msg) { msg.className = 'msg success'; msg.textContent = 'Fichier telecharge. Partagez-le via WhatsApp.'; }
    }
  } catch (e) {
    if (e.name !== 'AbortError' && msg) { msg.className = 'msg error'; msg.textContent = 'Impossible de partager le fichier.'; }
  }
}

async function importDatabase(input) {
  const msg = document.getElementById('exportImportMsg');
  const file = input.files[0];
  input.value = '';
  if (!file) return;
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Lecture du fichier...'; }
  let payload;
  try { payload = JSON.parse(await file.text()); }
  catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = 'Fichier invalide. Utilisez uniquement un fichier exporte par cette application.'; }
    return;
  }
  if (payload.export_version !== 1) {
    if (msg) { msg.className = 'msg error'; msg.textContent = 'Format non reconnu. Fichier incompatible.'; }
    return;
  }
  const products = (payload.products || []).length;
  const layouts = (payload.aisle_layouts || []).length;
  if (!confirm(`Importer ${products} produit(s) et ${layouts} allée(s) dans la base? Les positions existantes seront mises a jour.`)) return;
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Import en cours...'; }
  try {
    const {res, data} = await apiFetch('/api/import', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    if (!res.ok || !data.success) throw new Error(data.error || 'Import echoue');
    if (msg) { msg.className = 'msg success'; msg.textContent = `Import termine: ${data.imported_products} produit(s) et ${data.imported_layouts} allée(s) importes. ${data.skipped_products ? data.skipped_products + ' ignore(s).' : ''}`; }
    await refreshProductsCache(true);
    await refreshLayoutsCache(true);
    renderMapEditor();
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de l import.'; }
  }
}

async function resetDatabase(wipeLayouts) {
  const msg = document.getElementById('exportImportMsg');
  const what = wipeLayouts ? 'TOUS les produits ET le plan du magasin' : 'tous les produits';
  if (!confirm(`Effacer ${what}? Cette action’est irreversible. Faites une sauvegarde d’abord si nécessaire.`)) return;
  if (!confirm('Confirmation finale: effacer definitivement?')) return;
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Suppression en cours...'; }
  try {
    const {res, data} = await apiFetch('/api/reset', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', ...getEditorHeaders()},
      body: JSON.stringify({wipe_layouts: wipeLayouts})
    });
    if (!res.ok || !data.success) throw new Error(data.error || 'Erreur');
    if (msg) {
      msg.className = 'msg success';
      msg.textContent = `Base nettoyee: ${data.deleted_products} produit(s) supprime(s)${wipeLayouts ? `, ${data.deleted_layouts} allée(s) supprimée(s)` : ''}.`;
    }
    await refreshProductsCache(true);
    if (wipeLayouts) { await refreshLayoutsCache(true); renderMapEditor(); }
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de la suppression.'; }
  }
}

// ── Direct section / shelf / position editing ─────────────────────────────────
function addSection(aisle, side) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const sections = layout.config.sides[side].sections;
  const last = sections[sections.length - 1];
  const newShelvesCount = last?.shelves?.length ?? 3;
  const newPositions = last?.shelves?.[0] ?? 8;
  sections.push({
    shelves: Array.from({length: newShelvesCount}, () => newPositions),
    labels:  Array.from({length: newShelvesCount}, () => '')
  });
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSide(aisle, side);   // a section was added to this side
}

async function removeSection(aisle, side, sectionIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const sections = layout.config.sides[side].sections;
  const hasProducts = allProductsCache.some(p =>
    String(p.aisle) === String(aisle) && p.side === side && String(p.section) === String(sectionIndex + 1)
  );
  if (hasProducts && !confirm(`La section ${sectionIndex + 1} contient des produits. Supprimer quand même ?`)) return;
  // Server deletes that section's products and shifts higher sections down by 1,
  // keeping product numbering aligned with the config. Bail if it fails.
  if (!await _swapCall(aisle, 'remove-section', {side, section: String(sectionIndex + 1)})) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Suppression impossible.</div>';
    return;
  }
  sections.splice(sectionIndex, 1);
  await saveAisleLayout(aisle);   // persist config so DB + plan stay consistent
}

function addShelf(aisle, side, sectionIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side].sections[sectionIndex];
  if (!section) return;
  const fallback = section.shelves[section.shelves.length - 1] ?? 8;
  section.shelves.push(fallback);
  if (!section.labels) section.labels = [];
  section.labels.push('');
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSection(aisle, side, sectionIndex);   // only this section changed
}

async function removeShelf(aisle, side, sectionIndex, shelfIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side].sections[sectionIndex];
  if (!section) return;
  const hasProducts = allProductsCache.some(p =>
    String(p.aisle) === String(aisle) && p.side === side &&
    String(p.section) === String(sectionIndex + 1) && String(p.shelf) === String(shelfIndex + 1)
  );
  if (hasProducts && !confirm(`La tablette ${shelfIndex + 1} contient des produits. Supprimer quand même ?`)) return;
  // Server deletes that shelf's products and shifts higher shelves (in this
  // section) down by 1. Bail if it fails so config and DB don't diverge.
  if (!await _swapCall(aisle, 'remove-shelf', {side, section: String(sectionIndex + 1), shelf: String(shelfIndex + 1)})) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Suppression impossible.</div>';
    return;
  }
  section.shelves.splice(shelfIndex, 1);
  if (section.labels) section.labels.splice(shelfIndex, 1);
  await saveAisleLayout(aisle);
}

// Delete a tablette of a fixture side (Façade A/B or a présentoir façade): the
// server removes its products and renumbers the shelves above (no section scoping
// on fixture sides), then the config entry is removed and saved — same contract
// as removeShelf for the aisle sides.
async function _removeFixtureShelf(aisle, sideName, fixture, shelfIndex) {
  const hasProducts = allProductsCache.some(p =>
    String(p.aisle) === String(aisle) && p.side === sideName && String(p.shelf) === String(shelfIndex + 1)
  );
  if (hasProducts && !confirm(`La tablette ${shelfIndex + 1} contient des produits. Supprimer quand même ?`)) return;
  if (!await _swapCall(aisle, 'remove-shelf', {side: sideName, shelf: String(shelfIndex + 1)})) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Suppression impossible.</div>';
    return;
  }
  fixture.shelves.splice(shelfIndex, 1);
  if (fixture.labels) fixture.labels.splice(shelfIndex, 1);
  await saveAisleLayout(aisle);
}

async function removeFacadeShelf(aisle, facadeKey, shelfIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  if (!layout.config[facadeKey]) return;
  const sideName = facadeKey === 'facade_a' ? 'Façade A' : 'Façade B';
  await _removeFixtureShelf(aisle, sideName, _fixFixture(layout.config[facadeKey]), shelfIndex);
}

async function removePresentoirShelf(aisle, presIndex, facadeIndex, shelfIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const pres = layout.config.presentoirs?.[presIndex];
  const facade = pres?.facades?.[facadeIndex];
  if (!pres || !facade) return;
  await _removeFixtureShelf(aisle, `${pres.name} - ${facade.name}`, _fixFixture(facade), shelfIndex);
}

async function _swapCall(aisle, endpoint, body) {
  try {
    const {res, data} = await apiFetch(
      `/api/layout/aisles/${encodeURIComponent(aisle)}/${endpoint}`,
      {method:'POST', headers:{'Content-Type':'application/json',...getEditorHeaders()}, body:JSON.stringify(body)}
    );
    return res.ok && data.success;
  } catch (e) {
    return false;   // network failure/timeout → caller shows its error message
  }
}

async function moveSection(aisle, side, sectionIndex, direction) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const sections = layout.config.sides[side].sections;
  const target = sectionIndex + direction;
  if (target < 0 || target >= sections.length) return;
  // Server swaps the products of the two sections (3 SQL statements).
  if (!await _swapCall(aisle, 'swap-sections', {side, section_a: String(sectionIndex+1), section_b: String(target+1)})) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Déplacement impossible.</div>';
    return;
  }
  [sections[sectionIndex], sections[target]] = [sections[target], sections[sectionIndex]];
  await saveAisleLayout(aisle);   // persist config in the same step → no desync on refresh
}

async function moveShelf(aisle, side, sectionIndex, shelfIndex, direction) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side].sections[sectionIndex];
  if (!section) return;
  const target = shelfIndex + direction;
  if (target < 0 || target >= section.shelves.length) return;
  if (!await _swapCall(aisle, 'swap-shelves', {side, section: String(sectionIndex+1), shelf_a: String(shelfIndex+1), shelf_b: String(target+1)})) {
    document.getElementById('addMsg').innerHTML = '<div class="msg error">Déplacement impossible.</div>';
    return;
  }
  [section.shelves[shelfIndex], section.shelves[target]] = [section.shelves[target], section.shelves[shelfIndex]];
  if (section.labels) [section.labels[shelfIndex], section.labels[target]] = [section.labels[target], section.labels[shelfIndex]];
  await saveAisleLayout(aisle);
}

async function swapPositions(aisle, side, section, shelf, posA, posB) {
  const ok = await _swapCall(aisle, 'swap-positions', {side, section: String(section), shelf: String(shelf), position_a: String(posA), position_b: String(posB)});
  if (!ok) { const m = document.getElementById('addMsg'); if (m) m.innerHTML = '<div class="msg error">Échange impossible.</div>'; return; }
  // Mirror the server swap in the local cache instead of re-downloading the whole
  // catalog: just exchange the two products' position values, then re-render only
  // this shelf card.
  const at = pos => allProductsCache.find(p =>
    String(p.aisle) === String(aisle) && p.side === side &&
    String(p.section || '1') === String(section) && String(p.shelf) === String(shelf) &&
    String(p.position) === String(pos));
  const a = at(posA), b = at(posB);
  if (a) a.position = String(posB);
  if (b) b.position = String(posA);
  lastProductsRefreshAt = Date.now();   // invalidate memoized shelf/count indexes
  rerenderShelfCard(aisle, side, parseInt(section) - 1, parseInt(shelf) - 1);
}

// ── Façade & Présentoir management ────────────────────────────────────────────
function _fixFixture(fixture) {
  // Ensure {shelves, labels} are arrays of same length
  if (!fixture) return {shelves: [], labels: []};
  if (!Array.isArray(fixture.shelves)) fixture.shelves = [];
  if (!Array.isArray(fixture.labels)) fixture.labels = [];
  while (fixture.labels.length < fixture.shelves.length) fixture.labels.push('');
  fixture.labels.length = fixture.shelves.length;
  return fixture;
}

function setFacadeShelfCount(aisle, facadeKey, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  if (!layout.config[facadeKey]) layout.config[facadeKey] = {shelves: [], labels: []};
  const fx = _fixFixture(layout.config[facadeKey]);
  const count = Math.max(0, parseInt(rawValue) || 0);
  const fallback = fx.shelves[fx.shelves.length - 1] ?? 8;
  while (fx.shelves.length < count) { fx.shelves.push(fallback); fx.labels.push(''); }
  fx.shelves.length = count; fx.labels.length = count;
  markLayoutDirty(aisle); refreshPlanUi();
}

function setFacadeShelfPositions(aisle, facadeKey, shelfIndex, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const fx = _fixFixture(layout.config[facadeKey]);
  fx.shelves[shelfIndex] = Math.max(0, parseInt(rawValue) || 0);
  markLayoutDirty(aisle); refreshPlanUi();
}

function setFacadeShelfLabel(aisle, facadeKey, shelfIndex, value) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const fx = _fixFixture(layout.config[facadeKey]);
  fx.labels[shelfIndex] = value.trim();
  markLayoutDirty(aisle);   // oninput: capture only, no full rebuild (avoid heat)
}

function addPresentoir(aisle) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  if (!layout.config.presentoirs) layout.config.presentoirs = [];
  const n = layout.config.presentoirs.length + 1;
  layout.config.presentoirs.push({name: `Présentoir ${n}`, facades: [{name: 'Façade 1', shelves: [8], labels: ['']}]});
  markLayoutDirty(aisle); refreshPlanUi();
}

function removePresentoir(aisle, presIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const pres = layout.config.presentoirs?.[presIndex];
  if (!pres) return;
  if (!confirm(`Supprimer "${pres.name}" ? Les produits assignés restent en base.`)) return;
  layout.config.presentoirs.splice(presIndex, 1);
  markLayoutDirty(aisle); refreshPlanUi();
}

function renamePresentoir(aisle, presIndex, value) {
  const layout = getMutableLayout(aisle);
  if (!layout || !layout.config.presentoirs?.[presIndex]) return;
  layout.config.presentoirs[presIndex].name = value.trim() || `Présentoir ${presIndex + 1}`;
  markLayoutDirty(aisle);
}

function addPresentoirFacade(aisle, presIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const pres = layout.config.presentoirs?.[presIndex];
  if (!pres) return;
  if (!pres.facades) pres.facades = [];
  const n = pres.facades.length + 1;
  pres.facades.push({name: `Façade ${n}`, shelves: [8], labels: ['']});
  markLayoutDirty(aisle); refreshPlanUi();
}

function removePresentoirFacade(aisle, presIndex, facadeIndex) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const pres = layout.config.presentoirs?.[presIndex];
  if (!pres || !pres.facades?.[facadeIndex]) return;
  if (pres.facades.length === 1) { alert('Un présentoir doit avoir au moins une façade.'); return; }
  if (!confirm(`Supprimer "${pres.facades[facadeIndex].name}" ?`)) return;
  pres.facades.splice(facadeIndex, 1);
  markLayoutDirty(aisle); refreshPlanUi();
}

function renamePresentoirFacade(aisle, presIndex, facadeIndex, value) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const facade = layout.config.presentoirs?.[presIndex]?.facades?.[facadeIndex];
  if (!facade) return;
  facade.name = value.trim() || `Façade ${facadeIndex + 1}`;
  markLayoutDirty(aisle);
}

function setPresentoirShelfCount(aisle, presIndex, facadeIndex, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const facade = layout.config.presentoirs?.[presIndex]?.facades?.[facadeIndex];
  if (!facade) return;
  _fixFixture(facade);
  const count = Math.max(0, parseInt(rawValue) || 0);
  const fallback = facade.shelves[facade.shelves.length - 1] ?? 8;
  while (facade.shelves.length < count) { facade.shelves.push(fallback); facade.labels.push(''); }
  facade.shelves.length = count; facade.labels.length = count;
  markLayoutDirty(aisle); refreshPlanUi();
}

function setPresentoirShelfPositions(aisle, presIndex, facadeIndex, shelfIndex, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const facade = layout.config.presentoirs?.[presIndex]?.facades?.[facadeIndex];
  if (facade) { facade.shelves[shelfIndex] = Math.max(0, parseInt(rawValue) || 0); markLayoutDirty(aisle); refreshPlanUi(); }
}

function _facadeShelfGrid(aisle, sideName, fk, shelves, labels) {
  return (shelves || []).map((positions, shi) => {
    const sl = (labels || [])[shi] || '';
    const title = sl ? `📎 ${esc(sl)}` : `Tablette ${shi + 1}`;
    const bg = sl ? 'background:#fffbf0;border-color:#fbbf24' : '';
    const filled = allProductsCache.filter(p =>
      String(p.aisle) === String(aisle) && p.side === sideName && String(p.shelf) === String(shi + 1)
    ).length;
    return `<div class="plan-shelf-card" style="${bg}">
      <div class="shelf-header" style="gap:4px">
        <span class="shelf-title">${title}</span>
        <input type="number" min="1" value="${positions}" title="Positions"
               style="width:46px;padding:2px 4px;border:1px solid #e2e8f0;border-radius:5px;font-size:12px;text-align:center"
               onchange="setFacadeShelfPositions('${esc(aisle)}','${fk}',${shi},this.value)"/>
        <span style="font-size:10px;color:#94a3b8">pos</span>
        <span style="font-size:11px;color:#64748b">${filled} prod.</span>
        <button onclick="removeFacadeShelf('${esc(aisle)}','${fk}',${shi})"
                style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5"
                title="Supprimer cette tablette">✕ Suppr.</button>
      </div>
      <details class="struct-details">
        <summary class="struct-toggle" style="font-size:11px">⚙ Nom / étiquette</summary>
        <div class="field" style="margin-top:6px">
          <input type="text" value="${esc(sl)}" placeholder="Laisser vide = Tablette ${shi + 1}"
                 oninput="setFacadeShelfLabel('${esc(aisle)}','${fk}',${shi},this.value)"/>
        </div>
      </details>
      ${renderShelfProductList(aisle, sideName, 1, shi + 1, positions)}
    </div>`;
  }).join('');
}

function renderFacadesSection(aisle, config) {
  const parts = [
    {label: 'Façade A', key: 'facade_a', sideName: 'Façade A'},
    {label: 'Façade B', key: 'facade_b', sideName: 'Façade B'},
  ].map(({label, key, sideName}) => {
    const fx = config[key] || {shelves: [], labels: []};
    const shelves = fx.shelves || []; const labels = fx.labels || [];
    const nodeId = `planFacade-${aisle}-${key}`;
    const prods = allProductsCache.filter(p => String(p.aisle) === String(aisle) && p.side === sideName);
    const shelvesHtml = _facadeShelfGrid(aisle, sideName, key, shelves, labels);
    return `<details class="tree-node plan-section" data-node-id="${nodeId}"${detailsOpenAttr(nodeId)}>
      <summary>
        <span>🔲 ${label}</span>
        <span class="tree-meta">${shelves.length} tablette${shelves.length!==1?'s':''} · ${prods.length} produit${prods.length!==1?'s':''}</span>
      </summary>
      <div class="tree-body">
        <div style="display:flex;gap:6px;margin:6px 0 8px">
          <button class="btn btn-outline btn-inline" style="font-size:12px"
                  onclick="setFacadeShelfCount('${esc(aisle)}','${key}',${shelves.length + 1})">➕ Tablette</button>
        </div>
        ${shelves.length ? '' : '<div class="small" style="color:#94a3b8;padding:4px 0">Aucune tablette.</div>'}
        <div class="plan-shelf-grid">${shelvesHtml}</div>
      </div>
    </details>`;
  }).join('');

  return `<div style="margin-top:12px">
    <div style="font-weight:700;font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px">Façades (bouts d allée)</div>
    ${parts}
  </div>`;
}

function renderPresentoirSection(aisle, config) {
  const presentoirs = config.presentoirs || [];

  const presHtml = presentoirs.map((pres, pi) => {
    const presId = `planPres-${aisle}-${pi}`;
    const totalProds = allProductsCache.filter(p =>
      String(p.aisle) === String(aisle) && (pres.facades||[]).some(f => p.side === `${pres.name} - ${f.name}`)
    ).length;

    const facadesHtml = (pres.facades || []).map((facade, fi) => {
      const sideName = `${pres.name} - ${facade.name}`;
      const facadeNodeId = `planPres-${aisle}-${pi}-F${fi}`;
      const facadeProds = allProductsCache.filter(p => String(p.aisle) === String(aisle) && p.side === sideName);
      const shelves = facade.shelves || []; const labels = facade.labels || [];

      const shelvesHtml = shelves.map((positions, shi) => {
        const sl = labels[shi] || '';
        const title = sl ? `📎 ${esc(sl)}` : `T${shi + 1}`;
        const bg = sl ? 'background:#fffbf0;border-color:#fbbf24' : '';
        const filled = allProductsCache.filter(p =>
          String(p.aisle) === String(aisle) && p.side === sideName && String(p.shelf) === String(shi + 1)
        ).length;
        const isLibre = positions === 0;
        const posCtrl = isLibre
          ? `<span style="font-size:10px;color:#8b5cf6;font-weight:700">LIBRE · ${filled}</span>`
          : `<input type="number" min="1" value="${positions}" style="width:44px;padding:2px 4px;border:1px solid #e2e8f0;border-radius:5px;font-size:12px;text-align:center"
               onchange="setPresentoirShelfPositions('${esc(aisle)}',${pi},${fi},${shi},this.value)"/>
             <span style="font-size:10px;color:#94a3b8">pos · ${filled} prod.</span>`;
        return `<div class="plan-shelf-card" style="${bg}${isLibre?';border-color:#a78bfa;background:#faf5ff':''}">
          <div class="shelf-header" style="gap:4px">
            <span class="shelf-title">${title}</span>
            ${posCtrl}
            <button onclick="removePresentoirShelf('${esc(aisle)}',${pi},${fi},${shi})"
                    style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5"
                    title="Supprimer cette tablette">✕ Suppr.</button>
          </div>
          ${renderShelfProductList(aisle, sideName, 1, shi + 1, positions)}
        </div>`;
      }).join('');

      return `<details class="tree-node plan-section" data-node-id="${facadeNodeId}"${detailsOpenAttr(facadeNodeId)}>
        <summary>
          <span>${esc(facade.name)}</span>
          <span class="tree-meta">${shelves.length} T · ${facadeProds.length} prod.</span>
        </summary>
        <div class="tree-body">
          <div style="display:flex;gap:6px;align-items:center;margin:6px 0 8px">
            <input type="text" value="${esc(facade.name)}" style="flex:1;padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px"
                   oninput="renamePresentoirFacade('${esc(aisle)}',${pi},${fi},this.value)" placeholder="Nom de la façade"/>
            <button class="btn btn-outline btn-inline" style="font-size:11px" onclick="setPresentoirShelfCount('${esc(aisle)}',${pi},${fi},${shelves.length+1})">➕ T</button>
            <button class="btn btn-outline btn-inline" style="font-size:11px;color:#c8102e;border-color:#f1b8c2" onclick="removePresentoirFacade('${esc(aisle)}',${pi},${fi})">✕ Façade</button>
          </div>
          <div class="plan-shelf-grid">${shelvesHtml}</div>
        </div>
      </details>`;
    }).join('');

    return `<details class="tree-node plan-pres" data-node-id="${presId}"${detailsOpenAttr(presId)}>
      <summary>
        <span>📦 ${esc(pres.name)}</span>
        <span class="tree-meta">${(pres.facades||[]).length} façade${(pres.facades||[]).length!==1?'s':''} · ${totalProds} prod.</span>
      </summary>
      <div class="tree-body">
        <div style="display:flex;gap:8px;align-items:center;margin:8px 0 10px">
          <input type="text" value="${esc(pres.name)}" style="flex:1;padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px"
                 oninput="renamePresentoir('${esc(aisle)}',${pi},this.value)" placeholder="Nom du présentoir"/>
          <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addPresentoirFacade('${esc(aisle)}',${pi})">➕ Façade</button>
          <button class="btn btn-outline btn-inline" style="font-size:12px;color:#c8102e;border-color:#f1b8c2" onclick="removePresentoir('${esc(aisle)}',${pi})">✕ Supprimer</button>
        </div>
        ${facadesHtml}
      </div>
    </details>`;
  }).join('');

  return `<div style="margin-top:12px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <div style="font-weight:700;font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px">Présentoirs (couloir)</div>
      <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addPresentoir('${esc(aisle)}')">📦 Ajouter présentoir</button>
    </div>
    ${presHtml || '<div class="small" style="color:#94a3b8;padding:4px 0">Aucun présentoir.</div>'}
  </div>`;
}

// ── Planogram import ──────────────────────────────────────────────────────────
let planoData = null;

async function importPlanogramCatalog(input) {
  const file = input.files[0];
  input.value = '';
  if (!file) return;
  if (!requireEditorSession('importer le catalogue de planogrammes')) return;
  const msg = document.getElementById('planoCatalogMsg');
  msg.style.color = '#64748b';
  msg.textContent = 'Import du catalogue en cours…';
  const form = new FormData();
  form.append('file', file);
  try {
    const res = await fetch('/api/import/planogram-catalog', {method:'POST', body: form, headers: getEditorHeaders()});
    const data = await res.json();
    if (!res.ok || !data.success) {
      msg.style.color = '#c8102e';
      msg.textContent = data.error || 'Erreur lors de l import du catalogue.';
      return;
    }
    msg.style.color = '#16a34a';
    msg.innerHTML = `<strong>${data.planograms} planogrammes</strong> · ${data.products_seen} produits enregistrés au catalogue · `
      + `${data.enriched_products} produit(s) déjà placé(s) complété(s).`;
    if (typeof refreshProductsCache === 'function') { try { await refreshProductsCache(true); } catch(_){} }
  } catch (e) {
    msg.style.color = '#c8102e';
    msg.textContent = 'Impossible d importer le catalogue pour le moment.';
  }
}

let catalogEnrichTimer = null;
async function startCatalogEnrich() {
  if (!requireEditorSession('enrichir le catalogue')) return;
  const msg = document.getElementById('catalogEnrichMsg');
  if (msg) { msg.style.color = '#64748b'; msg.textContent = 'Démarrage de l enrichissement…'; }
  try { await fetch('/api/import/catalog-enrich/start', {method:'POST', headers: getEditorHeaders()}); } catch (_) {}
  const stop = document.getElementById('catalogEnrichStop'); if (stop) stop.style.display = '';
  pollCatalogEnrich();
}
async function pollCatalogEnrich() {
  window.clearTimeout(catalogEnrichTimer);
  let s = {};
  try { s = await (await fetch('/api/import/catalog-enrich/status')).json(); } catch (_) {}
  const msg = document.getElementById('catalogEnrichMsg');
  if (msg) {
    const pct = s.total ? Math.round(100 * (s.done || 0) / s.total) : 0;
    msg.style.color = s.running ? '#0369a1' : '#16a34a';
    if (!s.running && !s.total && !s.done) {
      msg.textContent = 'Rien à enrichir pour le moment (déjà fait, ou attendez la fin du déploiement puis réessayez).';
    } else {
      // A stopped run that did NOT reach the end is an interruption, not a success —
      // saying « Terminé » at 2% hid real failures.
      const incomplete = !s.running && s.total && (s.done || 0) < s.total;
      msg.style.color = s.running ? '#0369a1' : (incomplete ? '#b45309' : '#16a34a');
      const head = s.running ? '⏳ En cours…' : (incomplete ? '⚠ Interrompu —' : '✓ Terminé —');
      const eta = (s.running && s.eta_minutes > 0) ? ` · ≈ ${s.eta_minutes} min restantes` : '';
      const resumed = s.resumed ? ' (reprise automatique après redémarrage)' : '';
      const retry = incomplete
        ? ` · <b>cliquez « Enrichir » pour continuer</b>${s.error ? ` (cause: ${esc(s.error)})` : ''}`
        : '';
      msg.innerHTML = `${head} ${s.done || 0}/${s.total || 0} (${pct}%)${eta} · `
        + `<b>${s.updated || 0}</b> descriptions/images ajoutées · ${s.skipped || 0} sans correspondance fiable${resumed}${retry}`;
    }
  }
  const stop = document.getElementById('catalogEnrichStop');
  if (s.running) { catalogEnrichTimer = window.setTimeout(pollCatalogEnrich, 3000); }
  else if (stop) { stop.style.display = 'none'; }
}
async function stopCatalogEnrich() {
  try { await fetch('/api/import/catalog-enrich/stop', {method:'POST', headers: getEditorHeaders()}); } catch (_) {}
  const msg = document.getElementById('catalogEnrichMsg');
  if (msg) msg.textContent = 'Arrêt demandé…';
}

// Token so an old poll loop stops cleanly if the user picks another PDF.
let _planoParseToken = 0;

async function parsePlanogramPDF(input) {
  const file = input.files[0];
  input.value = '';
  if (!file) return;
  const token = ++_planoParseToken;
  const msg = document.getElementById('planoMsg');
  msg.textContent = 'Envoi du PDF…';
  msg.style.color = '#64748b';
  document.getElementById('planoConfig').style.display = 'none';
  planoData = null;

  const form = new FormData();
  form.append('file', file);
  try {
    // The server parses in the BACKGROUND and we poll for the result: a big
    // plano takes minutes on the server's small CPU — parsing inside the
    // upload request hit the HTTP timeout and looked like a dead button.
    const res  = await fetch('/api/import/planogram-parse', {method:'POST', body: form});
    const up = await res.json();
    if (!res.ok || !up.success || !up.job) {
      msg.textContent = up.error || 'Erreur lors de l analyse.';
      msg.style.color = '#c8102e';
      return;
    }
    const t0 = Date.now();
    let data = null;
    while (Date.now() - t0 < 8 * 60 * 1000) {           // up to 8 min for a huge plano
      await new Promise(r => setTimeout(r, 2500));
      if (token !== _planoParseToken) return;           // user picked another PDF
      let sj = null;
      try {
        const sr = await fetch(`/api/import/planogram-parse/status/${up.job}`, {cache: 'no-store'});
        sj = await sr.json();
      } catch (e) { continue; }                          // transient network — keep polling
      if (!sj) continue;
      if (sj.status === 'error') {
        msg.textContent = sj.error || 'Erreur lors de l analyse.';
        msg.style.color = '#c8102e';
        return;
      }
      if (sj.status === 'done') { data = sj; break; }
      if (sj.status === 'unknown') {
        msg.textContent = sj.error || 'Analyse perdue — re-choisissez le PDF.';
        msg.style.color = '#c8102e';
        return;
      }
      const s = Math.round((Date.now() - t0) / 1000);
      msg.textContent = `Analyse en cours… ${s}s (un gros plano peut prendre 1 à 3 minutes — restez sur cette page)`;
    }
    if (!data) {
      msg.textContent = 'L analyse prend trop de temps. Réessayez dans une minute.';
      msg.style.color = '#c8102e';
      return;
    }
    if (token !== _planoParseToken) return;
    planoData = data;
    // Every parsed line is a plano product by default (editable in the preview).
    (planoData.products || []).forEach(p => { if (p.is_plano === undefined) p.is_plano = true; });
    const tabs = Object.keys(data.tablettes).map(Number).sort((a,b)=>a-b);
    document.getElementById('planoTabStart').value = tabs[0] || 1;
    document.getElementById('planoTabEnd').value   = tabs[tabs.length-1] || 8;
    const aisleSelect = document.getElementById('planoAisle');
    aisleSelect.innerHTML = mapLayouts.map(l=>`<option value="${l.aisle}">${l.aisle}</option>`).join('');
    const sum = Object.entries(data.tablettes).map(([t,n])=>`T${t}:${n}`).join(' | ');
    msg.innerHTML = `<strong style="color:#16a34a">${data.count} produits</strong> trouvés — ${sum}`;
    msg.style.color = '#16a34a';
    document.getElementById('planoConfig').style.display = '';
    onPlanoSideChange();   // sets the start section to the côté's starting end, then previews
  } catch(e) {
    msg.textContent = 'Erreur réseau.';
    msg.style.color = '#c8102e';
  }
}

// Mutate a plano line then re-render (editable preview).
function planoSet(idx, field, value) {
  if (!planoData || !planoData.products[idx]) return;
  const p = planoData.products[idx];
  if (field === 'tablette' || field === 'position') p[field] = Math.max(0, parseInt(value) || 0);
  else if (field === 'name') p.name = value;
  else if (field === 'barcode') p.barcode = String(value || '').replace(/\s+/g, '');
  else if (field === 'code') p.code_familiprix = value;
  updatePlanoPreview();
}
function planoToggle(idx, field) {
  if (!planoData || !planoData.products[idx]) return;
  const p = planoData.products[idx];
  if (field === 'is_plano') p.is_plano = (p.is_plano === false);  // default true → false
  else if (field === 'en_stock') p.en_stock = !p.en_stock;
  else if (field === 'flipped_label') p.flipped_label = !p.flipped_label;
  updatePlanoPreview();
}
function planoRemoveLine(idx) {
  if (!planoData || !planoData.products[idx]) return;
  planoData.products.splice(idx, 1);
  updatePlanoPreview();
}
function planoAddLine() {
  if (!planoData) return;
  const tab = parseInt(document.getElementById('planoTabStart').value) || 1;
  planoData.products.push({tablette: tab, position: 1, barcode: '', code_familiprix: '',
                           name: 'Nouveau produit', is_new: false, en_stock: true, is_plano: false});
  updatePlanoPreview();
}

// Number of sections on a given aisle côté (0 if the aisle/côté isn't in the plan).
function planoSectionCount(aisle, side) {
  const layout = (typeof mapLayouts !== 'undefined' ? mapLayouts : []).find(l => String(l.aisle) === String(aisle));
  return (((layout && layout.config && layout.config.sides && layout.config.sides[side]) || {}).sections || []).length;
}

// The section a planogram should START from for a côté: Côté A (Gauche) fills from
// the Façade B end (its highest section) toward Façade A, so it starts at the last
// section; Côté B (Droite) starts at section 1 (Façade A end). A one-sided wall
// (no opposite côté — Labo, Caisse…) reads plainly left→right: start at section 1.
function planoDefaultStartSection(aisle, side) {
  if (side !== 'Gauche') return 1;
  if (!planoSectionCount(aisle, 'Droite')) return 1;   // mur à un seul côté
  return Math.max(1, planoSectionCount(aisle, side));
}

// Côté or allée changed: refresh the côté/façade list for the aisle, reset the
// "Section de départ" to that côté's natural starting end (hidden entirely for
// façades/présentoirs — they have no sections), then refresh the preview.
function onPlanoSideChange() {
  const aisle = document.getElementById('planoAisle')?.value;
  populatePlanoSides(aisle);
  const side = document.getElementById('planoSide')?.value;
  const isFixture = side !== 'Gauche' && side !== 'Droite';
  const secEl = document.getElementById('planoSection');
  if (secEl) secEl.value = isFixture ? '1' : String(planoDefaultStartSection(aisle, side));
  const secField = document.getElementById('planoSectionField');
  if (secField) secField.style.display = isFixture ? 'none' : '';
  updatePlanoPreview();
}

// Mirror of the server's plan_planogram_flow: lay each plano shelf (= lines that
// share a plano tablette) into the plan's EXISTING tablettes starting at
// (startSection, startTablette), rolling into the next section when one is full.
// Tablette COUNT per section is the plan's and never changes here. Returns a map
// from product index → {section, shelf, position}, plus the set of overflow rows.
function computePlanoFlow(config, side, startSection, startTablette, tabStart, tabEnd, skipNS) {
  const out = { byIdx: {}, overflow: new Set(), placed: 0 };
  const slots = [];   // [section_no, shelf_index] in fill order
  const fixture = _planoFixtureForSide(config, side);
  let singleSided = false;
  if (fixture) {
    // Fixture sides (Façade A/B, présentoir façades) are one flat run of
    // tablettes with no sections — fill from the start tablette downward.
    const shelves = fixture.shelves || [];
    for (let ti = Math.max(0, startTablette - 1); ti < shelves.length; ti++) slots.push([1, ti]);
  } else {
    const sections = ((config && config.sides && config.sides[side]) ? config.sides[side].sections : []) || [];
    // A one-sided "aisle" (Labo, Caisse, mur…) has no opposite côté and thus no
    // real Façade A/B ends: read PLAINLY left→right, nothing inverted.
    const other = side === 'Gauche' ? 'Droite' : 'Gauche';
    singleSided = !(((config && config.sides && config.sides[other]) || {}).sections || []).length;
    // Direction mirrors the server's plan_planogram_flow: Côté A (Gauche) reads Façade B
    // → Façade A, so it fills sections DESCENDING from the start section down to section 1;
    // Côté B (Droite) fills ascending. Only the section order flips (tablettes stay top→bottom).
    const startIdx = Math.min(Math.max(0, startSection - 1), Math.max(0, sections.length - 1));
    const pushSection = si => {
      const shelfCount = ((sections[si] || {}).shelves || []).length;
      const firstT = (si === startIdx) ? (startTablette - 1) : 0;
      for (let ti = Math.max(0, firstT); ti < shelfCount; ti++) slots.push([si + 1, ti]);
    };
    if (side === 'Gauche' && !singleSided) { for (let si = startIdx; si >= 0; si--) pushSection(si); }
    else { for (let si = startIdx; si < sections.length; si++) pushSection(si); }
  }
  const byTab = new Map();
  (planoData.products || []).forEach((p, idx) => {
    if (p.tablette < tabStart || p.tablette > tabEnd) return;
    if (skipNS && !p.en_stock) return;
    if (!byTab.has(p.tablette)) byTab.set(p.tablette, []);
    byTab.get(p.tablette).push(idx);
  });
  // STORE CONVENTION (mirror of the server): positions always count from the
  // Façade A end toward Façade B, on BOTH côtés. A plano reads left→right facing
  // the shelf, which on Côté A runs B→A — so its positions are MIRRORED there.
  // One-sided walls (no opposite côté) are read plainly: no mirroring.
  const mirrorPositions = (side === 'Gauche' && !singleSided);
  [...byTab.keys()].sort((a, b) => a - b).forEach((t, i) => {
    const idxs = byTab.get(t).slice().sort((a, b) => (planoData.products[a].position || 0) - (planoData.products[b].position || 0));
    if (i >= slots.length) { idxs.forEach(idx => out.overflow.add(idx)); return; }
    const [secNo, ti] = slots[i];
    const maxPos = Math.max(0, ...idxs.map(idx => planoData.products[idx].position || 0));
    idxs.forEach(idx => {
      const raw = planoData.products[idx].position;
      out.byIdx[idx] = { section: secNo, shelf: ti + 1, position: mirrorPositions ? (maxPos + 1 - raw) : raw };
      out.placed++;
    });
  });
  return out;
}

// Mirror of the server's fixture_for_side: the {shelves, labels} of a fixture
// side (Façade A/B or '<présentoir> - <façade>'), or null for the aisle côtés.
function _planoFixtureForSide(config, side) {
  if (!config || side === 'Gauche' || side === 'Droite') return null;
  if (side === 'Façade A') return config.facade_a || null;
  if (side === 'Façade B') return config.facade_b || null;
  for (const pres of (config.presentoirs || [])) {
    for (const f of (pres.facades || [])) {
      if (side === `${pres.name} - ${f.name}`) return f;
    }
  }
  return null;
}

// Fill the Côté selector with everything importable on this aisle: the two
// côtés always, plus the façades/présentoir façades that have tablettes.
function populatePlanoSides(aisle) {
  const sel = document.getElementById('planoSide');
  if (!sel) return;
  const layout = (typeof mapLayouts !== 'undefined' ? mapLayouts : []).find(l => String(l.aisle) === String(aisle));
  const cfg = layout ? layout.config : null;
  const opts = [['Gauche', 'Côté A'], ['Droite', 'Côté B'], ['Façade A', '🔲 Façade A'], ['Façade B', '🔲 Façade B']];
  for (const pres of (cfg?.presentoirs || [])) {
    for (const f of (pres.facades || [])) {
      const s = `${pres.name} - ${f.name}`;
      opts.push([s, `📦 ${s}`]);
    }
  }
  const current = sel.value;
  sel.innerHTML = opts.map(([v, label]) => `<option value="${esc(v)}">${esc(label)}</option>`).join('');
  sel.value = opts.some(([v]) => v === current) ? current : 'Gauche';
}

// Debounced preview: typing in the config fields rebuilds the whole row list
// (often 100+ rows). Coalesce keystrokes so we rebuild once the user pauses,
// instead of on every character — keeps the phone cool and responsive.
let _planoPreviewTimer = null;
function schedulePlanoPreview() {
  window.clearTimeout(_planoPreviewTimer);
  _planoPreviewTimer = window.setTimeout(updatePlanoPreview, 160);
}

function updatePlanoPreview() {
  if (!planoData) return;
  const aisle        = document.getElementById('planoAisle').value;
  const side         = document.getElementById('planoSide').value;
  const startSection = parseInt(document.getElementById('planoSection').value) || 1;
  const startTab     = parseInt(document.getElementById('planoStartTablette').value) || 1;
  const tabStart     = parseInt(document.getElementById('planoTabStart').value) || 1;
  const tabEnd       = parseInt(document.getElementById('planoTabEnd').value)   || 99;
  const skipNS       = document.getElementById('planoSkipNonStock').checked;
  const preview      = document.getElementById('planoPreview');

  const layout = (typeof mapLayouts !== 'undefined' ? mapLayouts : []).find(l => String(l.aisle) === String(aisle));
  const config = layout ? layout.config : null;
  const flow = computePlanoFlow(config, side, startSection, startTab, tabStart, tabEnd, skipNS);

  // Editable rows — each maps to its real index in planoData.products.
  const rows = planoData.products.map((p, idx) => {
    if (p.tablette < tabStart || p.tablette > tabEnd) return '';
    if (skipNS && !p.en_stock) return '';
    const isPlano = p.is_plano !== false;
    const place = flow.byIdx[idx];
    const isFixtureSide = side !== 'Gauche' && side !== 'Droite';
    const dest = place
      ? `→ Allée ${esc(aisle)} · ${esc(sideDisplayLabel(side))}${isFixtureSide ? '' : ` · S${esc(place.section)}`} · T${esc(place.shelf)} · P${esc(place.position)}`
      : `⚠ Hors plan — aucune tablette libre (le nombre de tablettes n'est pas modifié). Ajoutez des tablettes au plan ou changez le départ.`;
    return `<div style="display:flex;align-items:center;gap:6px;padding:6px 4px;border-bottom:1px solid #f1f5f9;flex-wrap:wrap${place ? '' : ';background:#fff5f5'}">
      <input type="number" value="${esc(p.tablette)}" title="Tablette (plano)" style="width:42px;padding:3px;font-size:12px;text-align:center"
             onchange="planoSet(${idx},'tablette',this.value)">
      <input type="number" value="${esc(p.position)}" title="Position" style="width:42px;padding:3px;font-size:12px;text-align:center"
             onchange="planoSet(${idx},'position',this.value)">
      <input type="text" value="${esc(p.name)}" title="Nom" style="flex:1;min-width:120px;padding:3px 6px;font-size:12px"
             onchange="planoSet(${idx},'name',this.value)">
      <input type="text" value="${esc(p.barcode || '')}" title="Code-barres (UPC)" placeholder="UPC" style="width:104px;padding:3px;font-size:12px;text-align:center;font-family:monospace"
             onchange="planoSet(${idx},'barcode',this.value)">
      <input type="text" value="${esc(p.code_familiprix || '')}" title="Code pharmacie" placeholder="code" style="width:60px;padding:3px;font-size:12px;text-align:center;color:#64748b"
             onchange="planoSet(${idx},'code',this.value)">
      <button title="${isPlano ? 'Plano — cliquer pour Hors-plano' : 'Hors-plano — cliquer pour Plano'}"
              onclick="planoToggle(${idx},'is_plano')"
              style="font-size:10px;font-weight:700;border:none;border-radius:6px;padding:3px 7px;cursor:pointer;${isPlano?'background:#eef2ff;color:#4338ca':'background:#f1f5f9;color:#64748b'}">${isPlano?'📋 PLANO':'HORS'}</button>
      <button title="${p.en_stock ? 'En stock — cliquer pour rupture' : 'Rupture — cliquer pour en stock'}"
              onclick="planoToggle(${idx},'en_stock')"
              style="font-size:10px;font-weight:700;border:none;border-radius:6px;padding:3px 7px;cursor:pointer;${p.en_stock?'background:#dcfce7;color:#15803d':'background:#fee2e2;color:#c8102e'}">${p.en_stock?'STOCK':'RUPTURE'}</button>
      ${!isPlano ? `<button title="Étiquette plano flippée en dessous"
              onclick="planoToggle(${idx},'flipped_label')"
              style="font-size:10px;font-weight:700;border:none;border-radius:6px;padding:3px 7px;cursor:pointer;${p.flipped_label?'background:#fef3c7;color:#92400e':'background:#f1f5f9;color:#94a3b8'}">🔄 ${p.flipped_label?'FLIPPÉE':'flip?'}</button>` : ''}
      <button title="Retirer cette ligne" onclick="planoRemoveLine(${idx})"
              style="border:1px solid #f1b8c2;color:#c8102e;background:#fff;border-radius:6px;padding:3px 7px;cursor:pointer;font-size:11px">✕</button>
      <div style="flex-basis:100%;font-size:10px;color:${place ? '#94a3b8' : '#c8102e'};padding-left:2px">${dest}</div>
    </div>`;
  }).filter(Boolean).join('');

  const overCount = flow.overflow.size;
  const overNote = overCount
    ? `<span style="font-size:11px;color:#c8102e;font-weight:700">${overCount} produit(s) hors plan</span>`
    : '';

  preview.innerHTML = `
    <div style="font-size:11px;color:#64748b;padding:4px 4px 6px">${(side === 'Gauche' && !planoSectionCount(aisle, 'Droite'))
      ? 'Allée à un seul côté (mur/comptoir) : <b>lecture simple de gauche à droite</b> — sections croissantes, positions telles quelles, rien d\'inversé.'
      : side === 'Gauche'
      ? 'Côté A : le plano remplit à partir de la section de départ <b>vers la Façade A</b> (sections décroissantes) et les positions sont <b>inversées</b>, car un planogramme se lit de gauche à droite face à la tablette. <b>Règle du magasin : sections ET positions comptent toujours de la Façade A vers la Façade B, sur les deux côtés.</b>'
      : side === 'Droite'
      ? 'Côté B : le plano remplit à partir de la section de départ <b>vers la Façade B</b> (sections croissantes).'
      : `${esc(side)} : le plano remplit les tablettes de la façade à partir de la tablette de départ, vers le bas.`} Le nombre de tablettes du plan ne change pas — seul le nombre de positions par tablette s'ajuste.</div>
    ${rows || '<div style="padding:10px;font-size:12px;color:#64748b">Aucun produit dans cette sélection.</div>'}
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 4px;border-top:1px solid #e2e8f0;margin-top:4px;flex-wrap:wrap">
      <button class="btn btn-outline btn-inline" style="font-size:12px;width:auto;margin:0" onclick="planoAddLine()">➕ Ajouter une ligne</button>
      <span style="font-size:12px;font-weight:700;color:#1e293b">${flow.placed} produit(s) placé(s) ${overNote}</span>
    </div>`;
}

async function importPlanogram() {
  if (!planoData) return;
  const aisle   = document.getElementById('planoAisle').value;
  if (!aisle) { document.getElementById('planoImportMsg').textContent = 'Choisissez une allée.'; return; }
  const side       = document.getElementById('planoSide').value;
  const startSec   = parseInt(document.getElementById('planoSection').value)        || 1;
  const startTab   = parseInt(document.getElementById('planoStartTablette').value)  || 1;
  const tabStart   = parseInt(document.getElementById('planoTabStart').value)       || 1;
  const tabEnd     = parseInt(document.getElementById('planoTabEnd').value)         || 99;
  const replace    = document.getElementById('planoReplace').checked;
  const skipNS     = document.getElementById('planoSkipNonStock').checked;

  const btn = document.getElementById('planoImportBtn');
  btn.disabled = true; btn.textContent = 'Importation…';
  const msg = document.getElementById('planoImportMsg');
  msg.textContent = '';

  try {
    const {res, data} = await apiFetch('/api/products/bulk-import', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({
        aisle, side,
        start_section:  startSec,
        start_tablette: startTab,
        tablette_start: tabStart,
        tablette_end:   tabEnd,
        replace_existing: replace,
        skip_non_stock:   skipNS,
        products: planoData.products,
        plano: planoData.plano || {},
        store: (typeof getCurrentStoreName === 'function') ? getCurrentStoreName() : ''
      })
    });
    if (res.ok && data.success) {
      const errTxt  = data.errors > 0 ? `, ${data.errors} erreur(s)` : '';
      const overTxt = data.overflow > 0 ? ` ⚠ ${data.overflow} produit(s) hors plan (pas assez de tablettes — le nombre de tablettes n'est pas modifié).` : '';
      msg.innerHTML = `✅ <strong>${data.imported}</strong> importé(s), ${data.skipped} ignoré(s)${errTxt}.${overTxt} Les photos manquantes sont récupérées automatiquement.`;
      msg.style.color = '#16a34a';
      refreshProductsCache();
      loadPlanogramHistory();
    } else {
      msg.textContent = data.error || 'Erreur lors de l’importation.';
      msg.style.color = '#c8102e';
    }
  } catch(e) {
    msg.textContent = 'Erreur réseau.'; msg.style.color = '#c8102e';
  }
  btn.disabled = false; btn.textContent = 'Importer dans le plan';
}

async function loadPlanogramHistory() {
  const box = document.getElementById('planoHistory');
  if (!box) return;
  try {
    const {res, data} = await apiFetch('/api/planograms/history');
    if (!res.ok || !Array.isArray(data) || !data.length) {
      box.innerHTML = '<div class="small" style="color:#94a3b8">Aucun planogramme importé pour le moment.</div>';
      return;
    }
    box.innerHTML = data.map(h => {
      const title = [h.plano_name, h.plano_number ? `#${h.plano_number}` : '', h.plano_version ? `(${h.plano_version})` : '']
        .filter(Boolean).join(' ') || 'Planogramme';
      const when = (h.created_at || '').replace('T', ' ').slice(0, 16);
      const loc = `Allée ${esc(h.aisle)} · ${esc(sideStaffLabel(h.side))} · S${esc(h.section)} · T${esc(h.tablette_start)}–${esc(h.tablette_end)}`;
      return `<div style="padding:8px 0;border-bottom:1px solid #f1f5f9">
        <div style="font-weight:600;font-size:13px">📋 ${esc(title)}</div>
        <div style="font-size:11px;color:#64748b">${loc}</div>
        <div style="font-size:11px;color:#94a3b8">${esc(when)} · ${esc(h.employee || '—')}${h.store ? ' · ' + esc(h.store) : ''} · ${h.imported} importé(s), ${h.skipped} ignoré(s)</div>
      </div>`;
    }).join('');
  } catch (e) {
    box.innerHTML = '<div class="small" style="color:#c8102e">Impossible de charger l’historique.</div>';
  }
}

window.AppLayout = { renderMapEditor, loadMapEditor, refreshPlanUi, createAisleLayout, saveAisleLayout, refreshProductsCache, refreshLayoutsCache, loadPlanogramHistory };
