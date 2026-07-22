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
  mapLayouts.sort((a, b) => {
    const orderA = Number(a.sort_order) > 0 ? Number(a.sort_order) : Number.MAX_SAFE_INTEGER;
    const orderB = Number(b.sort_order) > 0 ? Number(b.sort_order) : Number.MAX_SAFE_INTEGER;
    return orderA - orderB
      || (Number(a.aisle) || 0) - (Number(b.aisle) || 0)
      || String(a.aisle).localeCompare(String(b.aisle));
  });
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

const _layoutAutoSaveTimers = new Map();
const _layoutEditRevisions = new Map();
const _layoutAutoSaveInFlight = new Set();
const _layoutAutoSaveFailures = new Map();

function setLayoutSaveState(aisle, state, message='') {
  const el = document.getElementById(`aisleSaveState-${aisle}`);
  if (!el) return;
  const labels = {
    waiting: 'En attente...',
    saving: 'Enregistrement...',
    saved: 'Sauvegarde automatique',
    error: 'Echec - reessayer'
  };
  el.textContent = message || labels[state] || '';
  el.dataset.state = state;
  el.style.color = state === 'error' ? '#c8102e' : (state === 'saved' ? '#15803d' : '#d97706');
}

function scheduleLayoutAutoSave(aisle, delay=700) {
  const key = String(aisle);
  window.clearTimeout(_layoutAutoSaveTimers.get(key));
  _layoutAutoSaveTimers.set(key, window.setTimeout(() => autoSaveAisleLayout(key), delay));
}

function markLayoutDirty(aisle) {
  const key = String(aisle);
  dirtyLayoutAisles.add(key);
  _layoutAutoSaveFailures.set(key, 0);
  _layoutEditRevisions.set(key, (_layoutEditRevisions.get(key) || 0) + 1);
  setLayoutSaveState(key, 'waiting');
  scheduleLayoutAutoSave(key);
}
function clearLayoutDirty(aisle) { dirtyLayoutAisles.delete(String(aisle)); }
function hasDirtyLayouts() { return dirtyLayoutAisles.size > 0; }

async function autoSaveAisleLayout(aisle) {
  const key = String(aisle);
  _layoutAutoSaveTimers.delete(key);
  if (!dirtyLayoutAisles.has(key)) return;
  if (_layoutAutoSaveInFlight.has(key)) {
    scheduleLayoutAutoSave(key, 250);
    return;
  }
  const layout = getMutableLayout(key);
  if (!layout) return;
  const revision = _layoutEditRevisions.get(key) || 0;
  const config = readAisleLayoutConfig(key);
  _layoutAutoSaveInFlight.add(key);
  setLayoutSaveState(key, 'saving');
  const data = await apiUpdateLayoutAisle(key, {
    config,
    enabled: true,
    expected_modified_at: layout.modified_at || ''
  });
  _layoutAutoSaveInFlight.delete(key);

  if (!data.success) {
    const failures = (_layoutAutoSaveFailures.get(key) || 0) + 1;
    _layoutAutoSaveFailures.set(key, failures);
    setLayoutSaveState(key, 'error', data.error || (failures < 3 ? 'Sauvegarde impossible - nouvel essai' : 'Sauvegarde impossible'));
    if (data.code !== 'stale_layout' && failures < 3) {
      scheduleLayoutAutoSave(key, 3000 * failures);
    }
    return;
  }

  _layoutAutoSaveFailures.set(key, 0);
  layout.modified_by = loadEditorSession().username || layout.modified_by || '';
  layout.modified_at = data.modified_at || layout.modified_at || nowIsoWithoutMs();
  if ((_layoutEditRevisions.get(key) || 0) === revision) {
    clearLayoutDirty(key);
    setLayoutSaveState(key, 'saved');
  } else {
    setLayoutSaveState(key, 'waiting');
    scheduleLayoutAutoSave(key, 150);
  }
  savePlanSnapshot();

  if (Number(data.removed_products || 0) > 0) {
    await refreshProductsCache(true);
    refreshPlanUi();
  }
}

function compactPlanProduct(product) {
  if (!String(product?.aisle || '').trim()) return null;
  return {
    id: product.id ?? null,
    name: String(product.name || ''),
    brand: String(product.brand || ''),
    description: String(product.description || ''),
    image_url: String(product.image_url || ''),
    search_terms: String(product.search_terms || ''),
    usage_notes: String(product.usage_notes || ''),
    alternative_suggestions: String(product.alternative_suggestions || ''),
    barcode: String(product.barcode || ''),
    product_code: String(product.product_code || ''),
    aisle: String(product.aisle || ''),
    side: String(product.side || 'Gauche'),
    section: String(product.section || '1'),
    shelf: String(product.shelf || ''),
    position: String(product.position || ''),
    facings: Number(product.facings) > 0 ? Number(product.facings) : 1,
    is_plano: Number(product.is_plano) ? 1 : 0,
    in_stock: (product.in_stock === 0 || product.in_stock === '0') ? 0 : 1,
    linked_position: String(product.linked_position || ''),
    flipped_label: Number(product.flipped_label) ? 1 : 0,
    underneath_label: String(product.underneath_label || ''),
    modified_by: String(product.modified_by || ''),
    modified_at: String(product.modified_at || ''),
    created_by: String(product.created_by || ''),
    created_at: String(product.created_at || ''),
    last_change_by: String(product.last_change_by || product.modified_by || product.created_by || ''),
    last_change_at: String(product.last_change_at || product.modified_at || product.created_at || '')
  };
}

function savePlanSnapshot() {
  if (typeof isUnlocked === 'function' && !isUnlocked()) return;
  if (!STORAGE_KEYS.planSnapshot) return;
  if (!mapLayouts.length) {
    localStorage.removeItem(STORAGE_KEYS.planSnapshot);
    return;
  }
  try {
    const products = allProductsCache.map(compactPlanProduct).filter(Boolean);
    const snapshot = {
      savedAt: Date.now(),
      layouts: mapLayouts,
      products
    };
    let serialized = JSON.stringify(snapshot);
    if (serialized.length > 3800000) {
      snapshot.products = products.map(product => {
        const {
          description, image_url, search_terms, usage_notes,
          alternative_suggestions, ...compact
        } = product;
        return compact;
      });
      serialized = JSON.stringify(snapshot);
    }
    localStorage.setItem(STORAGE_KEYS.planSnapshot, serialized);
  } catch (e) {
    // A compact location/name snapshot is still far better than a blank app on
    // devices with a small localStorage quota.
    try {
      const products = allProductsCache.map(compactPlanProduct).filter(Boolean).map(product => {
        const {
          description, image_url, search_terms, usage_notes,
          alternative_suggestions, ...compact
        } = product;
        return compact;
      });
      localStorage.setItem(STORAGE_KEYS.planSnapshot, JSON.stringify({
        savedAt: Date.now(), layouts: mapLayouts, products
      }));
    } catch (_) {}
  }
}

function restorePlanSnapshot() {
  if (typeof isUnlocked === 'function' && !isUnlocked()) return false;
  if (!STORAGE_KEYS.planSnapshot || mapLayouts.length) return false;
  const raw = localStorage.getItem(STORAGE_KEYS.planSnapshot);
  if (!raw) return false;
  try {
    const snapshot = JSON.parse(raw);
    if (!snapshot || !Array.isArray(snapshot.layouts) || !snapshot.layouts.length) return false;
    const normalize = (typeof normalizeProduct === 'function') ? normalizeProduct : (item => item);
    allProductsCache = Array.isArray(snapshot.products) ? snapshot.products.map(normalize) : [];
    lastProductsRefreshAt = 0;
    mapLayouts = snapshot.layouts.map(layout => syncLayoutRecord({
      ...layout,
      config: normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position)
    }));
    sortMapLayouts();
    dirtyLayoutAisles = new Set();
    lastLayoutsRefreshAt = 0;
    return true;
  } catch (e) {
    localStorage.removeItem(STORAGE_KEYS.planSnapshot);
    return false;
  }
}

function applyPlanogramImportResult(aisle, side, data) {
  const aisleKey = String(aisle);
  const sideKey = String(side);

  if (data?.layout && typeof data.layout === 'object') {
    const nextLayout = syncLayoutRecord({
      ...data.layout,
      config: normalizeLayoutConfig(
        data.layout.config,
        data.layout.max_section,
        data.layout.max_shelf,
        data.layout.max_position
      )
    });
    const layoutIndex = mapLayouts.findIndex(item => String(item.aisle) === aisleKey);
    if (layoutIndex >= 0) mapLayouts[layoutIndex] = nextLayout;
    else mapLayouts.push(nextLayout);
    sortMapLayouts();
    lastLayoutsRefreshAt = Date.now();
  }

  if (Array.isArray(data?.products)) {
    const untouched = allProductsCache.filter(product => !(
      String(product.aisle) === aisleKey && String(product.side) === sideKey
    ));
    allProductsCache = untouched.concat(data.products.map(normalizeProduct));
    lastProductsRefreshAt = Date.now();
  }

  savePlanSnapshot();
}

function showPlanLoading(message='Chargement du plan...') {
  const div = document.getElementById('mapContent');
  if (div && !mapLayouts.length) div.innerHTML = `<div class="empty">${esc(message)}</div>`;
}

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
let _productsRefreshPromise = null;
let _layoutsRefreshPromise = null;

async function refreshProductsCache(force=false) {
  if (!force && allProductsCache.length && (Date.now() - lastProductsRefreshAt) < 30000) return allProductsCache;
  if (_productsRefreshPromise) return _productsRefreshPromise;
  _productsRefreshPromise = (async () => {
    try {
      allProductsCache = await apiGetProducts();
      mapLayouts.forEach(layout => syncLayoutRecord(layout));
      lastProductsRefreshAt = Date.now();
      savePlanSnapshot();
    } catch (e) {}
    return allProductsCache;
  })();
  try {
    return await _productsRefreshPromise;
  } finally {
    _productsRefreshPromise = null;
  }
}

async function refreshLayoutsCache(force=false) {
  if (!force && hasDirtyLayouts()) return mapLayouts;
  if (!force && mapLayouts.length && (Date.now() - lastLayoutsRefreshAt) < 30000) return mapLayouts;
  if (_layoutsRefreshPromise) return _layoutsRefreshPromise;
  _layoutsRefreshPromise = (async () => {
    try {
      mapLayouts = await apiGetLayoutAisles();
      mapLayouts = mapLayouts.map(layout => syncLayoutRecord({
        ...layout,
        config: normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position)
      }));
      sortMapLayouts();
      lastLayoutsRefreshAt = Date.now();
      savePlanSnapshot();
    } catch (e) {}
    return mapLayouts;
  })();
  try {
    return await _layoutsRefreshPromise;
  } finally {
    _layoutsRefreshPromise = null;
  }
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
  if (typeof isUnlocked === 'function' && !isUnlocked()) return;
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
  if (typeof isUnlocked === 'function' && !isUnlocked()) return;
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
  if (typeof isUnlocked === 'function' && !isUnlocked()) return;
  try {
    localStorage.setItem(STORAGE_KEYS.clientDraft, JSON.stringify({
      store_id: (typeof getCurrentStore === 'function' ? getCurrentStore()?.id : '') || '',
      question: document.getElementById('clientQuestion')?.value.trim() || '',
      conversation: (typeof getClientConversationForStorage === 'function')
        ? getClientConversationForStorage()
        : [],
      search_state: (typeof getClientSearchStateForStorage === 'function')
        ? getClientSearchStateForStorage()
        : null,
    }));
  } catch (_) {}
}

function loadClientDraft() {
  const saved = localStorage.getItem(STORAGE_KEYS.clientDraft);
  if (!saved) return;
  try {
    const draft = JSON.parse(saved);
    const currentStoreId = (typeof getCurrentStore === 'function' ? getCurrentStore()?.id : '') || '';
    if (draft.store_id && draft.store_id !== currentStoreId) return;
    document.getElementById('clientQuestion').value = draft.question || '';
    if (typeof restoreClientConversation === 'function') {
      restoreClientConversation(draft.conversation || []);
    }
    if (typeof restoreClientSearchState === 'function') {
      restoreClientSearchState(draft.search_state || null);
    }
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
    : 'Aide client IA inactive tant qu’une clé IA n’est pas configurée.');
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
let _skipPlanCaptureOnce = false;
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
  return '';                                                        // default: summary only
}

function isPlanNodeOpen(nodeId) {
  return openPlanNodes.has(nodeId) && !openPlanNodes.has('--closed--' + nodeId);
}

function directTreeBody(node) {
  return [...node.children].find(child => child.classList && child.classList.contains('tree-body'));
}

function hydratePlanNode(node) {
  if (!node || !node.open) return;
  const body = directTreeBody(node);
  if (!body || body.dataset.lazyEmpty !== '1') return;

  const kind = node.dataset.planKind || '';
  const aisle = node.dataset.aisle || '';
  const layout = mapLayouts.find(l => String(l.aisle) === String(aisle));
  const config = layout ? layout.config : null;

  if (kind === 'aisle' && layout && config) {
    renderMapEditor();
    return;
  } else if (kind === 'side' && config) {
    node.outerHTML = renderSide(aisle, node.dataset.side || '', config);
    return;
  } else if (kind === 'section' && config) {
    const side = node.dataset.side || '';
    const sectionIndex = Number(node.dataset.sectionIndex || 0);
    const section = config.sides?.[side]?.sections?.[sectionIndex];
    if (section) node.outerHTML = renderSection(aisle, side, sectionIndex, section);
  }
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
function regulatoryIdentifiersMarkup(product) {
  const identifiers = Array.isArray(product?.regulatory_identifiers)
    ? product.regulatory_identifiers : [];
  const rendered = identifiers.map(identifier => {
    const confirmed = identifier.status === 'confirmed';
    const type = identifier.type === 'DIN_HM' ? 'DIN-HM' : identifier.type;
    const title = confirmed
      ? 'Correspondance confirmée par une source officielle'
      : (identifier.match_method === 'health_canada_name_candidate'
        ? 'Candidat officiel trouvé par ressemblance du nom; confirmer le numéro sur l’emballage'
        : 'Correspondance probable liée à cet UPC, à confirmer sur l’emballage');
    return `<div class="meta-row"><span class="meta-label">${esc(type)}</span><span class="barcode-text">${esc(identifier.value)}</span><span title="${esc(title)}" style="font-size:10px;font-weight:700;color:${confirmed ? '#047857' : '#b45309'}">${confirmed ? 'CONFIRMÉ' : 'À CONFIRMER'}</span></div>`;
  }).join('');
  const hasDin = identifiers.some(identifier =>
    ['DIN', 'DIN_HM'].includes(String(identifier?.type || '').toUpperCase().replace('-', '_'))
  );
  const unavailable = hasDin ? '' : `<div class="meta-row" title="Aucun DIN ou DIN-HM n'est actuellement associé à ce produit dans le catalogue"><span class="meta-label">DIN / DIN-HM</span><span class="small">Non disponible</span></div>`;
  return rendered + unavailable;
}

function otherIdentifiersMarkup(product) {
  const labels = {
    UPC: 'UPC', GTIN: 'GTIN', FAMILIPRIX_CODE: 'Code Familiprix',
    MANUFACTURER_PART_NUMBER: 'No fabricant', SUPPLIER_ITEM_NUMBER: 'No fournisseur',
    WHOLESALER_ITEM_NUMBER: 'No grossiste', CASE_GTIN: 'GTIN caisse',
    INNER_GTIN: 'GTIN intérieur', PIN: 'PIN', NIP: 'NIP',
    PSEUDO_DIN: 'Pseudo-DIN', RAMQ_BILLING_CODE: 'Code RAMQ',
    INSURER_BILLING_CODE: 'Code assureur', HEALTH_CANADA_ID: 'ID Santé Canada',
    CLINICAL_ID: 'ID clinique',
  };
  const seen = new Set();
  return (Array.isArray(product?.identifiers) ? product.identifiers : [])
    .filter(identifier => !['DIN', 'NPN', 'DIN_HM'].includes(identifier.type))
    .filter(identifier => {
      const key = `${identifier.type}:${identifier.value}`;
      if (seen.has(key)) return false;
      seen.add(key);
      if (['UPC', 'GTIN'].includes(identifier.type)
          && normalizedDigits(identifier.value) === normalizedDigits(product.barcode)) return false;
      if (identifier.type === 'FAMILIPRIX_CODE'
          && String(identifier.value) === String(product.product_code)) return false;
      return true;
    })
    .slice(0, 8)
    .map(identifier => {
      const confirmed = identifier.status === 'confirmed';
      const status = confirmed ? '' : '<span style="font-size:10px;font-weight:700;color:#b45309">À CONFIRMER</span>';
      return `<div class="meta-row"><span class="meta-label">${esc(labels[identifier.type] || identifier.type)}</span><span class="barcode-text">${esc(identifier.value)}</span>${status}</div>`;
    }).join('');
}

function productCard(p, showDelete=true, showAiButton=true) {
  // Catalog-only products come from the imported planograms and have no shelf yet.
  const catalogOnly = p.catalog_only || !String(p.aisle || '').trim();
  const referenceImageBarcode = catalogOnly
    ? String(p.barcode || '').replace(/\D/g, '')
    : '';
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
      ${p.image_url
        ? `<img class="product-thumb" src="${esc(p.image_url)}" alt="Image produit">`
        : (p.id
          ? `<span class="product-thumb product-thumb-placeholder" data-product-image-id="${Number(p.id)}" aria-label="Photo en attente"></span>`
          : (referenceImageBarcode
            ? `<span class="product-thumb product-thumb-placeholder" data-reference-image-barcode="${referenceImageBarcode}" aria-label="Photo en attente"></span>`
            : ''))}
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
      ${regulatoryIdentifiersMarkup(p)}
      ${otherIdentifiersMarkup(p)}
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

const planSelectedProductIds = new Set();
let planMoveMode = false;
let planBulkActionBusy = false;
let _planScopeIndexVersion = -1;
let _planScopeIndex = null;
let planStructureMoveSource = null;
let planStructureMoveBusy = false;
let planStructurePointer = null;
let planStructurePointerFrame = 0;
let planStructureActiveDrop = null;
let planStructurePointerTarget = null;
let planStructureHoverNode = null;
let planStructureSuppressClickUntil = 0;
let planLastUndoAction = null;
let planUndoBusy = false;
const PLAN_MOVE_UNDO_STORAGE_KEY = 'familiprix_plan_move_undo_v1';
const PLAN_MOVE_UNDO_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function _planScopeKey(...parts) {
  return parts.map(part => String(part ?? '')).join('\x1f');
}

function planScopeIndex() {
  if (_planScopeIndex && _planScopeIndexVersion === lastProductsRefreshAt) return _planScopeIndex;
  const index = {
    aisle: new Map(), side: new Map(), section: new Map(), shelf: new Map(), validIds: new Set()
  };
  const add = (map, key, id) => {
    let ids = map.get(key);
    if (!ids) { ids = []; map.set(key, ids); }
    ids.push(id);
  };
  for (const product of allProductsCache) {
    const id = Number(product.id);
    if (!Number.isInteger(id) || id <= 0 || !String(product.aisle || '').trim()) continue;
    const aisle = String(product.aisle);
    const side = String(product.side);
    const section = String(product.section || '1');
    const shelf = String(product.shelf);
    index.validIds.add(id);
    add(index.aisle, _planScopeKey(aisle), id);
    add(index.side, _planScopeKey(aisle, side), id);
    add(index.section, _planScopeKey(aisle, side, section), id);
    add(index.shelf, _planScopeKey(aisle, side, section, shelf), id);
  }
  _planScopeIndex = index;
  _planScopeIndexVersion = lastProductsRefreshAt;
  return index;
}

function _planScopeValue(source, name, fallback='') {
  if (!source) return fallback;
  const datasetName = 'select' + name.charAt(0).toUpperCase() + name.slice(1);
  return source.dataset ? (source.dataset[datasetName] ?? fallback) : (source[name] ?? fallback);
}

function planScopeProductIds(kind, source={}) {
  const index = planScopeIndex();
  const normalizedKind = String(kind || _planScopeValue(source, 'kind')).toLowerCase();
  if (normalizedKind === 'product') {
    const id = Number(_planScopeValue(source, 'productId'));
    return index.validIds.has(id) ? [id] : [];
  }
  const aisle = String(_planScopeValue(source, 'aisle'));
  const side = String(_planScopeValue(source, 'side'));
  const section = String(_planScopeValue(source, 'section', '1'));
  const shelf = String(_planScopeValue(source, 'shelf'));
  if (normalizedKind === 'aisle') return index.aisle.get(_planScopeKey(aisle)) || [];
  if (normalizedKind === 'side') return index.side.get(_planScopeKey(aisle, side)) || [];
  if (normalizedKind === 'section') return index.section.get(_planScopeKey(aisle, side, section)) || [];
  if (normalizedKind === 'shelf') return index.shelf.get(_planScopeKey(aisle, side, section, shelf)) || [];
  return [];
}

function planSelectionDataAttrs(kind, aisle, side='', section='1', shelf='', productId='') {
  return `data-select-kind="${esc(kind)}" data-select-aisle="${esc(aisle)}" data-select-side="${esc(side)}" `
    + `data-select-section="${esc(section)}" data-select-shelf="${esc(shelf)}" data-select-product-id="${esc(productId)}"`;
}

function renderPlanSelectionCheckbox(kind, aisle, side='', section='1', shelf='', productId='', label='Sélectionner') {
  const source = {kind, aisle, side, section, shelf, productId};
  const ids = planScopeProductIds(kind, source);
  const checked = ids.length > 0 && ids.every(id => planSelectedProductIds.has(id));
  return `<input type="checkbox" class="plan-select-checkbox" ${planSelectionDataAttrs(kind, aisle, side, section, shelf, productId)}
    aria-label="${esc(label)}" title="${esc(label)}" ${checked ? 'checked' : ''} ${ids.length ? '' : 'disabled'}
    onclick="event.stopPropagation()" onchange="togglePlanSelectionFromInput(this,event)">`;
}

function togglePlanSelectionFromInput(input, event) {
  if (event) event.stopPropagation();
  const ids = planScopeProductIds(input.dataset.selectKind, input);
  for (const id of ids) {
    if (input.checked) planSelectedProductIds.add(id);
    else planSelectedProductIds.delete(id);
  }
  syncPlanSelectionUi();
}

function _planSelectionWrapper(input) {
  const kind = input?.dataset?.selectKind;
  if (kind === 'product') return input.closest('.plan-product-item');
  if (kind === 'shelf') return input.closest('.plan-shelf-card');
  if (kind === 'section') return input.closest('.plan-section');
  if (kind === 'side') return input.closest('.plan-side') || input.closest('.plan-section');
  if (kind === 'aisle') return input.closest('.plan-aisle-node');
  return null;
}

function syncPlanSelectionUi() {
  const index = planScopeIndex();
  for (const id of [...planSelectedProductIds]) {
    if (!index.validIds.has(id)) planSelectedProductIds.delete(id);
  }
  if (!planSelectedProductIds.size) planMoveMode = false;
  if (typeof document.querySelectorAll !== 'function') return;
  document.querySelectorAll('.plan-select-checkbox').forEach(input => {
    const ids = planScopeProductIds(input.dataset.selectKind, input);
    const selectedCount = ids.reduce((count, id) => count + (planSelectedProductIds.has(id) ? 1 : 0), 0);
    input.checked = ids.length > 0 && selectedCount === ids.length;
    input.indeterminate = selectedCount > 0 && selectedCount < ids.length;
    input.disabled = planBulkActionBusy || ids.length === 0;
    const wrapper = _planSelectionWrapper(input);
    if (wrapper?.classList) {
      wrapper.classList.toggle('plan-scope-selected', input.checked);
      wrapper.classList.toggle('plan-scope-partial', input.indeterminate);
    }
  });
  const count = planSelectedProductIds.size;
  const toolbar = document.getElementById('planBulkToolbar');
  if (toolbar) {
    toolbar.hidden = count === 0;
    toolbar.classList.toggle('is-busy', planBulkActionBusy);
    toolbar.setAttribute('aria-busy', planBulkActionBusy ? 'true' : 'false');
  }
  const countEl = document.getElementById('planSelectedCount');
  if (countEl) countEl.textContent = `${count} produit${count !== 1 ? 's' : ''} sélectionné${count !== 1 ? 's' : ''}`;
  const hint = document.getElementById('planSelectionHint');
  if (hint) hint.textContent = planMoveMode ? 'Destination à choisir' : 'Sélection active';
  const moveButton = document.getElementById('planSelectionMove');
  if (moveButton) {
    moveButton.disabled = planBulkActionBusy || count === 0;
    moveButton.setAttribute('aria-pressed', planMoveMode ? 'true' : 'false');
    moveButton.textContent = planMoveMode ? 'Annuler destination' : 'Déplacer';
  }
  const deleteButton = document.getElementById('planSelectionDelete');
  if (deleteButton) deleteButton.disabled = planBulkActionBusy || count === 0;
  const clearButton = document.getElementById('planSelectionClear');
  if (clearButton) clearButton.disabled = planBulkActionBusy || count === 0;
  const map = document.getElementById('mapContent');
  if (map?.classList) {
    map.classList.toggle('plan-move-mode', planMoveMode && count > 0);
    map.classList.toggle('plan-has-selection', count > 0);
  }
  document.getElementById('planMoveReceipt')?.classList?.toggle('above-plan-toolbar', count > 0);
}

function renderPlanBulkToolbar() {
  return `<div id="planBulkToolbar" class="plan-bulk-toolbar" hidden aria-live="polite"
      draggable="true" ondragstart="beginPlanSelectionDrag(event)" ondragend="endPlanSelectionDrag()">
    <div class="plan-bulk-status">
      <strong id="planSelectedCount">0 produit sélectionné</strong>
      <span id="planSelectionHint">Sélection active</span>
    </div>
    <div class="plan-bulk-actions">
      <button type="button" id="planSelectionMove" class="btn btn-inline" onclick="togglePlanMoveMode()">Déplacer</button>
      <button type="button" id="planSelectionDelete" class="btn btn-outline btn-inline plan-bulk-delete" onclick="deleteSelectedPlanProducts()">Supprimer sélection</button>
      <button type="button" id="planSelectionClear" class="btn btn-outline btn-inline" onclick="clearPlanSelection()">Effacer sélection</button>
    </div>
  </div>`;
}

function clearPlanSelection() {
  if (planBulkActionBusy) return;
  planSelectedProductIds.clear();
  planMoveMode = false;
  syncPlanSelectionUi();
}

function togglePlanMoveMode() {
  if (!planSelectedProductIds.size || planBulkActionBusy) return;
  planMoveMode = !planMoveMode;
  syncPlanSelectionUi();
}

function beginPlanSelectionDrag(event, productId=null) {
  if (planBulkActionBusy) { event.preventDefault(); return; }
  const id = Number(productId);
  if (Number.isInteger(id) && id > 0 && !planSelectedProductIds.has(id)) {
    planSelectedProductIds.add(id);
  }
  if (!planSelectedProductIds.size) { event.preventDefault(); return; }
  planMoveMode = false;
  if (event.dataTransfer) {
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', [...planSelectedProductIds].join(','));
  }
  document.getElementById('mapContent')?.classList.add('plan-dragging');
  syncPlanSelectionUi();
}

function endPlanSelectionDrag() {
  document.getElementById('mapContent')?.classList.remove('plan-dragging');
}

function allowPlanSelectionDrop(event) {
  if (!planSelectedProductIds.size || planBulkActionBusy) return;
  event.preventDefault();
  if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
}

function planDropTargetAttrs(aisle, side, section='1', shelf='', mode='shelf') {
  return `data-drop-aisle="${esc(aisle)}" data-drop-side="${esc(side)}" data-drop-section="${esc(section)}" `
    + `data-drop-shelf="${esc(shelf)}" data-drop-mode="${esc(mode)}" `
    + 'ondragover="allowPlanSelectionDrop(event)" ondrop="dropPlanSelectionOnElement(event,this)"';
}

function renderPlanDropButton(aisle, side, section='1', shelf='', mode='shelf') {
  return `<button type="button" class="plan-drop-action" ${planDropTargetAttrs(aisle, side, section, shelf, mode)}
    onclick="movePlanSelectionFromElement(this,event)">Déposer ici</button>`;
}

function _planDropTarget(element) {
  return {
    aisle: String(element?.dataset?.dropAisle || ''),
    side: String(element?.dataset?.dropSide || ''),
    section: String(element?.dataset?.dropSection || '1'),
    shelf: String(element?.dataset?.dropShelf || ''),
    mode: String(element?.dataset?.dropMode || 'shelf'),
  };
}

function dropPlanSelectionOnElement(event, element) {
  event.preventDefault();
  event.stopPropagation();
  endPlanSelectionDrag();
  movePlanSelection(_planDropTarget(element));
}

function movePlanSelectionFromElement(element, event) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  movePlanSelection(_planDropTarget(element));
}

function _selectedProductVersions(productIds) {
  const selected = new Set(productIds.map(Number));
  return Object.fromEntries(allProductsCache
    .filter(product => selected.has(Number(product.id)))
    .map(product => [String(product.id), String(product.modified_at || '')]));
}

function _applyBulkProductUpdates(products) {
  const updates = new Map((products || []).map(product => [Number(product.id), product]));
  allProductsCache = allProductsCache.map(product => {
    const update = updates.get(Number(product.id));
    return update ? normalizeProduct({...product, ...update}) : product;
  });
  if (typeof invalidateProductSearchIndexes === 'function') invalidateProductSearchIndexes();
  lastProductsRefreshAt = Date.now();
  savePlanSnapshot();
}

function setPlanBulkBusy(busy) {
  planBulkActionBusy = Boolean(busy);
  syncPlanSelectionUi();
}

async function movePlanSelection(target) {
  const productIds = [...planSelectedProductIds];
  if (!productIds.length || planBulkActionBusy) return;
  if (!requireEditorSession('deplacer les produits selectionnes')) return;
  const aisle = String(target.aisle || '');
  await waitForLayoutSave(aisle);
  if (dirtyLayoutAisles.has(aisle)) await autoSaveAisleLayout(aisle);
  await waitForLayoutSave(aisle);
  if (dirtyLayoutAisles.has(aisle)) {
    showPlanActionMessage('La destination doit etre sauvegardee avant le deplacement.');
    return;
  }
  const layout = getMutableLayout(aisle);
  if (!layout) {
    showPlanActionMessage('Destination introuvable. Rechargez le plan.');
    return;
  }
  setPlanBulkBusy(true);
  const data = await apiBulkMoveLayoutProducts({
    product_ids: productIds,
    target,
    expected_layout_modified_at: layout.modified_at || '',
    expected_products: _selectedProductVersions(productIds),
  });
  setPlanBulkBusy(false);
  if (!data.success) {
    showPlanActionMessage(data.error || 'Deplacement impossible. Aucun produit n a ete modifie.');
    return;
  }
  _applyBulkProductUpdates(data.product_updates || data.products || []);
  planSelectedProductIds.clear();
  planMoveMode = false;
  refreshPlanUi();
  showPlanActionMessage(`${Number(data.moved_products || productIds.length)} produit(s) deplace(s).`, 'success');
}

function renderPlanScopeDeleteButton(kind, aisle, side='', section='1', shelf='', count=0) {
  if (!Number(count)) return '';
  return `<button type="button" class="btn btn-outline btn-inline plan-products-only-delete"
    ${planSelectionDataAttrs(kind, aisle, side, section, shelf, '')}
    onclick="deletePlanScopeProductsFromElement(this,event)">Vider produits (${Number(count)})</button>`;
}

function deletePlanScopeProductsFromElement(element, event) {
  if (event) { event.preventDefault(); event.stopPropagation(); }
  const ids = planScopeProductIds(element.dataset.selectKind, element);
  deletePlanProducts(ids, 'cette zone');
}

function deleteSelectedPlanProducts() {
  deletePlanProducts([...planSelectedProductIds], 'la selection');
}

async function deletePlanProducts(productIds, scopeLabel='cette zone') {
  const ids = [...new Set((productIds || []).map(Number).filter(id => Number.isInteger(id) && id > 0))];
  if (!ids.length || planBulkActionBusy) return;
  if (!requireEditorSession('supprimer des produits du plan')) return;
  if (!confirm(`Supprimer ${ids.length} produit(s) de ${scopeLabel} ?\n\nLa structure du plan restera intacte.`)) return;
  setPlanBulkBusy(true);
  const data = await apiBulkDeleteLayoutProducts({
    product_ids: ids,
    expected_products: _selectedProductVersions(ids),
  });
  setPlanBulkBusy(false);
  if (!data.success) {
    showPlanActionMessage(data.error || 'Suppression impossible. Aucun produit n a ete retire.');
    return;
  }
  const deleted = new Set((data.deleted_product_ids || ids).map(Number));
  allProductsCache = allProductsCache.filter(product => !deleted.has(Number(product.id)));
  for (const id of deleted) planSelectedProductIds.delete(id);
  if (typeof invalidateProductSearchIndexes === 'function') invalidateProductSearchIndexes();
  lastProductsRefreshAt = Date.now();
  savePlanSnapshot();
  planMoveMode = false;
  refreshPlanUi();
  showPlanActionMessage(`${Number(data.removed_products || deleted.size)} produit(s) supprime(s); structure conservee.`, 'success');
}

function planStructureKey(item) {
  return [
    item?.kind || '', item?.aisle || '', item?.side || '',
    item?.sectionIndex ?? '', item?.index ?? ''
  ].join('\x1f');
}

function planStructureLabel(item) {
  if (!item) return '';
  if (item.kind === 'aisle') return `Allée ${item.aisle}`;
  if (item.kind === 'section') {
    return `Allée ${item.aisle} · ${sideDisplayLabel(item.side)} · Section ${Number(item.index) + 1}`;
  }
  return `Allée ${item.aisle} · ${sideDisplayLabel(item.side)} · Section ${Number(item.sectionIndex) + 1} · Tablette ${Number(item.index) + 1}`;
}

function planMoveList(values) {
  const items = (values || []).map(value => String(value));
  if (items.length < 2) return items[0] || '';
  if (items.length === 2) return `${items[0]} et ${items[1]}`;
  return `${items.slice(0, -1).join(', ')} et ${items[items.length - 1]}`;
}

function describePlanAisleMove(aisle, sourceIndex, finalIndex, beforeOrder, afterOrder) {
  const previous = afterOrder[finalIndex - 1];
  const next = afterOrder[finalIndex + 1];
  let landing = '';
  if (previous && next) landing = `entre les allées ${previous} et ${next}`;
  else if (previous) landing = `après l’allée ${previous}`;
  else if (next) landing = `avant l’allée ${next}`;

  const start = Math.min(sourceIndex, finalIndex);
  const end = Math.max(sourceIndex, finalIndex);
  const shifted = beforeOrder.slice(start, end + 1).filter(value => String(value) !== String(aisle));
  let detail = `Allée ${aisle} déplacée de la position ${sourceIndex + 1} à la position ${finalIndex + 1}`;
  if (landing) detail += `, ${landing}`;
  detail += '.';
  if (shifted.length === 1) {
    detail += ` Elle a échangé sa place avec l’allée ${shifted[0]}.`;
  } else if (shifted.length > 1) {
    detail += ` Allées décalées : ${planMoveList(shifted)}.`;
  }
  return detail;
}

function describePlanStructureMove(source, finalTarget) {
  const sourceIndex = Number(source.index) + 1;
  const finalIndex = Number(finalTarget.index) + 1;
  if (source.kind === 'section') {
    const from = `Allée ${source.aisle} · ${sideDisplayLabel(source.side)}`;
    const destination = `Allée ${finalTarget.aisle} · ${sideDisplayLabel(finalTarget.side)}`;
    if (String(source.aisle) === String(finalTarget.aisle) && source.side === finalTarget.side) {
      return `Section ${sourceIndex} déplacée de la position ${sourceIndex} à la position ${finalIndex} dans ${destination}.`;
    }
    return `Section ${sourceIndex} (${from}) déplacée vers la position ${finalIndex} de ${destination}.`;
  }
  const sourceSection = Number(source.sectionIndex) + 1;
  const finalSection = Number(finalTarget.section_index ?? finalTarget.sectionIndex) + 1;
  const from = `Allée ${source.aisle} · ${sideDisplayLabel(source.side)} · Section ${sourceSection}`;
  const destination = `Allée ${finalTarget.aisle} · ${sideDisplayLabel(finalTarget.side)} · Section ${finalSection}`;
  const sameContainer = String(source.aisle) === String(finalTarget.aisle)
    && source.side === finalTarget.side && sourceSection === finalSection;
  if (sameContainer) {
    return `Tablette ${sourceIndex} déplacée de la position ${sourceIndex} à la position ${finalIndex} dans ${destination}.`;
  }
  return `Tablette ${sourceIndex} (${from}) déplacée vers la position ${finalIndex} de ${destination}.`;
}

function buildPlanStructureInverse(source, finalTarget) {
  const kind = source.kind;
  const current = {
    kind,
    aisle: String(finalTarget.aisle),
    side: String(finalTarget.side),
    index: Number(finalTarget.index),
  };
  const destination = {
    kind,
    aisle: String(source.aisle),
    side: String(source.side),
    index: Number(source.index),
  };
  if (kind === 'shelf') {
    current.sectionIndex = Number(finalTarget.section_index ?? finalTarget.sectionIndex);
    destination.sectionIndex = Number(source.sectionIndex);
  }
  const sameContainer = kind === 'section'
    ? current.aisle === destination.aisle && current.side === destination.side
    : current.aisle === destination.aisle && current.side === destination.side
      && current.sectionIndex === destination.sectionIndex;
  if (sameContainer && destination.index > current.index) destination.index += 1;
  return {source: current, target: destination};
}

function planStructureDropIsNoop(source, target) {
  const sourceIndex = Number(source.index);
  let itemCount = 0;
  if (source.kind === 'aisle') {
    const actualIndex = mapLayouts.findIndex(layout => String(layout.aisle) === String(source.aisle));
    if (actualIndex < 0) return false;
    let boundary = Math.max(0, Math.min(Number(target.index) || 0, mapLayouts.length));
    if (boundary > actualIndex) boundary -= 1;
    return Math.max(0, Math.min(boundary, mapLayouts.length - 1)) === actualIndex;
  }
  const sameContainer = source.kind === 'section'
    ? String(source.aisle) === String(target.aisle) && source.side === target.side
    : String(source.aisle) === String(target.aisle) && source.side === target.side
      && Number(source.sectionIndex) === Number(target.sectionIndex);
  if (!sameContainer) return false;
  const layout = getMutableLayout(source.aisle);
  if (source.kind === 'section') {
    itemCount = layout?.config?.sides?.[source.side]?.sections?.length || 0;
  } else {
    itemCount = layout?.config?.sides?.[source.side]?.sections?.[Number(source.sectionIndex)]?.shelves?.length || 0;
  }
  let boundary = Math.max(0, Math.min(Number(target.index) || 0, itemCount));
  if (boundary > sourceIndex) boundary -= 1;
  return Math.max(0, Math.min(boundary, Math.max(0, itemCount - 1))) === sourceIndex;
}

function clearStoredPlanUndoAction() {
  try { localStorage.removeItem(PLAN_MOVE_UNDO_STORAGE_KEY); } catch (_error) {}
}

function rememberPlanUndoAction(action) {
  planLastUndoAction = action;
  try { localStorage.setItem(PLAN_MOVE_UNDO_STORAGE_KEY, JSON.stringify(action)); } catch (_error) {}
}

function isValidStoredPlanUndoAction(action) {
  if (!['aisle', 'section', 'shelf'].includes(action?.kind)) return false;
  if (typeof action.description !== 'string' || !action.description.trim()) return false;
  if (!action.expectedLayouts || typeof action.expectedLayouts !== 'object'
      || !Object.keys(action.expectedLayouts).length) return false;
  if (action.kind === 'aisle') {
    return Array.isArray(action.previousOrder) && action.previousOrder.length > 0;
  }
  const source = action.inverse?.source;
  const target = action.inverse?.target;
  const commonValid = Boolean(source && target && source.kind === action.kind && target.kind === action.kind
    && source.aisle && target.aisle && source.side && target.side
    && Number.isInteger(Number(source.index)) && Number.isInteger(Number(target.index)));
  return commonValid && (action.kind !== 'shelf'
    || (Number.isInteger(Number(source.sectionIndex)) && Number.isInteger(Number(target.sectionIndex))));
}

function loadStoredPlanUndoAction() {
  if (planLastUndoAction) return planLastUndoAction;
  try {
    const raw = localStorage.getItem?.(PLAN_MOVE_UNDO_STORAGE_KEY);
    if (!raw) return null;
    const action = JSON.parse(raw);
    const age = Date.now() - Number(action?.createdAt || 0);
    if (!isValidStoredPlanUndoAction(action) || age < 0 || age > PLAN_MOVE_UNDO_MAX_AGE_MS) {
      clearStoredPlanUndoAction();
      return null;
    }
    planLastUndoAction = action;
    return action;
  } catch (_error) {
    clearStoredPlanUndoAction();
    return null;
  }
}

function ensurePlanMoveReceipt() {
  let receipt = document.getElementById('planMoveReceipt');
  if (receipt || typeof document.createElement !== 'function' || !document.body?.appendChild) return receipt;
  receipt = document.createElement('div');
  receipt.id = 'planMoveReceipt';
  receipt.className = 'plan-move-receipt';
  receipt.setAttribute('role', 'status');
  receipt.setAttribute('aria-live', 'polite');
  receipt.innerHTML = `<div class="plan-move-receipt-copy">
      <div class="plan-move-receipt-heading"><strong>Dernier déplacement</strong><span id="planMoveReceiptTime"></span></div>
      <div id="planMoveReceiptDetail" class="plan-move-receipt-detail"></div>
      <div id="planMoveReceiptError" class="plan-move-receipt-error" hidden></div>
    </div>
    <div class="plan-move-receipt-actions">
      <button type="button" id="planMoveUndoButton" class="btn btn-inline" onclick="undoLastPlanMove()">↶ Annuler</button>
      <button type="button" class="plan-move-receipt-close" title="Fermer" aria-label="Fermer" onclick="dismissPlanMoveReceipt(true)">×</button>
    </div>`;
  document.body.appendChild(receipt);
  return receipt;
}

function showPlanMoveReceipt(action, updatePageMessage=true) {
  if (!action) return;
  planLastUndoAction = action;
  if (updatePageMessage) showPlanActionMessage(action.description, 'success');
  const receipt = ensurePlanMoveReceipt();
  if (!receipt) return;
  receipt.classList.toggle('above-plan-toolbar', planSelectedProductIds.size > 0);
  receipt.hidden = false;
  const heading = receipt.querySelector?.('.plan-move-receipt-heading strong');
  if (heading) heading.textContent = 'Dernier déplacement';
  const detail = document.getElementById('planMoveReceiptDetail');
  if (detail) detail.textContent = action.description;
  const time = document.getElementById('planMoveReceiptTime');
  if (time) {
    time.textContent = new Date(action.createdAt).toLocaleTimeString('fr-CA', {hour: '2-digit', minute: '2-digit'});
  }
  const error = document.getElementById('planMoveReceiptError');
  if (error) { error.hidden = true; error.textContent = ''; }
  const button = document.getElementById('planMoveUndoButton');
  if (button) { button.hidden = false; button.disabled = false; button.textContent = '↶ Annuler'; }
}

function restorePlanMoveReceipt() {
  const action = loadStoredPlanUndoAction();
  if (action) showPlanMoveReceipt(action, false);
}

function dismissPlanMoveReceipt(clearAction=false) {
  document.getElementById('planMoveReceipt')?.remove();
  if (clearAction) {
    planLastUndoAction = null;
    clearStoredPlanUndoAction();
  }
}

function planStructureAttrs(kind, item) {
  return `data-structure-kind="${esc(kind)}" data-structure-aisle="${esc(item.aisle || '')}" `
    + `data-structure-side="${esc(item.side || '')}" data-structure-section-index="${esc(item.sectionIndex ?? '')}" `
    + `data-structure-index="${esc(item.index ?? '')}"`;
}

function planStructureItemFromElement(element) {
  return {
    kind: String(element?.dataset?.structureKind || ''),
    aisle: String(element?.dataset?.structureAisle || ''),
    side: String(element?.dataset?.structureSide || ''),
    sectionIndex: element?.dataset?.structureSectionIndex === ''
      ? null : Number(element?.dataset?.structureSectionIndex),
    index: Number(element?.dataset?.structureIndex),
  };
}

function renderPlanStructureHandle(kind, item, label) {
  return `<button type="button" class="plan-structure-handle" ${planStructureAttrs(kind, item)}
    aria-label="${esc(label)}" title="${esc(label)}"
    onclick="planStructureHandleClick(event,this)"
    onpointerdown="beginPlanStructurePointer(event,this)"
    onpointermove="movePlanStructurePointer(event)"
    onpointerup="endPlanStructurePointer(event)"
    onpointercancel="cancelPlanStructurePointer(event)">⠿</button>`;
}

function renderPlanStructureDropZone(kind, item, label) {
  return `<button type="button" class="plan-structure-drop-zone plan-structure-drop-${esc(kind)}"
    ${planStructureAttrs(kind, item)} onclick="commitPlanStructureDropFromElement(event,this)">
    <span aria-hidden="true">↳</span><span>${esc(label)}</span>
  </button>`;
}

function renderPlanStructureStatus() {
  return `<div id="planStructureMoveStatus" class="plan-structure-status" hidden aria-live="polite">
    <div><strong id="planStructureMoveTitle"></strong><span id="planStructureMoveTarget"></span></div>
    <button type="button" aria-label="Annuler le déplacement" title="Annuler" onclick="cancelPlanStructureMove()">✕</button>
  </div>`;
}

function syncPlanStructureMoveUi() {
  const map = document.getElementById('mapContent');
  if (!map?.classList) return;
  for (const kind of ['aisle', 'section', 'shelf']) {
    map.classList.toggle(`plan-structure-move-${kind}`, planStructureMoveSource?.kind === kind);
  }
  map.classList.toggle('plan-structure-active', Boolean(planStructureMoveSource));
  map.classList.toggle('plan-structure-busy', planStructureMoveBusy);
  const sourceKey = planStructureKey(planStructureMoveSource);
  map.querySelectorAll?.('.plan-structure-handle').forEach(handle => {
    handle.classList.toggle(
      'is-source', Boolean(planStructureMoveSource)
        && planStructureKey(planStructureItemFromElement(handle)) === sourceKey
    );
    handle.disabled = planStructureMoveBusy;
  });
  const status = document.getElementById('planStructureMoveStatus');
  if (status) status.hidden = !planStructureMoveSource;
  const title = document.getElementById('planStructureMoveTitle');
  if (title) title.textContent = planStructureMoveBusy
    ? 'Déplacement en cours…' : `Déplacer ${planStructureLabel(planStructureMoveSource)}`;
  const target = document.getElementById('planStructureMoveTarget');
  if (target && !planStructureMoveBusy && !target.textContent) {
    target.textContent = 'Choisissez une ligne verte';
  }
}

function setPlanStructureMoveSource(source) {
  if (planStructureMoveBusy) return;
  planStructureMoveSource = source;
  planMoveMode = false;
  const targetText = document.getElementById('planStructureMoveTarget');
  if (targetText) targetText.textContent = '';
  syncPlanSelectionUi();
  syncPlanStructureMoveUi();
}

function cancelPlanStructureMove() {
  if (planStructureMoveBusy) return;
  planStructureMoveSource = null;
  planStructureActiveDrop?.classList?.remove('is-active');
  planStructureActiveDrop = null;
  planStructureHoverNode?.classList?.remove(
    'plan-structure-hover-before', 'plan-structure-hover-after'
  );
  planStructureHoverNode = null;
  planStructurePointerTarget = null;
  const ghost = document.getElementById('planStructureDragGhost');
  if (ghost) ghost.remove();
  syncPlanStructureMoveUi();
}

function planStructureHandleClick(event, handle) {
  event.preventDefault();
  event.stopPropagation();
  if (Date.now() < planStructureSuppressClickUntil || planStructureMoveBusy) return;
  const source = planStructureItemFromElement(handle);
  if (planStructureMoveSource && planStructureKey(planStructureMoveSource) === planStructureKey(source)) {
    cancelPlanStructureMove();
    return;
  }
  setPlanStructureMoveSource(source);
}

function commitPlanStructureDropFromElement(event, element) {
  event.preventDefault();
  event.stopPropagation();
  if (!planStructureMoveSource || planStructureMoveBusy) return;
  commitPlanStructureDrop(planStructureMoveSource, planStructureItemFromElement(element));
}

function beginPlanStructurePointer(event, handle) {
  if (planStructureMoveBusy || event.button > 0) return;
  event.stopPropagation();
  const source = planStructureItemFromElement(handle);
  planStructurePointer = {
    pointerId: event.pointerId,
    pointerType: event.pointerType || 'mouse',
    handle,
    source,
    startX: event.clientX,
    startY: event.clientY,
    x: event.clientX,
    y: event.clientY,
    active: false,
    holdTimer: window.setTimeout(
      activatePlanStructurePointer,
      event.pointerType === 'mouse' ? 120 : 260
    ),
  };
  handle.classList.add('is-pressed');
  try { handle.setPointerCapture(event.pointerId); } catch (_error) {}
}

function activatePlanStructurePointer() {
  const drag = planStructurePointer;
  if (!drag || drag.active) return;
  window.clearTimeout(drag.holdTimer);
  drag.active = true;
  planStructureSuppressClickUntil = Date.now() + 500;
  setPlanStructureMoveSource(drag.source);
  const ghost = document.createElement('div');
  ghost.id = 'planStructureDragGhost';
  ghost.className = 'plan-structure-drag-ghost';
  ghost.textContent = planStructureLabel(drag.source);
  document.body.appendChild(ghost);
  document.getElementById('mapContent')?.classList.add('plan-structure-pointer-active');
  schedulePlanStructurePointerFrame();
}

function movePlanStructurePointer(event) {
  const drag = planStructurePointer;
  if (!drag || drag.pointerId !== event.pointerId) return;
  drag.x = event.clientX;
  drag.y = event.clientY;
  const distance = Math.hypot(drag.x - drag.startX, drag.y - drag.startY);
  if (!drag.active && distance > 7) activatePlanStructurePointer();
  if (drag.active) {
    event.preventDefault();
    schedulePlanStructurePointerFrame();
  }
}

function schedulePlanStructurePointerFrame() {
  if (planStructurePointerFrame) return;
  planStructurePointerFrame = window.requestAnimationFrame(renderPlanStructurePointerFrame);
}

function planStructureDirectTarget(pointed, kind, pointerY) {
  if (!pointed?.closest) return null;
  let node = null;
  let header = null;
  if (kind === 'aisle') {
    node = pointed.closest('.plan-aisle-node');
    header = node?.querySelector?.(':scope > summary');
  } else if (kind === 'section') {
    node = pointed.closest('.plan-section[data-plan-kind="section"]');
    header = node?.querySelector?.(':scope > summary');
  } else if (kind === 'shelf') {
    node = pointed.closest('.plan-shelf-card');
    header = node?.querySelector?.(':scope > .shelf-header');
  }
  if ((!node || !header) && kind === 'section') {
    const sideNode = pointed.closest('.plan-side[data-plan-kind="side"]');
    const aisle = String(sideNode?.dataset?.aisle || '');
    const side = String(sideNode?.dataset?.side || '');
    const layout = getMutableLayout(aisle);
    const sections = layout?.config?.sides?.[side]?.sections;
    if (sideNode && aisle && Array.isArray(sections)) {
      return {
        item: {kind, aisle, side, sectionIndex: null, index: sections.length},
        node: sideNode,
        after: true,
        label: `Fin de ${sideDisplayLabel(side)}`,
      };
    }
  }
  if ((!node || !header) && kind === 'shelf') {
    const sectionNode = pointed.closest('.plan-section[data-plan-kind="section"]');
    const aisle = String(sectionNode?.dataset?.aisle || '');
    const side = String(sectionNode?.dataset?.side || '');
    const sectionIndex = Number(sectionNode?.dataset?.sectionIndex);
    const layout = getMutableLayout(aisle);
    const shelves = layout?.config?.sides?.[side]?.sections?.[sectionIndex]?.shelves;
    if (sectionNode && aisle && Number.isInteger(sectionIndex) && Array.isArray(shelves)) {
      return {
        item: {kind, aisle, side, sectionIndex, index: shelves.length},
        node: sectionNode,
        after: true,
        label: `Fin de la section ${sectionIndex + 1}`,
      };
    }
  }
  if (!node || !header) return null;
  const handle = header.querySelector?.(`.plan-structure-handle[data-structure-kind="${kind}"]`);
  if (!handle) return null;
  const item = planStructureItemFromElement(handle);
  const rect = header.getBoundingClientRect();
  const after = pointerY > rect.top + (rect.height / 2);
  item.index += after ? 1 : 0;
  const positionLabel = after ? 'Après' : 'Avant';
  return {
    item,
    node,
    after,
    label: `${positionLabel} ${planStructureLabel(planStructureItemFromElement(handle))}`,
  };
}

function renderPlanStructurePointerFrame() {
  planStructurePointerFrame = 0;
  const drag = planStructurePointer;
  if (!drag?.active) return;
  const ghost = document.getElementById('planStructureDragGhost');
  if (ghost) {
    ghost.style.transform = `translate3d(${Math.round(drag.x + 12)}px,${Math.round(drag.y + 12)}px,0)`;
  }
  const pointed = typeof document.elementFromPoint === 'function'
    ? document.elementFromPoint(drag.x, drag.y) : null;
  const directTarget = planStructureDirectTarget(
    pointed, drag.source.kind, drag.y
  );
  if (directTarget?.node !== planStructureHoverNode || directTarget?.after !== planStructurePointerTarget?.after) {
    planStructureHoverNode?.classList?.remove(
      'plan-structure-hover-before', 'plan-structure-hover-after'
    );
    planStructureHoverNode = directTarget?.node || null;
    planStructurePointerTarget = directTarget || null;
    planStructureHoverNode?.classList?.add(
      directTarget?.after ? 'plan-structure-hover-after' : 'plan-structure-hover-before'
    );
    const targetText = document.getElementById('planStructureMoveTarget');
    if (targetText) {
      targetText.textContent = directTarget
        ? directTarget.label
        : 'Aucune destination';
    }
  }

  const topEdge = 138;
  const bottomEdge = Math.max(topEdge + 80, window.innerHeight - 72);
  let scrollSpeed = 0;
  if (drag.y < topEdge) {
    scrollSpeed = -Math.min(16, Math.max(3, (topEdge - drag.y) / 5));
  } else if (drag.y > bottomEdge) {
    scrollSpeed = Math.min(16, Math.max(3, (drag.y - bottomEdge) / 5));
  }
  if (scrollSpeed) {
    window.scrollBy(0, scrollSpeed);
    schedulePlanStructurePointerFrame();
  }
}

function cleanupPlanStructurePointer() {
  const drag = planStructurePointer;
  if (drag) {
    window.clearTimeout(drag.holdTimer);
    drag.handle?.classList?.remove('is-pressed');
    try { drag.handle?.releasePointerCapture?.(drag.pointerId); } catch (_error) {}
  }
  if (planStructurePointerFrame) window.cancelAnimationFrame(planStructurePointerFrame);
  planStructurePointerFrame = 0;
  planStructurePointer = null;
  document.getElementById('mapContent')?.classList.remove('plan-structure-pointer-active');
  document.getElementById('planStructureDragGhost')?.remove();
  planStructureActiveDrop?.classList?.remove('is-active');
  planStructureHoverNode?.classList?.remove(
    'plan-structure-hover-before', 'plan-structure-hover-after'
  );
  planStructureHoverNode = null;
}

function endPlanStructurePointer(event) {
  const drag = planStructurePointer;
  if (!drag || drag.pointerId !== event.pointerId) return;
  const wasActive = drag.active;
  const source = drag.source;
  const target = planStructurePointerTarget?.item || null;
  cleanupPlanStructurePointer();
  planStructureActiveDrop = null;
  planStructurePointerTarget = null;
  if (!wasActive) return;
  event.preventDefault();
  event.stopPropagation();
  planStructureSuppressClickUntil = Date.now() + 500;
  if (target) {
    commitPlanStructureDrop(source, target);
  } else {
    setPlanStructureMoveSource(source);
  }
}

function cancelPlanStructurePointer(event) {
  const drag = planStructurePointer;
  if (!drag || (event && drag.pointerId !== event.pointerId)) return;
  const source = drag.source;
  const wasActive = drag.active;
  cleanupPlanStructurePointer();
  planStructureActiveDrop = null;
  planStructurePointerTarget = null;
  if (wasActive) setPlanStructureMoveSource(source);
}

function planExpectedLayoutVersions(aisles) {
  const keys = new Set((aisles || []).map(String));
  return Object.fromEntries(mapLayouts
    .filter(layout => keys.has(String(layout.aisle)))
    .map(layout => [String(layout.aisle), String(layout.modified_at || '')]));
}

async function preparePlanStructureAisles(aisles) {
  const keys = [...new Set((aisles || []).map(String))];
  for (const key of keys) {
    await waitForLayoutSave(key);
    if (dirtyLayoutAisles.has(key)) await autoSaveAisleLayout(key);
    await waitForLayoutSave(key);
    if (dirtyLayoutAisles.has(key)) return false;
  }
  return true;
}

function applyPlanStructureResponse(data) {
  for (const [aisle, config] of Object.entries(data.configs || {})) {
    const layout = getMutableLayout(aisle);
    if (!layout) continue;
    layout.config = normalizeLayoutConfig(config);
    layout.modified_at = data.layout_versions?.[aisle] || layout.modified_at || nowIsoWithoutMs();
    layout.modified_by = loadEditorSession().username || layout.modified_by || '';
    clearLayoutDirty(aisle);
    syncLayoutRecord(layout);
  }
  if (Array.isArray(data.product_updates) && data.product_updates.length) {
    _applyBulkProductUpdates(data.product_updates);
  } else {
    savePlanSnapshot();
  }
  lastLayoutsRefreshAt = Date.now();
}

function finishPlanStructureMove(success=true) {
  planStructureMoveBusy = false;
  if (success) planStructureMoveSource = null;
  const targetText = document.getElementById('planStructureMoveTarget');
  if (targetText && success) targetText.textContent = '';
  syncPlanStructureMoveUi();
}

async function commitPlanStructureDrop(source, target) {
  if (!source || !target || source.kind !== target.kind || planStructureMoveBusy) return false;
  if (!requireEditorSession('deplacer la structure du plan')) return false;
  if (planStructureDropIsNoop(source, target)) {
    finishPlanStructureMove(true);
    showPlanActionMessage(`${planStructureLabel(source)} est déjà à cette position.`, 'info');
    return true;
  }
  const involvedAisles = source.kind === 'aisle'
    ? mapLayouts.map(layout => String(layout.aisle))
    : [String(source.aisle), String(target.aisle)];
  if (!await preparePlanStructureAisles(involvedAisles)) {
    showPlanActionMessage('Le plan doit etre sauvegarde avant ce deplacement.');
    return false;
  }

  planStructureMoveBusy = true;
  syncPlanStructureMoveUi();
  let data;
  let undoAction = null;
  if (source.kind === 'aisle') {
    const previousOrder = mapLayouts.map(layout => String(layout.aisle));
    const sourceIndex = mapLayouts.findIndex(
      layout => String(layout.aisle) === String(source.aisle)
    );
    if (sourceIndex < 0) {
      finishPlanStructureMove(false);
      showPlanActionMessage('Cette allee n existe plus. Rechargez le plan.');
      return false;
    }
    const nextLayouts = mapLayouts.slice();
    const [moving] = nextLayouts.splice(sourceIndex, 1);
    let insertionIndex = Math.max(0, Math.min(Number(target.index) || 0, mapLayouts.length));
    if (insertionIndex > sourceIndex) insertionIndex -= 1;
    insertionIndex = Math.max(0, Math.min(insertionIndex, nextLayouts.length));
    nextLayouts.splice(insertionIndex, 0, moving);
    data = await apiReorderLayoutAisles({
      ordered_aisles: nextLayouts.map(layout => String(layout.aisle)),
      expected_layouts: planExpectedLayoutVersions(involvedAisles),
    });
    if (data.success) {
      mapLayouts = nextLayouts;
      mapLayouts.forEach((layout, index) => {
        layout.sort_order = index + 1;
        layout.modified_at = data.layout_versions?.[String(layout.aisle)]
          || layout.modified_at || nowIsoWithoutMs();
      });
      lastLayoutsRefreshAt = Date.now();
      savePlanSnapshot();
      const nextOrder = nextLayouts.map(layout => String(layout.aisle));
      undoAction = {
        kind: 'aisle',
        createdAt: Date.now(),
        description: describePlanAisleMove(
          String(source.aisle), sourceIndex, insertionIndex, previousOrder, nextOrder
        ),
        previousOrder,
        expectedLayouts: {...(data.layout_versions || planExpectedLayoutVersions(involvedAisles))},
      };
    }
  } else {
    data = await apiMoveLayoutStructure(source.kind, {
      source,
      target,
      expected_layouts: planExpectedLayoutVersions(involvedAisles),
    });
    if (data.success) {
      applyPlanStructureResponse(data);
      const finalTarget = {kind: source.kind, ...data.target};
      undoAction = {
        kind: source.kind,
        createdAt: Date.now(),
        description: describePlanStructureMove(source, finalTarget),
        inverse: buildPlanStructureInverse(source, finalTarget),
        expectedLayouts: {...(data.layout_versions || planExpectedLayoutVersions(involvedAisles))},
      };
    }
  }

  if (!data?.success) {
    finishPlanStructureMove(false);
    const message = data?.error || 'Deplacement impossible. Aucun element n a ete modifie.';
    const targetText = document.getElementById('planStructureMoveTarget');
    if (targetText) targetText.textContent = message;
    showPlanActionMessage(message);
    return false;
  }
  finishPlanStructureMove(true);
  rememberPlanUndoAction(undoAction);
  _skipPlanCaptureOnce = true;
  refreshPlanUi();
  showPlanMoveReceipt(undoAction);
  return true;
}

function setPlanUndoBusyUi(busy) {
  planUndoBusy = Boolean(busy);
  const button = document.getElementById('planMoveUndoButton');
  if (button) {
    button.disabled = planUndoBusy;
    button.textContent = planUndoBusy ? 'Annulation…' : '↶ Annuler';
  }
}

function showPlanUndoFailure(message) {
  setPlanUndoBusyUi(false);
  planStructureMoveBusy = false;
  syncPlanStructureMoveUi();
  const receipt = ensurePlanMoveReceipt();
  if (receipt) receipt.hidden = false;
  const error = document.getElementById('planMoveReceiptError');
  if (error) { error.hidden = false; error.textContent = message; }
  showPlanActionMessage(message, 'error');
}

function finishPlanUndoSuccess(action) {
  planLastUndoAction = null;
  clearStoredPlanUndoAction();
  setPlanUndoBusyUi(false);
  planStructureMoveBusy = false;
  syncPlanStructureMoveUi();
  _skipPlanCaptureOnce = true;
  refreshPlanUi();
  const receipt = ensurePlanMoveReceipt();
  const heading = receipt?.querySelector?.('.plan-move-receipt-heading strong');
  if (heading) heading.textContent = 'Déplacement annulé';
  const detail = document.getElementById('planMoveReceiptDetail');
  if (detail) detail.textContent = `Retour effectué. ${action.description}`;
  const error = document.getElementById('planMoveReceiptError');
  if (error) { error.hidden = true; error.textContent = ''; }
  const button = document.getElementById('planMoveUndoButton');
  if (button) button.hidden = true;
  showPlanActionMessage(`Déplacement annulé. ${action.description}`, 'success');
}

async function undoLastPlanMove() {
  const action = planLastUndoAction || loadStoredPlanUndoAction();
  if (!action || planUndoBusy || planStructureMoveBusy) return false;
  if (!requireEditorSession('annuler le dernier déplacement du plan')) return false;
  const involvedAisles = action.kind === 'aisle'
    ? (action.previousOrder || []).map(String)
    : [...new Set([
        String(action.inverse?.source?.aisle || ''),
        String(action.inverse?.target?.aisle || ''),
      ].filter(Boolean))];
  for (const aisle of involvedAisles) {
    await waitForLayoutSave(aisle);
    if (dirtyLayoutAisles.has(String(aisle))) {
      showPlanUndoFailure('Annulation protégée : sauvegardez ou rechargez les modifications en cours avant de réessayer.');
      return false;
    }
  }
  const expectedLayouts = action.expectedLayouts || {};
  const localVersionsMatch = Object.entries(expectedLayouts).every(([aisle, version]) => {
    const layout = getMutableLayout(aisle);
    return layout && String(layout.modified_at || '') === String(version || '');
  });
  if (!localVersionsMatch) {
    showPlanUndoFailure('Annulation refusée : le plan a changé depuis ce déplacement. Aucune donnée n’a été modifiée.');
    return false;
  }

  planStructureMoveBusy = true;
  setPlanUndoBusyUi(true);
  syncPlanStructureMoveUi();
  let data;
  if (action.kind === 'aisle') {
    const byAisle = new Map(mapLayouts.map(layout => [String(layout.aisle), layout]));
    const previousOrder = (action.previousOrder || []).map(String);
    if (previousOrder.length !== mapLayouts.length || previousOrder.some(aisle => !byAisle.has(aisle))) {
      showPlanUndoFailure('Annulation refusée : la liste des allées a changé. Aucune donnée n’a été modifiée.');
      return false;
    }
    data = await apiReorderLayoutAisles({
      ordered_aisles: previousOrder,
      expected_layouts: expectedLayouts,
    });
    if (data.success) {
      mapLayouts = previousOrder.map(aisle => byAisle.get(aisle));
      mapLayouts.forEach((layout, index) => {
        layout.sort_order = index + 1;
        layout.modified_at = data.layout_versions?.[String(layout.aisle)]
          || layout.modified_at || nowIsoWithoutMs();
      });
      lastLayoutsRefreshAt = Date.now();
      savePlanSnapshot();
    }
  } else {
    data = await apiMoveLayoutStructure(action.kind, {
      source: action.inverse?.source,
      target: action.inverse?.target,
      expected_layouts: expectedLayouts,
    });
    if (data.success) applyPlanStructureResponse(data);
  }
  if (!data?.success) {
    showPlanUndoFailure(data?.error || 'Annulation impossible. Aucune donnée n’a été modifiée.');
    return false;
  }
  finishPlanUndoSuccess(action);
  return true;
}

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
      <button class="btn btn-inline" onclick="confirmMoveSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex})">Déplacer</button>
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
  const targetPosition = Math.max(1, Number(document.getElementById('msPosition')?.value) || 1);
  const sameContainer = String(aisle) === String(target_aisle) && side === target_side;
  const desiredIndex = targetPosition - 1;
  const targetBoundary = sameContainer && desiredIndex > sectionIndex
    ? desiredIndex + 1 : desiredIndex;
  const moved = await commitPlanStructureDrop(
    {kind: 'section', aisle: String(aisle), side, index: sectionIndex},
    {kind: 'section', aisle: String(target_aisle), side: target_side, index: targetBoundary},
  );
  if (moved) {
    if (overlay) overlay.remove();
  } else if (msg) {
    msg.textContent = document.getElementById('addMsg')?.textContent || 'Déplacement impossible.';
  }
}

function renderShelfProductList(aisle, side, section, shelf, positions) {
  const products = productsAtShelf(String(aisle), side, String(section), String(shelf))
    .slice().sort((a, b) => Number(a.position) - Number(b.position));
  const filled = products.length;
  const total = Number(positions) || 0;
  // "Scanner ici" makes EVERY tablette directly scannable — côté sections,
  // accroches, façades and présentoirs all render their products through here.
  const scanBtn = `<button class="btn btn-outline btn-inline" style="font-size:11px;padding:3px 9px;margin:0 0 5px;width:100%;color:#16a34a;border-color:#16a34a" onclick="startScanAt('${jsq(aisle)}','${jsq(side)}','${jsq(section)}','${jsq(shelf)}')">▶ Scanner ici</button>`;
  if (!total && !filled) return `<div class="plan-product-list">${scanBtn}</div>`;

  // Mode libre (positions = 0): show all scanned products without fixed slots
  if (!total) {
    return `<div class="plan-product-list">${scanBtn}
      <div style="font-size:10px;color:#8b5cf6;font-weight:600;padding:3px 0 4px">📦 ${filled} produit${filled!==1?'s':''} libre${filled!==1?'s':''}</div>
      ${products.map(p => `<div class="plan-product-item">
        <div class="plan-product-row1">
          ${renderPlanSelectionCheckbox('product', aisle, side, section, shelf, p.id, `Sélectionner ${p.name}`)}
          <span class="plan-product-name">${esc(p.name)}${p.brand ? ` <span class="plan-product-brand">${esc(p.brand)}</span>` : ''}</span>
          <button class="plan-drag-handle" draggable="true" title="Glisser ou choisir une position exacte" onclick="openMoveProduct(${p.id})"
                  ondragstart="beginPlanSelectionDrag(event,${p.id})" ondragend="endPlanSelectionDrag()"
                  style="margin-left:auto;flex-shrink:0;border:1px solid #cbd5e1;color:#334155;background:#f8fafc;border-radius:5px;padding:2px 7px;cursor:grab;font-size:11px">⇄</button>
          <button title="Retirer ce produit" onclick="deleteProduct(${p.id})" style="flex-shrink:0;border:1px solid #f1b8c2;color:#c8102e;background:#fff;border-radius:5px;padding:2px 7px;cursor:pointer;font-size:11px">✕</button>
        </div>
        <div class="plan-product-row2">${p.barcode ? esc(p.barcode) : '—'}${p.product_code ? ` · code ${esc(p.product_code)}` : ''}</div>
      </div>`).join('')}
      ${!filled ? `<div class="plan-product-item"><span class="plan-slot-empty">Scannez les produits via le Scan tab</span></div>` : ''}
    </div>`;
  }
  const byPos = {};
  products.forEach(p => { byPos[Number(p.position)] = p; });
  const ae = s => jsq(s);
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
          ${renderPlanSelectionCheckbox('product', aisle, side, section, shelf, p.id, `Sélectionner ${p.name}`)}
          <span class="plan-product-name">${esc(p.name)}${p.brand ? ` <span class="plan-product-brand">${esc(p.brand)}</span>` : ''}</span>
          <span style="display:flex;gap:4px;margin-left:auto;flex-shrink:0">
            <button title="Échanger avec la position précédente" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:14px;line-height:1;${canUp?'':'opacity:.25;cursor:default'}" onclick="swapPositions(${swapArgs},${pos},${pos-1})" ${canUp?'':'disabled'}>↑</button>
            <button title="Échanger avec la position suivante" style="background:#f8fafc;border:1px solid #cbd5e1;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:14px;line-height:1;${canDown?'':'opacity:.25;cursor:default'}" onclick="swapPositions(${swapArgs},${pos},${pos+1})" ${canDown?'':'disabled'}>↓</button>
            <button class="plan-drag-handle" draggable="true" title="Glisser ou choisir une position exacte"
                    style="background:#f8fafc;border:1px solid #cbd5e1;color:#334155;border-radius:6px;cursor:grab;padding:4px 9px;font-size:13px;line-height:1"
                    onclick="openMoveProduct(${p.id})" ondragstart="beginPlanSelectionDrag(event,${p.id})" ondragend="endPlanSelectionDrag()">⇄</button>
            <button title="Retirer ce produit" style="background:#fff;border:1px solid #f1b8c2;color:#c8102e;border-radius:6px;cursor:pointer;padding:4px 9px;font-size:13px;line-height:1" onclick="deleteProduct(${p.id})">✕</button>
          </span>
        </div>
        <div class="plan-product-row2">${p.barcode ? esc(p.barcode) : '—'}${p.product_code ? ` · code ${esc(p.product_code)}` : ''}</div>
      </div>`;
    } else {
      html += `<div class="plan-product-item">
        <div class="plan-product-row1">
          <span class="plan-pos-badge">${pos}</span>
          <span class="plan-slot-empty" title="Aucun produit n'est enregistré à cette position">aucun produit importé</span>
        </div>
      </div>`;
    }
  }
  products.filter(p => Number(p.position) < 1 || Number(p.position) > total).forEach(p => {
    html += `<div class="plan-product-item" style="background:#fff5f5;border-radius:4px;padding:5px 4px">
      <div class="plan-product-row1">
        <span class="plan-pos-badge" style="color:#c8102e">${esc(String(p.position))}</span>
        ${renderPlanSelectionCheckbox('product', aisle, side, section, shelf, p.id, `Sélectionner ${p.name}`)}
        <span class="plan-product-name" style="color:#c8102e">${esc(p.name)} <span class="plan-product-brand">hors limite</span></span>
        <button class="plan-drag-handle" draggable="true" title="Glisser ou deplacer ce produit"
                onclick="openMoveProduct(${p.id})" ondragstart="beginPlanSelectionDrag(event,${p.id})" ondragend="endPlanSelectionDrag()">⇄</button>
        <button title="Retirer ce produit" onclick="deleteProduct(${p.id})">✕</button>
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
  return `<div class="plan-shelf-card" id="shelfcard-${esc(aisle)}|${esc(side)}|${sectionIndex}|${shelfIndex}"
    ${planDropTargetAttrs(aisle, side, sectionIndex + 1, shelfIndex + 1, 'shelf')} style="${cardBg}">
    <div class="shelf-header" style="gap:4px">
      ${renderPlanStructureHandle('shelf', {aisle, side, sectionIndex, index: shelfIndex}, `Déplacer la tablette ${shelfIndex + 1}`)}
      ${renderPlanSelectionCheckbox('shelf', aisle, side, sectionIndex + 1, shelfIndex + 1, '', `Sélectionner la tablette ${shelfIndex + 1}`)}
      <span class="shelf-title">${shelfTitle}</span>
      ${renderPlanDropButton(aisle, side, sectionIndex + 1, shelfIndex + 1, 'shelf')}
      ${isLibre
        ? `<span style="font-size:10px;color:#8b5cf6;font-weight:700">LIBRE · ${shelfFilled} prod.</span>
           <button title="Définir un nombre fixe de positions" style="background:none;border:1px solid #a78bfa;border-radius:4px;color:#8b5cf6;cursor:pointer;font-size:10px;padding:1px 5px"
                   onclick="setShelfPositionCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},prompt('Nombre de positions fixes ?','8')||0)">→ Positions fixes</button>`
        : `<button title="Retirer une position" style="background:none;border:1px solid #e2e8f0;border-radius:5px;cursor:pointer;font-size:14px;padding:1px 8px;line-height:1.3;${positions<=1?'opacity:.3;cursor:default':''}" onclick="setShelfPositionCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},${positions-1})" ${positions<=1?'disabled':''}>➖</button>
           <input type="number" min="1" value="${positions}" title="Positions"
                 style="width:46px;padding:2px 4px;border:1px solid #e2e8f0;border-radius:5px;font-size:12px;text-align:center"
                 onchange="setShelfPositionCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},this.value)"/>
           <button title="Ajouter une position" style="background:none;border:1px solid #e2e8f0;border-radius:5px;cursor:pointer;font-size:14px;padding:1px 8px;line-height:1.3" onclick="setShelfPositionCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},${positions+1})">➕</button>
           <span style="font-size:11px;color:#64748b">${shelfFilled} prod.</span>
           <button title="Passer en mode libre (cosmétiques, presentoirs...)" style="background:none;border:1px solid #e2e8f0;border-radius:4px;color:#8b5cf6;cursor:pointer;font-size:10px;padding:1px 5px"
                   onclick="setShelfPositionCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},0)">📦 Libre</button>`
      }
      <button type="button" class="plan-delete-action" onclick="removeShelf('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},this)" style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5" title="Supprimer cette tablette">✕ Tablette</button>
    </div>
    <div style="display:flex;gap:6px;padding:5px 0 4px;border-top:1px solid rgba(0,0,0,.06);margin-top:4px;flex-wrap:wrap">
      <button class="btn btn-outline btn-inline" style="font-size:12px;flex:1" onclick="moveShelf('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},-1)">↑ Monter</button>
      <button class="btn btn-outline btn-inline" style="font-size:12px;flex:1" onclick="moveShelf('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},1)">↓ Descendre</button>
      ${renderPlanScopeDeleteButton('shelf', aisle, side, sectionIndex + 1, shelfIndex + 1, shelfFilled)}
    </div>
    <details class="struct-details">
      <summary class="struct-toggle" style="font-size:11px">⚙ Nom / étiquette</summary>
      <div class="field" style="margin-top:6px">
        <input type="text" value="${esc(shelfLabel)}" placeholder="Laisser vide = Tablette ${shelfIndex + 1}"
               oninput="setShelfLabel('${jsq(aisle)}','${jsq(side)}',${sectionIndex},${shelfIndex},this.value)"/>
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
  const attrs = `data-plan-kind="section" data-aisle="${esc(aisle)}" data-side="${esc(side)}" data-section-index="${sectionIndex}" data-node-id="${sectionNodeId}"`;
  if (!isPlanNodeOpen(sectionNodeId)) {
    return `<details class="tree-node plan-section" ${attrs} ${planDropTargetAttrs(aisle, side, sectionIndex + 1, '', 'section')}${detailsOpenAttr(sectionNodeId)}>
    <summary>
      ${renderPlanStructureHandle('section', {aisle, side, index: sectionIndex}, `Déplacer la section ${sectionIndex + 1}`)}
      ${renderPlanSelectionCheckbox('section', aisle, side, sectionIndex + 1, '', '', `Sélectionner la section ${sectionIndex + 1}`)}
      <span>Section ${sectionIndex + 1}</span>
      <span class="tree-meta">${sectionProducts} prod. · ${section.shelves.length} T${sectionHome ? ` · <span style="color:#c8102e">★${sectionHome}</span>` : ''}</span>
      ${renderPlanDropButton(aisle, side, sectionIndex + 1, '', 'section')}
      ${renderPlanStructureDropZone('shelf', {aisle, side, sectionIndex, index: section.shelves.length}, `Fin de la section ${sectionIndex + 1}`)}
    </summary>
    <div class="tree-body plan-lazy-body" data-lazy-empty="1"></div>
  </details>`;
  }
  return `<details class="tree-node plan-section" ${attrs} ${planDropTargetAttrs(aisle, side, sectionIndex + 1, '', 'section')}${detailsOpenAttr(sectionNodeId)}>
    <summary>
      ${renderPlanStructureHandle('section', {aisle, side, index: sectionIndex}, `Déplacer la section ${sectionIndex + 1}`)}
      ${renderPlanSelectionCheckbox('section', aisle, side, sectionIndex + 1, '', '', `Sélectionner la section ${sectionIndex + 1}`)}
      <span>Section ${sectionIndex + 1}</span>
      <span class="tree-meta">${sectionProducts} prod. · ${section.shelves.length} T${sectionHome ? ` · <span style="color:#c8102e">★${sectionHome}</span>` : ''}</span>
      ${renderPlanDropButton(aisle, side, sectionIndex + 1, '', 'section')}
      ${renderPlanStructureDropZone('shelf', {aisle, side, sectionIndex, index: section.shelves.length}, `Fin de la section ${sectionIndex + 1}`)}
    </summary>
    <div class="tree-body">
      <div style="display:flex;gap:6px;flex-wrap:wrap;margin:6px 0 6px">
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addShelf('${jsq(aisle)}','${jsq(side)}',${sectionIndex})">➕ Tablette</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addAccrocheToSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex})">📎 Accroche</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="startScanFromSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex})">▶ Scanner ici</button>
      </div>
      <div style="display:flex;gap:6px;margin-bottom:10px;align-items:center;flex-wrap:wrap">
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:6px 14px" onclick="moveSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex},-1)">↑ Monter</button>
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:6px 14px" onclick="moveSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex},1)">↓ Descendre</button>
        <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="openMoveSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex})">⇄ Autre allée</button>
        ${renderPlanScopeDeleteButton('section', aisle, side, sectionIndex + 1, '', sectionProducts)}
        <button type="button" class="btn btn-outline btn-inline plan-delete-action" style="font-size:12px;color:#c8102e;border-color:#f1b8c2;margin-left:auto" onclick="removeSection('${jsq(aisle)}','${jsq(side)}',${sectionIndex},this)">✕ Supprimer section</button>
      </div>
      ${section.shelves.length ? '' : `<div class="small" style="padding:4px 0;color:#94a3b8">Aucune tablette — cliquez ➕ Tablette ci-dessus.</div>`}
      <label class="small" style="display:flex;align-items:center;gap:6px;margin:4px 0 8px;font-weight:700;color:#475569">
        Nombre de tablettes
        <input type="number" min="0" max="60" value="${section.shelves.length}" style="width:58px;padding:5px;text-align:center"
               onchange="setSectionShelfCount('${jsq(aisle)}','${jsq(side)}',${sectionIndex},this.value)"/>
      </label>
      <div class="plan-shelf-grid">
        ${section.shelves.map((positions, shelfIndex) =>
          renderPlanStructureDropZone(
            'shelf', {aisle, side, sectionIndex, index: shelfIndex},
            `Avant la tablette ${shelfIndex + 1}`
          ) + renderShelfCard(
            aisle, side, sectionIndex, shelfIndex, positions,
            (section.labels || [])[shelfIndex] || ''
          )
        ).join('')}
        ${renderPlanStructureDropZone(
          'shelf', {aisle, side, sectionIndex, index: section.shelves.length},
          `Fin de la section ${sectionIndex + 1}`
        )}
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
  const attrs = `data-plan-kind="side" data-aisle="${esc(aisle)}" data-side="${esc(side)}" data-node-id="${sideNodeId}"`;
  if (!isPlanNodeOpen(sideNodeId)) {
    return `<details class="tree-node plan-side" ${attrs}${detailsOpenAttr(sideNodeId)}>
    <summary>
      ${renderPlanSelectionCheckbox('side', aisle, side, '1', '', '', `Sélectionner ${sideLabel}`)}
      <span>${sideLabel}</span>
      <span class="tree-meta">${sections.length} section${sections.length !== 1 ? 's' : ''} · ${sideCount} produit${sideCount !== 1 ? 's' : ''}</span>
      ${renderPlanStructureDropZone('section', {aisle, side, index: sections.length}, `Fin de ${sideLabel}`)}
    </summary>
    <div class="tree-body plan-lazy-body" data-lazy-empty="1"></div>
  </details>`;
  }
  return `<details class="tree-node plan-side" ${attrs}${detailsOpenAttr(sideNodeId)}>
    <summary>
      ${renderPlanSelectionCheckbox('side', aisle, side, '1', '', '', `Sélectionner ${sideLabel}`)}
      <span>${sideLabel}</span>
      <span class="tree-meta">${sections.length} section${sections.length !== 1 ? 's' : ''} · ${sideCount} produit${sideCount !== 1 ? 's' : ''}</span>
      ${renderPlanStructureDropZone('section', {aisle, side, index: sections.length}, `Fin de ${sideLabel}`)}
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
          <button class="btn btn-outline btn-inline" style="margin-top:8px" onclick="applySideTemplate('${jsq(aisle)}','${jsq(side)}')">Appliquer modèle uniforme a ${sideLabel}</button>
          <div class="field" style="margin-top:8px">
            <label class="label" for="sectionCount-${aisle}-${side}">Nombre de sections</label>
            <input id="sectionCount-${esc(aisle)}-${esc(side)}" type="number" min="0" value="${sections.length}" onchange="setSideSectionCount('${jsq(aisle)}','${jsq(side)}', this.value)"/>
          </div>
        </div>
      </details>
      ${sections.length ? '' : `<div class="small" style="padding:8px 0">Aucune section sur ${sideLabel}.</div>`}
      ${sections.map((section, sectionIndex) =>
        renderPlanStructureDropZone(
          'section', {aisle, side, index: sectionIndex},
          `Avant la section ${sectionIndex + 1}`
        ) + renderSection(aisle, side, sectionIndex, section)
      ).join('')}
      ${renderPlanStructureDropZone(
        'section', {aisle, side, index: sections.length}, `Fin de ${sideLabel}`
      )}
      <button class="btn btn-outline btn-inline" style="margin-top:8px;font-size:12px;width:100%" onclick="addSection('${jsq(aisle)}','${jsq(side)}')">➕ Ajouter une section</button>
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
  if (_skipPlanCaptureOnce) _skipPlanCaptureOnce = false;
  else captureOpenPlanNodesFromDom();
  const msgDiv = document.getElementById('addMsg');
  const div = document.getElementById('mapContent');
  const counts = planSummaryCounts();   // memoized per cache version (no rescan on layout-only edits)
  div.innerHTML = renderPlanStructureStatus() + renderPlanBulkToolbar() + (mapLayouts.length
    ? `<div class="tool-row" style="margin-bottom:12px">
        <button class="btn btn-outline btn-inline" onclick="setAllPlanTrees(true)">Ouvrir tout</button>
        <button class="btn btn-outline btn-inline" onclick="setAllPlanTrees(false)">Fermer tout</button>
      </div>` + mapLayouts.map((layout, aisleIndex) => {
        syncLayoutRecord(layout);          // normalizes layout.config + refreshes metrics/count (count is memoized)
        const config = layout.config;       // already normalized above — skip a 2nd deep rebuild
        const slotCount = countSlotsFromConfig(config);   // no per-slot object allocation
        const aisleNodeId = `planAisle-${layout.aisle}`;
        const homeCount = counts.home.get(String(layout.aisle)) || 0;
        const dirty = dirtyLayoutAisles.has(String(layout.aisle));
        const aisleDrop = renderPlanStructureDropZone(
          'aisle', {aisle: layout.aisle, index: aisleIndex},
          `Avant l allée ${layout.aisle}`
        );
        if (!isPlanNodeOpen(aisleNodeId)) {
          return `${aisleDrop}<details class="tree-node plan-aisle-node" id="${aisleNodeId}" data-plan-kind="aisle" data-aisle="${esc(layout.aisle)}" data-node-id="${aisleNodeId}"${detailsOpenAttr(aisleNodeId)}>
        <summary>
          ${renderPlanStructureHandle('aisle', {aisle: layout.aisle, index: aisleIndex}, `Déplacer l allée ${layout.aisle}`)}
          ${renderPlanSelectionCheckbox('aisle', layout.aisle, '', '1', '', '', `Sélectionner l allée ${layout.aisle}`)}
          <span>Allée ${esc(layout.aisle)}</span>
          <span class="tree-meta">${layout.product_count || 0} produit${Number(layout.product_count || 0) !== 1 ? 's' : ''} · <span id="aisleSlots-${esc(layout.aisle)}">${slotCount}</span> slots${homeCount ? ` · <span style="color:#c8102e">★${homeCount} maison</span>` : ''}${dirty ? ' · <span style="color:#d97706">non sauvegardé</span>' : ''}</span>
        </summary>
        <div class="tree-body plan-lazy-body" data-lazy-empty="1"></div>
      </details>`;
        }
        return `${aisleDrop}<details class="tree-node plan-aisle-node" id="${aisleNodeId}" data-plan-kind="aisle" data-aisle="${esc(layout.aisle)}" data-node-id="${aisleNodeId}"${detailsOpenAttr(aisleNodeId)}>
        <summary>
          ${renderPlanStructureHandle('aisle', {aisle: layout.aisle, index: aisleIndex}, `Déplacer l allée ${layout.aisle}`)}
          ${renderPlanSelectionCheckbox('aisle', layout.aisle, '', '1', '', '', `Sélectionner l allée ${layout.aisle}`)}
          <span>Allée ${esc(layout.aisle)}</span>
          <span class="tree-meta">${layout.product_count || 0} produit${Number(layout.product_count || 0) !== 1 ? 's' : ''} · <span id="aisleSlots-${esc(layout.aisle)}">${slotCount}</span> slots${homeCount ? ` · <span style="color:#c8102e">★${homeCount} maison</span>` : ''}${dirty ? ' · <span style="color:#d97706">non sauvegardé</span>' : ''}</span>
        </summary>
        <div class="tree-body">
        <div class="plan-actions" style="margin-top:8px">
          <span id="aisleSaveState-${esc(layout.aisle)}" class="small" data-state="${dirty ? 'waiting' : 'saved'}" style="color:${dirty ? '#d97706' : '#15803d'}">${dirty ? 'En attente...' : 'Sauvegarde automatique'}</span>
          <button class="btn btn-inline" onclick="saveAisleLayout('${jsq(layout.aisle)}')">Sauver</button>
          <button class="btn btn-outline btn-inline" onclick="applyAisleLayoutToCursor('${jsq(layout.aisle)}')">Utiliser pour scan</button>
          <button class="btn btn-outline btn-inline" onclick="setPlanAisleTrees('${jsq(layout.aisle)}', true)">Tout ouvrir</button>
          <button class="btn btn-outline btn-inline" onclick="setPlanAisleTrees('${jsq(layout.aisle)}', false)">Tout fermer</button>
          ${renderPlanScopeDeleteButton('aisle', layout.aisle, '', '1', '', layout.product_count || 0)}
          <button class="btn btn-outline btn-inline plan-delete-action" style="border-color:#f1b8c2;color:#c8102e" onclick="removeAisleLayout('${jsq(layout.aisle)}')">✕ Supprimer allée</button>
        </div>
        ${layout.modified_by ? `<div class="small" style="margin-top:6px">Modifie par: ${esc(layout.modified_by)}</div>` : ''}
        <div class="plan-sides">
        ${['Gauche','Droite'].map(side => renderSide(layout.aisle, side, config)).join('')}
        </div>
        ${renderFacadesSection(layout.aisle, config)}
        ${renderPresentoirSection(layout.aisle, config)}
        </div>
      </details>`;
      }).join('') + renderPlanStructureDropZone(
        'aisle', {aisle: '', index: mapLayouts.length}, 'Fin du magasin'
      )
    : '<div class="empty">Aucune allée configurée.</div>');
  if (!msgDiv.textContent) msgDiv.innerHTML = '';
  if (typeof window.requestAnimationFrame === 'function') {
    window.requestAnimationFrame(() => {
      syncPlanSelectionUi();
      syncPlanStructureMoveUi();
    });
  } else {
    syncPlanSelectionUi();
    syncPlanStructureMoveUi();
  }
  restorePlanMoveReceipt();
}

function planNodeIdsForAisle(layout) {
  const aisle = String(layout.aisle);
  const config = layout.config || defaultLayoutConfig();
  const ids = [`planAisle-${aisle}`];
  ['Gauche', 'Droite'].forEach(side => {
    ids.push(`planSide-${aisle}-${side}`);
    (config.sides?.[side]?.sections || []).forEach((_section, index) => {
      ids.push(`planSection-${aisle}-${side}-${index}`);
    });
  });
  ids.push(`planFacade-${aisle}-facade_a`, `planFacade-${aisle}-facade_b`);
  (config.presentoirs || []).forEach((pres, pi) => {
    ids.push(`planPres-${aisle}-${pi}`);
    (pres.facades || []).forEach((_facade, fi) => ids.push(`planPres-${aisle}-${pi}-F${fi}`));
  });
  return ids;
}

function setAllPlanTrees(open) {
  openPlanNodes = new Set();
  if (open) {
    mapLayouts.forEach(layout => planNodeIdsForAisle(layout).forEach(id => openPlanNodes.add(id)));
  }
  _skipPlanCaptureOnce = true;
  renderMapEditor();
}

function setPlanAisleTrees(aisle, open) {
  const layout = mapLayouts.find(item => String(item.aisle) === String(aisle));
  if (!layout) return;
  planNodeIdsForAisle(layout).forEach(id => {
    openPlanNodes.delete('--closed--' + id);
    if (open) openPlanNodes.add(id);
    else openPlanNodes.delete(id);
  });
  _skipPlanCaptureOnce = true;
  renderMapEditor();
}

async function loadMapEditor(forceServer=false) {
  const restoredSnapshot = restorePlanSnapshot();
  if (mapLayouts.length) refreshPlanUi();   // instant paint from boot memory or local snapshot
  else showPlanLoading();
  await Promise.allSettled([
    refreshProductsCache(forceServer || restoredSnapshot),
    refreshLayoutsCache(forceServer || restoredSnapshot)
  ]);
  savePlanSnapshot();
  refreshPlanUi();
  loadPlanogramHistory();
  loadReferenceCount();
  pollRegulatorySync();
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

function setSectionShelfCount(aisle, side, sectionIndex, rawValue) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side]?.sections?.[sectionIndex];
  if (!section) return;
  const count = Math.min(60, Math.max(0, parseInt(rawValue) || 0));
  const currentCount = section.shelves.length;
  if (count === currentCount) return;

  if (count < currentCount) {
    const nextConfig = readAisleLayoutConfig(aisle);
    nextConfig.sides[side].sections[sectionIndex].shelves.length = count;
    nextConfig.sides[side].sections[sectionIndex].labels.length = count;
    if (!confirmLayoutReduction(aisle, nextConfig, `Reduire la section ${sectionIndex + 1} a ${count} tablettes`)) {
      rerenderSection(aisle, side, sectionIndex);
      return;
    }
  }

  const fallback = section.shelves[section.shelves.length - 1] ?? 8;
  if (!Array.isArray(section.labels)) section.labels = [];
  while (section.shelves.length < count) {
    section.shelves.push(fallback);
    section.labels.push('');
  }
  section.shelves.length = count;
  section.labels.length = count;
  syncLayoutRecord(layout);
  markLayoutDirty(aisle);
  rerenderSection(aisle, side, sectionIndex);
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
      sort_order: Number(data.sort_order) || (mapLayouts.length + 1),
      modified_by: loadEditorSession().username || '',
      modified_at: data.modified_at || nowIsoWithoutMs(), product_count: 0,
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
    savePlanSnapshot();
    refreshPlanUi();
  }
}

async function saveAisleLayout(aisle) {
  if (!requireEditorSession('modifier le plan du magasin')) return;
  const key = String(aisle);
  window.clearTimeout(_layoutAutoSaveTimers.get(key));
  _layoutAutoSaveTimers.delete(key);
  if (_layoutAutoSaveInFlight.has(key)) {
    setLayoutSaveState(key, 'waiting');
    scheduleLayoutAutoSave(key, 150);
    return;
  }
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const revision = _layoutEditRevisions.get(key) || 0;
  syncLayoutRecord(layout);
  const config = readAisleLayoutConfig(aisle);
  const data = await apiUpdateLayoutAisle(aisle, {
    config,
    enabled: true,
    expected_modified_at: layout.modified_at || ''
  });
  const msgDiv = document.getElementById('addMsg');
  msgDiv.className = data.success ? 'msg success' : 'msg error';
  msgDiv.textContent = data.success
    ? `Allée ${aisle} sauvee.${Number(data.removed_products || 0) ? ` ${data.removed_products} produit(s) supprime(s) car hors structure.` : ''}`
    : (data.error || 'Sauvegarde impossible.');
  if (data.success) {
    layout.modified_at = data.modified_at || layout.modified_at || nowIsoWithoutMs();
    if ((_layoutEditRevisions.get(key) || 0) === revision) {
      clearLayoutDirty(aisle);
      setLayoutSaveState(aisle, 'saved');
    } else {
      setLayoutSaveState(key, 'waiting');
      scheduleLayoutAutoSave(key, 150);
    }
    await refreshProductsCache(true);
    layout.modified_by = loadEditorSession().username || layout.modified_by || '';
    syncLayoutRecord(layout);
    lastLayoutsRefreshAt = Date.now();
    savePlanSnapshot();
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
  try {
    data = await apiDeleteLayoutAisle(aisle, {
      expected_modified_at: layout?.modified_at || ''
    });
  }
  catch (e) { data = {success: false, error: 'Suppression impossible pour le moment.'}; }
  const msgDiv = document.getElementById('addMsg');
  const success = Boolean(data && data.success);
  msgDiv.className = success ? 'msg success' : 'msg error';
  msgDiv.textContent = success
    ? (data.message || `Allée ${aisle} retirée.`)
    : ((data && data.error) || `Impossible de supprimer l’allée ${aisle}.`);
  if (success) {
    const removedIds = new Set(allProductsCache
      .filter(product => String(product.aisle) === String(aisle))
      .map(product => Number(product.id)));
    mapLayouts = mapLayouts.filter(item => String(item.aisle) !== String(aisle));
    allProductsCache = allProductsCache.filter(
      product => String(product.aisle) !== String(aisle)
    );
    for (const id of removedIds) planSelectedProductIds.delete(id);
    if (typeof invalidateProductSearchIndexes === 'function') invalidateProductSearchIndexes();
    lastProductsRefreshAt = Date.now();
    clearLayoutDirty(aisle);
    lastLayoutsRefreshAt = Date.now();
    planStartDraft = getCursorSelection();
    savePlanSnapshot();
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
  planSelectedProductIds.delete(Number(id));
  if (!planSelectedProductIds.size) planMoveMode = false;
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
    const res = await secureFetch('/api/export');
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
    savePlanSnapshot();
    renderMapEditor();
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de l import.'; }
  }
}

async function resetDatabase(wipeLayouts) {
  const msg = document.getElementById('exportImportMsg');
  const what = wipeLayouts ? 'TOUS les produits ET le plan du magasin' : 'tous les produits';
  const confirmation = wipeLayouts ? 'SUPPRIMER LE PLAN' : 'SUPPRIMER LES PRODUITS';
  if (!confirm(`Effacer ${what}? Cette action est irreversible. Faites une sauvegarde d'abord si necessaire.`)) return;
  const typed = prompt(`Confirmation finale : ecrivez exactement ${confirmation}`);
  if (typed !== confirmation) {
    if (typed !== null && msg) {
      msg.className = 'msg error';
      msg.textContent = 'Suppression annulee : la phrase de confirmation ne correspond pas.';
    }
    return;
  }
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Suppression en cours...'; }
  try {
    const {res, data} = await apiFetch('/api/reset', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', ...getEditorHeaders()},
      body: JSON.stringify({wipe_layouts: wipeLayouts, confirmation})
    });
    if (!res.ok || !data.success) throw new Error(data.error || 'Erreur');
    if (msg) {
      msg.className = 'msg success';
      msg.textContent = `Base nettoyee: ${data.deleted_products} produit(s) supprime(s)${wipeLayouts ? `, ${data.deleted_layouts} allée(s) supprimée(s)` : ''}.`;
    }
    await refreshProductsCache(true);
    if (wipeLayouts) { await refreshLayoutsCache(true); renderMapEditor(); }
    savePlanSnapshot();
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de la suppression.'; }
  }
}

// ── Direct section / shelf / position editing ─────────────────────────────────
const _layoutRemovalInFlight = new Set();

function copyLayoutConfig(config) {
  return JSON.parse(JSON.stringify(config));
}

function setPlanDeleteBusy(button, busy) {
  if (!button) return;
  if (busy) {
    button.dataset.deleteLabel = button.textContent;
    button.disabled = true;
    button.setAttribute('aria-busy', 'true');
    button.textContent = 'Suppression...';
  } else {
    button.disabled = false;
    button.removeAttribute('aria-busy');
    button.textContent = button.dataset.deleteLabel || 'Supprimer';
  }
}

function showPlanActionMessage(message, type='error') {
  const target = document.getElementById('addMsg');
  if (!target) return;
  target.className = `msg ${type}`;
  target.textContent = message;
}

function confirmPlanRemoval(subject, productCount) {
  const suffix = productCount
    ? `\n\n${productCount} produit(s) dans cet élément seront aussi retirés du plan.`
    : '';
  return confirm(`${subject} ?${suffix}`);
}

async function waitForLayoutSave(aisle) {
  const key = String(aisle);
  for (let attempt = 0; attempt < 80 && _layoutAutoSaveInFlight.has(key); attempt += 1) {
    await new Promise(resolve => window.setTimeout(resolve, 50));
  }
  return !_layoutAutoSaveInFlight.has(key);
}

function applyLocalProductRemoval(aisle, side, field, removedNumber, sectionNumber=null) {
  const removed = Number(removedNumber);
  const kept = [];
  for (const product of allProductsCache) {
    const sameLocation = String(product.aisle) === String(aisle) && product.side === side &&
      (field !== 'shelf' || sectionNumber === null || String(product.section) === String(sectionNumber));
    if (!sameLocation) {
      kept.push(product);
      continue;
    }
    const value = Number(product[field]);
    if (value === removed) continue;
    if (value > removed) product[field] = String(value - 1);
    kept.push(product);
  }
  allProductsCache = kept;
  lastProductsRefreshAt = Date.now();
}

async function commitLayoutRemoval({aisle, endpoint, payload, nextConfig, button, productRemoval, successLabel}) {
  const key = String(aisle);
  if (_layoutRemovalInFlight.has(key)) return false;
  _layoutRemovalInFlight.add(key);
  setPlanDeleteBusy(button, true);
  window.clearTimeout(_layoutAutoSaveTimers.get(key));
  _layoutAutoSaveTimers.delete(key);
  if (!await waitForLayoutSave(key)) {
    _layoutRemovalInFlight.delete(key);
    setPlanDeleteBusy(button, false);
    showPlanActionMessage('Une sauvegarde est encore en cours. Réessayez dans quelques secondes.');
    return false;
  }

  if (dirtyLayoutAisles.has(key)) await autoSaveAisleLayout(key);
  if (dirtyLayoutAisles.has(key)) {
    _layoutRemovalInFlight.delete(key);
    setPlanDeleteBusy(button, false);
    showPlanActionMessage('Le plan doit être sauvegardé avant cette suppression. Rechargez-le si une autre personne vient de le modifier.');
    return false;
  }
  const currentLayout = getMutableLayout(aisle);
  if (!currentLayout) {
    _layoutRemovalInFlight.delete(key);
    setPlanDeleteBusy(button, false);
    showPlanActionMessage('Le plan doit être rechargé avant cette suppression.');
    return false;
  }

  const data = await apiRemoveLayoutPart(aisle, endpoint, {
    ...payload,
    expected_modified_at: currentLayout.modified_at || ''
  });
  if (!data.success) {
    _layoutRemovalInFlight.delete(key);
    setPlanDeleteBusy(button, false);
    showPlanActionMessage(data.error || 'Suppression impossible.');
    if (dirtyLayoutAisles.has(key)) scheduleLayoutAutoSave(key);
    return false;
  }

  const layout = getMutableLayout(aisle);
  if (layout) {
    layout.config = normalizeLayoutConfig(data.config || nextConfig);
    layout.modified_at = data.modified_at || layout.modified_at || nowIsoWithoutMs();
    syncLayoutRecord(layout);
  }
  productRemoval();
  _layoutEditRevisions.set(key, (_layoutEditRevisions.get(key) || 0) + 1);
  clearLayoutDirty(key);
  setLayoutSaveState(key, 'saved');
  lastLayoutsRefreshAt = Date.now();
  savePlanSnapshot();
  refreshPlanUi();
  const removedCount = Number(data.removed_products || 0);
  showPlanActionMessage(
    `${successLabel}${removedCount ? ` ${removedCount} produit(s) retiré(s) du plan.` : ''}`,
    'success'
  );
  _layoutRemovalInFlight.delete(key);

  void refreshProductsCache(true).then(() => {
    savePlanSnapshot();
    refreshPlanUi();
  });
  return true;
}

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

async function removeSection(aisle, side, sectionIndex, button=null) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const baseConfig = copyLayoutConfig(layout.config);
  const nextConfig = copyLayoutConfig(layout.config);
  const sections = nextConfig.sides[side]?.sections;
  if (!sections || sectionIndex < 0 || sectionIndex >= sections.length) return;
  sections.splice(sectionIndex, 1);
  const productCount = allProductsCache.filter(product =>
    String(product.aisle) === String(aisle) && product.side === side &&
    String(product.section) === String(sectionIndex + 1)
  ).length;
  if (!confirmPlanRemoval(`Supprimer la section ${sectionIndex + 1}`, productCount)) return;
  return commitLayoutRemoval({
    aisle,
    endpoint: 'remove-section',
    payload: {side, section: String(sectionIndex + 1), config: baseConfig},
    nextConfig,
    button,
    productRemoval: () => applyLocalProductRemoval(aisle, side, 'section', sectionIndex + 1),
    successLabel: `Section ${sectionIndex + 1} supprimée.`
  });
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

async function removeShelf(aisle, side, sectionIndex, shelfIndex, button=null) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const baseConfig = copyLayoutConfig(layout.config);
  const nextConfig = copyLayoutConfig(layout.config);
  const section = nextConfig.sides[side]?.sections?.[sectionIndex];
  if (!section || shelfIndex < 0 || shelfIndex >= section.shelves.length) return;
  section.shelves.splice(shelfIndex, 1);
  if (section.labels) section.labels.splice(shelfIndex, 1);
  const productCount = allProductsCache.filter(product =>
    String(product.aisle) === String(aisle) && product.side === side &&
    String(product.section) === String(sectionIndex + 1) &&
    String(product.shelf) === String(shelfIndex + 1)
  ).length;
  if (!confirmPlanRemoval(`Supprimer la tablette ${shelfIndex + 1}`, productCount)) return;
  return commitLayoutRemoval({
    aisle,
    endpoint: 'remove-shelf',
    payload: {
      side,
      section: String(sectionIndex + 1),
      shelf: String(shelfIndex + 1),
      config: baseConfig
    },
    nextConfig,
    button,
    productRemoval: () => applyLocalProductRemoval(
      aisle, side, 'shelf', shelfIndex + 1, sectionIndex + 1
    ),
    successLabel: `Tablette ${shelfIndex + 1} supprimée.`
  });
}

// Delete a tablette of a fixture side (Façade A/B or a présentoir façade): the
// server removes its products and renumbers the shelves above (no section scoping
// on fixture sides), then the config entry is removed and saved — same contract
// as removeShelf for the aisle sides.
async function _removeFixtureShelf(aisle, sideName, fixtureSelector, shelfIndex, button=null) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const baseConfig = copyLayoutConfig(layout.config);
  const nextConfig = copyLayoutConfig(layout.config);
  const fixture = fixtureSelector(nextConfig);
  if (!fixture || shelfIndex < 0 || shelfIndex >= fixture.shelves.length) return;
  fixture.shelves.splice(shelfIndex, 1);
  if (fixture.labels) fixture.labels.splice(shelfIndex, 1);
  const productCount = allProductsCache.filter(product =>
    String(product.aisle) === String(aisle) && product.side === sideName &&
    String(product.shelf) === String(shelfIndex + 1)
  ).length;
  if (!confirmPlanRemoval(`Supprimer la tablette ${shelfIndex + 1}`, productCount)) return;
  return commitLayoutRemoval({
    aisle,
    endpoint: 'remove-shelf',
    payload: {side: sideName, shelf: String(shelfIndex + 1), config: baseConfig},
    nextConfig,
    button,
    productRemoval: () => applyLocalProductRemoval(aisle, sideName, 'shelf', shelfIndex + 1),
    successLabel: `Tablette ${shelfIndex + 1} supprimée.`
  });
}

async function removeFacadeShelf(aisle, facadeKey, shelfIndex, button=null) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  if (!layout.config[facadeKey]) return;
  const sideName = facadeKey === 'facade_a' ? 'Façade A' : 'Façade B';
  return _removeFixtureShelf(
    aisle, sideName, config => _fixFixture(config[facadeKey]), shelfIndex, button
  );
}

async function removePresentoirShelf(aisle, presIndex, facadeIndex, shelfIndex, button=null) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const pres = layout.config.presentoirs?.[presIndex];
  const facade = pres?.facades?.[facadeIndex];
  if (!pres || !facade) return;
  return _removeFixtureShelf(
    aisle,
    `${pres.name} - ${facade.name}`,
    config => _fixFixture(config.presentoirs?.[presIndex]?.facades?.[facadeIndex]),
    shelfIndex,
    button
  );
}

async function _swapCall(aisle, endpoint, body) {
  const key = String(aisle);
  if (dirtyLayoutAisles.has(key)) await autoSaveAisleLayout(key);
  if (dirtyLayoutAisles.has(key)) return false;
  const layout = getMutableLayout(key);
  if (!layout) return false;
  try {
    const {res, data} = await apiFetch(
      `/api/layout/aisles/${encodeURIComponent(aisle)}/${endpoint}`,
      {
        method:'POST',
        headers:{'Content-Type':'application/json',...getEditorHeaders()},
        body:JSON.stringify({...body, expected_modified_at: layout.modified_at || ''})
      }
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
  const boundary = target > sectionIndex ? target + 1 : target;
  await commitPlanStructureDrop(
    {kind: 'section', aisle: String(aisle), side, index: sectionIndex},
    {kind: 'section', aisle: String(aisle), side, index: boundary},
  );
}

async function moveShelf(aisle, side, sectionIndex, shelfIndex, direction) {
  const layout = getMutableLayout(aisle);
  if (!layout) return;
  const section = layout.config.sides[side].sections[sectionIndex];
  if (!section) return;
  const target = shelfIndex + direction;
  if (target < 0 || target >= section.shelves.length) return;
  const boundary = target > shelfIndex ? target + 1 : target;
  await commitPlanStructureDrop(
    {
      kind: 'shelf', aisle: String(aisle), side,
      sectionIndex, index: shelfIndex,
    },
    {
      kind: 'shelf', aisle: String(aisle), side,
      sectionIndex, index: boundary,
    },
  );
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
    return `<div class="plan-shelf-card" ${planDropTargetAttrs(aisle, sideName, 1, shi + 1, 'shelf')} style="${bg}">
      <div class="shelf-header" style="gap:4px">
        ${renderPlanSelectionCheckbox('shelf', aisle, sideName, 1, shi + 1, '', `Sélectionner la tablette ${shi + 1}`)}
        <span class="shelf-title">${title}</span>
        ${renderPlanDropButton(aisle, sideName, 1, shi + 1, 'shelf')}
        <input type="number" min="1" value="${positions}" title="Positions"
               style="width:46px;padding:2px 4px;border:1px solid #e2e8f0;border-radius:5px;font-size:12px;text-align:center"
               onchange="setFacadeShelfPositions('${jsq(aisle)}','${jsq(fk)}',${shi},this.value)"/>
        <span style="font-size:10px;color:#94a3b8">pos</span>
        <span style="font-size:11px;color:#64748b">${filled} prod.</span>
        <button type="button" class="plan-delete-action" onclick="removeFacadeShelf('${jsq(aisle)}','${jsq(fk)}',${shi},this)"
                style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5"
                title="Supprimer cette tablette">✕ Suppr.</button>
        ${renderPlanScopeDeleteButton('shelf', aisle, sideName, 1, shi + 1, filled)}
      </div>
      <details class="struct-details">
        <summary class="struct-toggle" style="font-size:11px">⚙ Nom / étiquette</summary>
        <div class="field" style="margin-top:6px">
          <input type="text" value="${esc(sl)}" placeholder="Laisser vide = Tablette ${shi + 1}"
                 oninput="setFacadeShelfLabel('${jsq(aisle)}','${jsq(fk)}',${shi},this.value)"/>
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
        ${renderPlanSelectionCheckbox('side', aisle, sideName, '1', '', '', `Sélectionner ${label}`)}
        <span>🔲 ${label}</span>
        <span class="tree-meta">${shelves.length} tablette${shelves.length!==1?'s':''} · ${prods.length} produit${prods.length!==1?'s':''}</span>
      </summary>
      <div class="tree-body">
        <div style="display:flex;gap:6px;margin:6px 0 8px">
          <button class="btn btn-outline btn-inline" style="font-size:12px"
                  onclick="setFacadeShelfCount('${jsq(aisle)}','${jsq(key)}',${shelves.length + 1})">➕ Tablette</button>
          ${renderPlanScopeDeleteButton('side', aisle, sideName, '1', '', prods.length)}
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
               onchange="setPresentoirShelfPositions('${jsq(aisle)}',${pi},${fi},${shi},this.value)"/>
             <span style="font-size:10px;color:#94a3b8">pos · ${filled} prod.</span>`;
        return `<div class="plan-shelf-card" ${planDropTargetAttrs(aisle, sideName, 1, shi + 1, 'shelf')}
          style="${bg}${isLibre?';border-color:#a78bfa;background:#faf5ff':''}">
          <div class="shelf-header" style="gap:4px">
            ${renderPlanSelectionCheckbox('shelf', aisle, sideName, 1, shi + 1, '', `Sélectionner la tablette ${shi + 1}`)}
            <span class="shelf-title">${title}</span>
            ${renderPlanDropButton(aisle, sideName, 1, shi + 1, 'shelf')}
            ${posCtrl}
            <button type="button" class="plan-delete-action" onclick="removePresentoirShelf('${jsq(aisle)}',${pi},${fi},${shi},this)"
                    style="margin-left:auto;background:none;border:1px solid #f1b8c2;border-radius:5px;color:#c8102e;cursor:pointer;font-size:12px;padding:2px 8px;line-height:1.5"
                    title="Supprimer cette tablette">✕ Suppr.</button>
            ${renderPlanScopeDeleteButton('shelf', aisle, sideName, 1, shi + 1, filled)}
          </div>
          ${renderShelfProductList(aisle, sideName, 1, shi + 1, positions)}
        </div>`;
      }).join('');

      return `<details class="tree-node plan-section" data-node-id="${facadeNodeId}"${detailsOpenAttr(facadeNodeId)}>
        <summary>
          ${renderPlanSelectionCheckbox('side', aisle, sideName, '1', '', '', `Sélectionner ${facade.name}`)}
          <span>${esc(facade.name)}</span>
          <span class="tree-meta">${shelves.length} T · ${facadeProds.length} prod.</span>
        </summary>
        <div class="tree-body">
          <div style="display:flex;gap:6px;align-items:center;margin:6px 0 8px">
            <input type="text" value="${esc(facade.name)}" style="flex:1;padding:5px 8px;border:1px solid #e2e8f0;border-radius:6px;font-size:13px"
                   oninput="renamePresentoirFacade('${jsq(aisle)}',${pi},${fi},this.value)" placeholder="Nom de la façade"/>
            <button class="btn btn-outline btn-inline" style="font-size:11px" onclick="setPresentoirShelfCount('${jsq(aisle)}',${pi},${fi},${shelves.length+1})">➕ T</button>
            ${renderPlanScopeDeleteButton('side', aisle, sideName, '1', '', facadeProds.length)}
            <button class="btn btn-outline btn-inline" style="font-size:11px;color:#c8102e;border-color:#f1b8c2" onclick="removePresentoirFacade('${jsq(aisle)}',${pi},${fi})">✕ Façade</button>
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
                 oninput="renamePresentoir('${jsq(aisle)}',${pi},this.value)" placeholder="Nom du présentoir"/>
          <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addPresentoirFacade('${jsq(aisle)}',${pi})">➕ Façade</button>
          <button class="btn btn-outline btn-inline" style="font-size:12px;color:#c8102e;border-color:#f1b8c2" onclick="removePresentoir('${jsq(aisle)}',${pi})">✕ Supprimer</button>
        </div>
        ${facadesHtml}
      </div>
    </details>`;
  }).join('');

  return `<div style="margin-top:12px">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
      <div style="font-weight:700;font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.5px">Présentoirs (couloir)</div>
      <button class="btn btn-outline btn-inline" style="font-size:12px" onclick="addPresentoir('${jsq(aisle)}')">📦 Ajouter présentoir</button>
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
    const res = await secureFetch('/api/import/planogram-catalog', {method:'POST', body: form});
    const data = await res.json();
    if (!res.ok || !data.success) {
      msg.style.color = '#c8102e';
      msg.textContent = data.error || 'Erreur lors de l import du catalogue.';
      return;
    }
    msg.style.color = '#16a34a';
    msg.innerHTML = `<strong>${data.planograms} planogrammes</strong> · ${data.products_seen} produits enregistrés au catalogue · `
      + `${data.enriched_products} code(s)/façade(s) complété(s) · `
      + `${data.metadata_linked_products || 0} description(s)/image(s) reliée(s) au plan.`;
    if (typeof refreshProductsCache === 'function') { try { await refreshProductsCache(true); } catch(_){} }
  } catch (e) {
    msg.style.color = '#c8102e';
    msg.textContent = 'Impossible d importer le catalogue pour le moment.';
  }
}

let catalogEnrichTimer = null;
let catalogEnrichWasRunning = false;
async function startCatalogEnrich() {
  if (!requireEditorSession('enrichir le catalogue')) return;
  catalogEnrichWasRunning = true;
  const msg = document.getElementById('catalogEnrichMsg');
  if (msg) { msg.style.color = '#64748b'; msg.textContent = 'Démarrage de l enrichissement…'; }
  try { await secureFetch('/api/import/catalog-enrich/start', {method:'POST'}); } catch (_) {}
  const stop = document.getElementById('catalogEnrichStop'); if (stop) stop.style.display = '';
  pollCatalogEnrich();
}
async function pollCatalogEnrich() {
  window.clearTimeout(catalogEnrichTimer);
  let s = {};
  try { s = await (await secureFetch('/api/import/catalog-enrich/status')).json(); } catch (_) {}
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
        + `<b>${s.updated || 0}</b> descriptions/images ajoutées · `
        + `${s.linked || 0} produit(s) du plan mis à jour · `
        + `${s.skipped || 0} sans correspondance fiable${resumed}${retry}`;
    }
  }
  const stop = document.getElementById('catalogEnrichStop');
  if (s.running) {
    catalogEnrichWasRunning = true;
    catalogEnrichTimer = window.setTimeout(pollCatalogEnrich, 3000);
  } else {
    if (stop) stop.style.display = 'none';
    if (catalogEnrichWasRunning && typeof refreshProductsCache === 'function') {
      catalogEnrichWasRunning = false;
      try { await refreshProductsCache(true); } catch (_) {}
    }
  }
}
async function stopCatalogEnrich() {
  try { await secureFetch('/api/import/catalog-enrich/stop', {method:'POST'}); } catch (_) {}
  const msg = document.getElementById('catalogEnrichMsg');
  if (msg) msg.textContent = 'Arrêt demandé…';
}

let regulatorySyncTimer = null;
let regulatorySyncWasRunning = false;

const REGULATORY_PHASE_LABELS = {
  prepare: 'Préparation du catalogue',
  read_existing_identifier_labels: 'Lecture des DIN et NPN déjà trouvés',
  dpd_upc_retired: 'Vérification des sources UPC exactes',
  download_packages_api: 'Lecture des emballages Santé Canada',
  download_drugs_api: 'Lecture des médicaments Santé Canada',
  api_fallback_zip: 'Repli vers les fichiers Santé Canada',
  download_packages: 'Téléchargement des emballages Santé Canada',
  download_drugs: 'Téléchargement des médicaments Santé Canada',
  match_exact_upc: 'Correspondance exacte des UPC',
  save_exact_matches: 'Enregistrement des DIN',
  verify_labeled_identifiers: 'Vérification des NPN et DIN-HM',
  inspect_exact_upc_sources: 'Inspection des sources UPC exactes',
  refresh_product_quality: 'Mise à jour des fiches produits',
};

async function startRegulatorySync() {
  if (!requireEditorSession('synchroniser les identifiants')) return;
  regulatorySyncWasRunning = true;
  const msg = document.getElementById('regulatorySyncMsg');
  if (msg) { msg.style.color = '#64748b'; msg.textContent = 'Démarrage de la synchronisation…'; }
  try {
    await secureFetch('/api/product-quality/regulatory/start', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({force: false}),
    });
  } catch (_) {}
  pollRegulatorySync();
}

async function pollRegulatorySync() {
  window.clearTimeout(regulatorySyncTimer);
  let state = {};
  try {
    state = await (await secureFetch('/api/product-quality/regulatory/status')).json();
  } catch (_) {}
  const msg = document.getElementById('regulatorySyncMsg');
  const stop = document.getElementById('regulatorySyncStop');
  const running = Boolean(state.running || ['running', 'starting'].includes(state.status));
  if (stop) stop.style.display = running ? '' : 'none';
  if (msg) {
    const phase = REGULATORY_PHASE_LABELS[state.phase] || 'Synchronisation des identifiants';
    if (running) {
      msg.style.color = '#0369a1';
      msg.textContent = `${phase} · ${Number(state.confirmed_catalog_identifiers || 0)} confirmés · ${Number(state.probable_catalog_identifiers || 0)} utilisables à confirmer`;
    } else if (state.status === 'error') {
      msg.style.color = '#b91c1c';
      msg.textContent = `Synchronisation interrompue : ${String(state.error || 'source temporairement indisponible')}`;
    } else if (state.status === 'partial') {
      msg.style.color = '#b45309';
      msg.textContent = `${Number(state.confirmed_catalog_identifiers || 0)} confirmés · ${Number(state.probable_catalog_identifiers || 0)} utilisables à confirmer · ${Number(state.remaining_online || 0)} produits seront repris automatiquement.`;
    } else if (state.completed_at) {
      msg.style.color = '#16a34a';
      msg.textContent = `${Number(state.confirmed_catalog_identifiers || 0)} identifiants confirmés · ${Number(state.probable_catalog_identifiers || 0)} utilisables à confirmer · ${Number(state.conflicts || 0)} conflit(s).`;
    }
  }
  if (running) {
    regulatorySyncWasRunning = true;
    regulatorySyncTimer = window.setTimeout(pollRegulatorySync, 3000);
  } else if (regulatorySyncWasRunning) {
    regulatorySyncWasRunning = false;
    try { await refreshProductsCache(true); } catch (_) {}
    if (document.getElementById('productQualityPanel')?.open) {
      try { await loadProductQuality(true); } catch (_) {}
    }
  }
}

async function stopRegulatorySync() {
  try { await secureFetch('/api/product-quality/regulatory/stop', {method: 'POST'}); } catch (_) {}
  const msg = document.getElementById('regulatorySyncMsg');
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
    const res  = await secureFetch('/api/import/planogram-parse', {method:'POST', body: form});
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
        const sr = await secureFetch(`/api/import/planogram-parse/status/${up.job}`, {cache: 'no-store'});
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
    aisleSelect.innerHTML = mapLayouts.map(l=>`<option value="${esc(l.aisle)}">${esc(l.aisle)}</option>`).join('');
    const sum = Object.entries(data.tablettes).map(([t,n])=>`T${esc(t)}:${esc(n)}`).join(' | ');
    msg.innerHTML = `<strong style="color:#16a34a">${esc(data.count)} produits</strong> trouvés — ${sum}`;
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
  const out = {
    byIdx: {}, overflow: new Set(), placed: 0, planoShelves: 0,
    availableShelves: 0, availableSections: 0, startSectionShelves: 0,
    overflowShelves: 0, isFixture: false, filteredNonStock: 0
  };
  const slots = [];   // [section_no, shelf_index] in fill order
  const fixture = _planoFixtureForSide(config, side);
  out.isFixture = Boolean(fixture);
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
    // Direction mirrors the server: Côté A travels from Façade B toward Façade A,
    // so only its section numbers decrease. Tablettes keep their normal order.
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
    if (skipNS && !p.en_stock) {
      out.filteredNonStock++;
      return;
    }
    if (!byTab.has(p.tablette)) byTab.set(p.tablette, []);
    byTab.get(p.tablette).push(idx);
  });
  out.planoShelves = byTab.size;
  out.availableShelves = slots.length;
  out.availableSections = fixture ? 0 : new Set(slots.map(([sectionNo]) => sectionNo)).size;
  if (!fixture) {
    const sections = ((config && config.sides && config.sides[side]) ? config.sides[side].sections : []) || [];
    out.startSectionShelves = ((sections[Math.max(0, startSection - 1)] || {}).shelves || []).length;
  }
  // Reversing Côté A's section path must never reverse products on a tablette.
  // Positions always stay exactly as numbered in the planogram.
  [...byTab.keys()].sort((a, b) => a - b).forEach((t, i) => {
    const idxs = byTab.get(t).slice().sort((a, b) => (planoData.products[a].position || 0) - (planoData.products[b].position || 0));
    if (i >= slots.length) {
      out.overflowShelves++;
      idxs.forEach(idx => out.overflow.add(idx));
      return;
    }
    const [secNo, ti] = slots[i];
    idxs.forEach(idx => {
      const raw = planoData.products[idx].position;
      out.byIdx[idx] = { section: secNo, shelf: ti + 1, position: raw };
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
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:7px 4px;border-bottom:1px solid #e2e8f0;font-size:12px;font-weight:700;color:#334155">
      <span>PDF : ${flow.planoShelves} tablette${flow.planoShelves !== 1 ? 's' : ''}</span>
      ${flow.isFixture
        ? `<span>${esc(sideDisplayLabel(side))} : ${flow.availableShelves} tablette${flow.availableShelves !== 1 ? 's' : ''} disponible${flow.availableShelves !== 1 ? 's' : ''}</span>`
        : `<span>Section ${startSection} : ${flow.startSectionShelves} tablette${flow.startSectionShelves !== 1 ? 's' : ''}</span>
           <span>Parcours d'import : ${flow.availableShelves} tablette${flow.availableShelves !== 1 ? 's' : ''} répartie${flow.availableShelves !== 1 ? 's' : ''} sur ${flow.availableSections} section${flow.availableSections !== 1 ? 's' : ''}</span>`}
      ${flow.overflowShelves ? `<span style="color:#c8102e">${flow.overflowShelves} tablette${flow.overflowShelves !== 1 ? 's' : ''} du PDF sans emplacement physique</span>` : '<span style="color:#15803d">Structure compatible</span>'}
    </div>
    ${flow.filteredNonStock ? `<div style="padding:8px 10px;background:#fffbeb;border-bottom:1px solid #fde68a;color:#92400e;font-size:12px">
      <strong>${flow.filteredNonStock} produit(s) « En stock: non » seront exclus.</strong> Leurs positions resteront sans produit importé.
    </div>` : ''}
    <div style="font-size:11px;color:#64748b;padding:4px 4px 6px">${((side === 'Gauche' || side === 'Droite') && !planoSectionCount(aisle, side === 'Gauche' ? 'Droite' : 'Gauche'))
      ? 'Allée à un seul côté (mur/comptoir) : <b>lecture simple de gauche à droite</b> — sections croissantes, positions telles quelles, rien d\'inversé.'
      : side === 'Gauche'
      ? 'Côté A : le plano va de la <b>Façade B vers la Façade A</b>. Seules les sections sont inversées : elles diminuent à partir de la section de départ (ex. S9 → S8). Les tablettes et les positions des produits restent telles quelles.'
      : side === 'Droite'
      ? 'Côté B : le plano continue normalement à partir de la section de départ, avec des <b>sections croissantes</b>. Les tablettes et les positions des produits restent telles quelles.'
      : `${esc(side)} : le plano remplit les tablettes de la façade à partir de la tablette de départ, vers le bas.`} Le plan physique du magasin reste prioritaire; seules les positions de ses tablettes sont ajustées.</div>
    ${rows || '<div style="padding:10px;font-size:12px;color:#64748b">Aucun produit dans cette sélection.</div>'}
    <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 4px;border-top:1px solid #e2e8f0;margin-top:4px;flex-wrap:wrap">
      <button class="btn btn-outline btn-inline" style="font-size:12px;width:auto;margin:0" onclick="planoAddLine()">➕ Ajouter une ligne</button>
      <span style="font-size:12px;font-weight:700;color:#1e293b">${flow.placed} produit(s) placé(s) ${overNote}</span>
    </div>`;
}

function reimportIncludingNonStock() {
  const skip = document.getElementById('planoSkipNonStock');
  const replace = document.getElementById('planoReplace');
  if (skip) skip.checked = false;
  if (replace) replace.checked = true;
  updatePlanoPreview();
  void importPlanogram();
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

  const filteredNonStock = skipNS
    ? (planoData.products || []).filter(p =>
        Number(p.tablette) >= tabStart && Number(p.tablette) <= tabEnd && !p.en_stock
      ).length
    : 0;
  if (filteredNonStock && !window.confirm(
    `${filteredNonStock} produit(s) marqués « En stock: non » ne seront pas importés et laisseront des positions vides. Continuer?`
  )) return;

  const btn = document.getElementById('planoImportBtn');
  btn.disabled = true; btn.textContent = 'Importation…';
  const msg = document.getElementById('planoImportMsg');
  msg.textContent = '';

  // The manually entered shelf count is what defines every section boundary.
  // Never import against an older server copy while an autosave is pending.
  const aisleKey = String(aisle);
  window.clearTimeout(_layoutAutoSaveTimers.get(aisleKey));
  _layoutAutoSaveTimers.delete(aisleKey);
  let saveWaits = 0;
  while (_layoutAutoSaveInFlight.has(aisleKey) && saveWaits < 200) {
    await new Promise(resolve => window.setTimeout(resolve, 50));
    saveWaits++;
  }
  if (_layoutAutoSaveInFlight.has(aisleKey)) {
    msg.textContent = 'La sauvegarde du plan prend trop de temps. Attendez un instant avant de relancer l’importation.';
    msg.style.color = '#c8102e';
    btn.disabled = false; btn.textContent = 'Importer dans le plan';
    return;
  }
  if (dirtyLayoutAisles.has(aisleKey)) await autoSaveAisleLayout(aisleKey);
  if (dirtyLayoutAisles.has(aisleKey)) {
    msg.textContent = 'Le plan du magasin doit être sauvegardé avant l’importation. Réessayez lorsque la sauvegarde est terminée.';
    msg.style.color = '#c8102e';
    btn.disabled = false; btn.textContent = 'Importer dans le plan';
    return;
  }
  const layout = getMutableLayout(aisleKey);
  if (!layout) {
    msg.textContent = 'Le plan de cette allée doit être rechargé avant l’importation.';
    msg.style.color = '#c8102e';
    btn.disabled = false; btn.textContent = 'Importer dans le plan';
    return;
  }

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
        expected_layout_modified_at: layout.modified_at || '',
        skip_non_stock:   skipNS,
        products: planoData.products,
        plano: planoData.plano || {},
        store: (typeof getCurrentStoreName === 'function') ? getCurrentStoreName() : ''
      })
    });
    if (res.ok && data.success) {
      const errTxt  = data.errors > 0 ? `, ${data.errors} erreur(s)` : '';
      const overflowShelves = Number(data.overflow_shelves ?? data.overflow ?? 0);
      const overflowProducts = Number(data.overflow_products ?? 0);
      const nonStockSkipped = Number(data.filtered_non_stock ?? 0);
      const otherSkipped = Math.max(0, Number(data.skipped ?? 0) - nonStockSkipped);
      const skippedTxt = otherSkipped > 0 ? `, ${otherSkipped} autre(s) ignoré(s)` : '';
      const nonStockTxt = nonStockSkipped > 0 ? `, ${nonStockSkipped} hors stock non importé(s)` : '';
      const replacedRemoved = Number(data.replaced_removed ?? data.pruned ?? 0);
      const replacedTxt = replacedRemoved > 0 ? `, ${replacedRemoved} ancien(s) remplacé(s)` : '';
      const overTxt = overflowShelves > 0 ? ` ⚠ ${overflowProducts} produit(s), sur ${overflowShelves} tablette(s) du PDF, n'ont pas d'emplacement physique dans le plan magasin.` : '';
      const recoveryAction = nonStockSkipped > 0
        ? ` <button type="button" class="btn btn-outline btn-inline" style="width:auto;margin:5px 0 0;font-size:12px" onclick="reimportIncludingNonStock()">Importer aussi les ${nonStockSkipped} hors stock</button>`
        : '';
      msg.innerHTML = `✅ <strong>${data.imported}</strong> importé(s)${nonStockTxt}${skippedTxt}${replacedTxt}${errTxt}.${overTxt} Les photos manquantes sont récupérées automatiquement.${recoveryAction}`;
      msg.style.color = nonStockSkipped > 0 ? '#92400e' : '#16a34a';
      // The import response already carries the committed aisle and affected
      // products. Paint it now; full-list revalidation can happen off-screen.
      applyPlanogramImportResult(aisle, side, data);
      refreshPlanUi();
      updatePlanoPreview();
      void loadPlanogramHistory();
      void Promise.allSettled([
        refreshLayoutsCache(true),
        refreshProductsCache(true),
      ]).then(() => {
        savePlanSnapshot();
        if (activeTab === 'add') refreshPlanUi();
      });
    } else {
      msg.textContent = data.error || 'Erreur lors de l’importation.';
      msg.style.color = '#c8102e';
    }
  } catch(e) {
    msg.textContent = 'Erreur réseau.'; msg.style.color = '#c8102e';
  }
  btn.disabled = false; btn.textContent = 'Importer dans le plan';
}

async function loadPlanogramHistory(force=false) {
  const panel = document.getElementById('planoHistoryPanel');
  const box = document.getElementById('planoHistory');
  const count = document.getElementById('planoHistoryCount');
  if (!box) return;
  if (!panel?.open) {
    box.dataset.loaded = '';
    if (count) count.textContent = 'Nouveau';
    return;
  }
  if (!force && box.dataset.loaded === '1') return;
  if (box.dataset.loading === '1') return;
  box.dataset.loading = '1';
  box.innerHTML = '<div class="small" style="padding:12px">Chargement...</div>';
  try {
    const {res, data} = await apiFetch('/api/planograms/history');
    if (!res.ok || !Array.isArray(data) || !data.length) {
      box.innerHTML = '<div class="small" style="padding:12px">Aucun planogramme importé pour le moment.</div>';
      box.dataset.loaded = '1';
      if (count) count.textContent = '0 import';
      return;
    }
    if (count) count.textContent = `${data.length} récent${data.length > 1 ? 's' : ''}`;
    box.innerHTML = data.map(h => {
      const title = [h.plano_name, h.plano_number ? `#${h.plano_number}` : '', h.plano_version ? `(${h.plano_version})` : '']
        .filter(Boolean).join(' ') || 'Planogramme';
      const when = (h.created_at || '').replace('T', ' ').slice(0, 16);
      const loc = `Allée ${esc(h.aisle)} · ${esc(sideStaffLabel(h.side))} · S${esc(h.section)} · T${esc(h.tablette_start)}–${esc(h.tablette_end)}`;
      return `<div class="plano-history-item">
        <div class="plano-history-title">${esc(title)}</div>
        <div class="plano-history-location">${loc}</div>
        <div class="plano-history-meta">${esc(when)} · ${esc(h.employee || '—')}${h.store ? ' · ' + esc(h.store) : ''} · ${esc(h.imported)} importé(s), ${esc(h.skipped)} ignoré(s)</div>
      </div>`;
    }).join('');
    box.dataset.loaded = '1';
  } catch (e) {
    box.innerHTML = '<div class="small" style="padding:12px;color:#c8102e">Impossible de charger l’historique.</div>';
    if (count) count.textContent = 'Réessayer';
  } finally {
    box.dataset.loading = '';
  }
}

let productQualityPollTimer = null;

const PRODUCT_QUALITY_LABELS = {
  possible_wrong_image: 'Image possiblement incorrecte',
  possible_wrong_description: 'Description possiblement incorrecte',
  package_size_conflict: 'Conflit de format',
  strength_conflict: 'Conflit de concentration',
  variant_conflict: 'Conflit de variante',
  format_conflict: 'Conflit de forme',
  product_name_conflict: 'Nom incohérent',
  multiple_possible_matches: 'Plusieurs correspondances',
  unverified_suggestion: 'Proposition à vérifier',
  missing_description: 'Description manquante',
  missing_image: 'Image manquante',
  upc_conflict: 'UPC à vérifier',
  identifier_conflict: 'Identifiant incohérent',
};

function productQualityValue(value, field) {
  const text = String(value || '').trim();
  if (!text) return '<span class="product-quality-empty">Aucune valeur</span>';
  if (field === 'image_url' && /^https:\/\//i.test(text)) {
    return `<span class="product-quality-image-value"><img src="${esc(text)}" alt="" loading="lazy"/>${esc(text)}</span>`;
  }
  return esc(text);
}

function renderProductQualityIssues(issues) {
  const box = document.getElementById('productQualityIssues');
  if (!box) return;
  if (!issues.length) {
    box.innerHTML = '<div class="small product-quality-empty-list">Aucune anomalie ouverte dans ce filtre.</div>';
    return;
  }
  box.innerHTML = issues.map(issue => {
    const field = String(issue.field_name || '');
    const current = String(issue.existing_value || issue[field] || '').trim();
    const candidate = String(issue.candidate_value || '').trim();
    const location = issue.aisle
      ? `Allée ${esc(issue.aisle)} · ${esc(sideStaffLabel(issue.side))} · S${esc(issue.section)} T${esc(issue.shelf)} P${esc(issue.position)}`
      : 'Position non confirmée';
    const actions = [];
    if (candidate) {
      actions.push(`<button class="btn btn-inline" type="button" onclick="resolveProductQualityIssue(${Number(issue.id)},'accept_candidate')">Utiliser la proposition</button>`);
      if (current) actions.push(`<button class="btn btn-outline btn-inline" type="button" onclick="resolveProductQualityIssue(${Number(issue.id)},'keep_existing')">Garder l’actuel</button>`);
    } else if (current && field) {
      actions.push(`<button class="btn btn-outline btn-inline" type="button" onclick="resolveProductQualityIssue(${Number(issue.id)},'mark_verified')">Marquer vérifié</button>`);
      if (String(issue.issue_type || '').startsWith('possible_wrong')) {
        actions.push(`<button class="btn btn-outline btn-inline btn-danger" type="button" onclick="resolveProductQualityIssue(${Number(issue.id)},'clear_field')">Retirer la valeur</button>`);
      }
    }
    return `<div class="product-quality-item">
      <div class="product-quality-item-head">
        <div><strong>${esc(issue.product_name || 'Produit')}</strong><span>${esc(issue.barcode || 'UPC inconnu')}</span></div>
        <span class="product-quality-issue-label">${esc(PRODUCT_QUALITY_LABELS[issue.issue_type] || issue.issue_type || 'À vérifier')}</span>
      </div>
      <div class="product-quality-location">${location}</div>
      ${field ? `<div class="product-quality-values">
        <div><span>Valeur actuelle</span>${productQualityValue(current, field)}</div>
        ${candidate ? `<div><span>Proposition</span>${productQualityValue(candidate, field)}</div>` : ''}
      </div>` : ''}
      ${issue.source ? `<div class="product-quality-source">Source : ${esc(issue.source)}</div>` : ''}
      ${actions.length ? `<div class="product-quality-actions">${actions.join('')}</div>` : ''}
    </div>`;
  }).join('');
}

async function loadProductQuality(force=false) {
  const panel = document.getElementById('productQualityPanel');
  const summaryBox = document.getElementById('productQualitySummary');
  const issueBox = document.getElementById('productQualityIssues');
  const count = document.getElementById('productQualityCount');
  if (!panel?.open || !summaryBox || !issueBox) return;
  if (!force && issueBox.dataset.loading === '1') return;
  issueBox.dataset.loading = '1';
  const filter = document.getElementById('productQualityFilter')?.value || '';
  try {
    const [summaryResult, issuesResult] = await Promise.all([
      apiFetch('/api/product-quality/summary'),
      apiFetch(`/api/product-quality/issues?status=open&limit=80${filter ? `&type=${encodeURIComponent(filter)}` : ''}`),
    ]);
    if (!summaryResult.res.ok || !issuesResult.res.ok) throw new Error('quality-load');
    const summary = summaryResult.data || {};
    const issues = Array.isArray(issuesResult.data?.issues) ? issuesResult.data.issues : [];
    const openTotal = Object.values(summary.open_issues || {}).reduce((sum, value) => sum + Number(value || 0), 0);
    const audit = summary.audit || {};
    const identifierCoverage = summary.identifier_coverage || {};
    const verifiedIdentifierCount = type => Number(identifierCoverage[type]?.verified || 0);
    if (count) count.textContent = `${openTotal} à vérifier`;
    const progress = audit.running
      ? `<div class="product-quality-progress"><span style="width:${Math.min(100, audit.total ? (Number(audit.scanned || 0) / Number(audit.total)) * 100 : 4)}%"></span></div><small>Vérification ${Number(audit.scanned || 0)} / ${Number(audit.total || 0)}</small>`
      : audit.error ? `<small class="product-quality-error">${esc(audit.error)}</small>` : '';
    summaryBox.innerHTML = `<div><strong>${Number(summary.verified_products || 0)}</strong><span>fiches entièrement vérifiées</span></div>
      <div><strong>${openTotal}</strong><span>points à examiner</span></div>
      <div><strong>${Number(summary.total_products || 0)}</strong><span>produits dans le plan</span></div>
      <div><strong>${verifiedIdentifierCount('DIN')}</strong><span>DIN vérifiés</span></div>
      <div><strong>${verifiedIdentifierCount('NPN')}</strong><span>NPN vérifiés</span></div>
      <div><strong>${verifiedIdentifierCount('DIN_HM')}</strong><span>DIN-HM vérifiés</span></div>${progress}`;
    renderProductQualityIssues(issues);
    clearTimeout(productQualityPollTimer);
    if (audit.running) productQualityPollTimer = setTimeout(() => loadProductQuality(true), 1800);
  } catch (_) {
    issueBox.innerHTML = '<div class="small product-quality-error">Impossible de charger les vérifications.</div>';
    if (count) count.textContent = 'Réessayer';
  } finally {
    issueBox.dataset.loading = '';
  }
}

async function startProductQualityAudit() {
  const summary = document.getElementById('productQualitySummary');
  if (summary) summary.innerHTML = '<div class="small">Démarrage de la vérification...</div>';
  try {
    const {res, data} = await apiFetch('/api/product-quality/audit', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({}),
    });
    if (!res.ok || !data?.success) throw new Error(data?.error || 'quality-start');
    await loadProductQuality(true);
  } catch (_) {
    if (summary) summary.innerHTML = '<div class="small product-quality-error">La vérification n’a pas pu démarrer.</div>';
  }
}

async function resolveProductQualityIssue(issueId, action) {
  try {
    const {res, data} = await apiFetch(`/api/product-quality/issues/${Number(issueId)}/resolve`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({action}),
    });
    if (!res.ok || !data?.success) throw new Error(data?.error || 'quality-resolve');
    await loadProductQuality(true);
  } catch (error) {
    const box = document.getElementById('productQualityIssues');
    if (box) box.insertAdjacentHTML('afterbegin', `<div class="small product-quality-error">${esc(error.message || 'Impossible d’enregistrer la décision.')}</div>`);
  }
}

window.AppLayout = { renderMapEditor, loadMapEditor, refreshPlanUi, createAisleLayout, saveAisleLayout, refreshProductsCache, refreshLayoutsCache, loadPlanogramHistory, loadProductQuality };
