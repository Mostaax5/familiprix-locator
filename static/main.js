// ── Product normalization ─────────────────────────────────────────────────────
function normalizeProduct(product) {
  return {
    id: (product.id === undefined || product.id === null || product.id === '') ? null : Number(product.id),
    client_id: String(product.client_id || '').trim(),
    catalog_only: product.catalog_only ? true : false,
    name: String(product.name || '').trim(),
    brand: String(product.brand || '').trim(),
    description: String(product.description || '').trim(),
    image_url: String(product.image_url || '').trim(),
    source_url: String(product.source_url || '').trim(),
    search_terms: String(product.search_terms || '').trim(),
    usage_notes: String(product.usage_notes || '').trim(),
    alternative_suggestions: String(product.alternative_suggestions || '').trim(),
    barcode: String(product.barcode || '').trim(),
    product_code: String(product.product_code || '').trim(),
    facings: Number(product.facings) > 0 ? Number(product.facings) : 1,
    aisle: String(product.aisle || '').trim(),
    side: String(product.side || 'Gauche').trim() || 'Gauche',
    section: String(product.section || '1').trim() || '1',
    shelf: String(product.shelf || '').trim(),
    position: String(product.position || '').trim(),
    locations: Array.isArray(product.locations) ? product.locations.map(location => ({
      aisle: String(location?.aisle || '').trim(),
      side: String(location?.side || 'Gauche').trim() || 'Gauche',
      section: String(location?.section || '1').trim() || '1',
      shelf: String(location?.shelf || '').trim(),
      position: String(location?.position || '').trim(),
    })) : [],
    is_plano: Number(product.is_plano) ? 1 : 0,
    in_stock: (product.in_stock === 0 || product.in_stock === '0') ? 0 : 1,
    linked_position: String(product.linked_position || '').trim(),
    flipped_label: Number(product.flipped_label) ? 1 : 0,
    underneath_label: String(product.underneath_label || '').trim(),
    modified_by: String(product.modified_by || '').trim(),
    modified_at: String(product.modified_at || '').trim(),
    created_by: String(product.created_by || '').trim(),
    created_at: String(product.created_at || '').trim(),
    last_change_by: String(product.last_change_by || product.modified_by || product.created_by || '').trim(),
    last_change_at: String(product.last_change_at || product.modified_at || product.created_at || '').trim()
  };
}

function upsertCachedProduct(product) {
  const normalized = normalizeProduct(product);
  const index = allProductsCache.findIndex(item => Number(item.id) === Number(normalized.id));
  if (index >= 0) allProductsCache[index] = normalized;
  else allProductsCache.push(normalized);
  if (typeof invalidateProductSearchIndexes === 'function') invalidateProductSearchIndexes();
  lastProductsRefreshAt = Date.now();
  if (typeof savePlanSnapshot === 'function') savePlanSnapshot();
}

function removeCachedProduct(productId) {
  allProductsCache = allProductsCache.filter(item => Number(item.id) !== Number(productId));
  if (typeof invalidateProductSearchIndexes === 'function') invalidateProductSearchIndexes();
  lastProductsRefreshAt = Date.now();
  if (typeof savePlanSnapshot === 'function') savePlanSnapshot();
}

// ── Utilities ────────────────────────────────────────────────────────────────
function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// The internal side value stays 'Gauche'/'Droite' (DB + layout config keys),
// but it is NEVER shown to users — everything displays "Côté A"/"Côté B".
function sideDisplayLabel(side) {
  return side === 'Gauche' ? 'Côté A' : side === 'Droite' ? 'Côté B' : String(side || '');
}
// Back-compat alias: some call sites used a separate "staff" label that used to
// show the raw side. Côté A/B is now the only label shown anywhere.
function sideStaffLabel(side) {
  return sideDisplayLabel(side);
}

function isHomeBrand(brand) {
  const b = (brand || '').toLowerCase().trim();
  return b.startsWith('essentiel') || b.startsWith('biomedic');
}

function isStandaloneApp() {
  return window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
}

function nowIsoWithoutMs() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

// ── Editor session ────────────────────────────────────────────────────────────
function loadEditorSession() {
  const raw = localStorage.getItem(STORAGE_KEYS.editorSession);
  if (!raw) return {username: 'appareil'};
  try { return {username: JSON.parse(raw).username || 'appareil'}; }
  catch (e) { return {username: 'appareil'}; }
}

function saveEditorName() {
  const username = document.getElementById('editorName').value.trim() || 'appareil';
  localStorage.setItem(STORAGE_KEYS.editorSession, JSON.stringify({username}));
  updateAppShellState();
}

function requireEditorSession() { return true; }

function getEditorHeaders() {
  return {'X-User-Name': loadEditorSession().username || 'appareil'};
}

function setActiveTabUi(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.getElementById(tab)?.classList.add('active');
  const tabs = ['search','client','scan','add'];
  const button = document.querySelectorAll('.tab')[tabs.indexOf(tab)];
  if (button) button.classList.add('active');
  localStorage.setItem(STORAGE_KEYS.activeTab, tab);
}

// ── Tab switching ─────────────────────────────────────────────────────────────
async function switchTab(tab) {
  // Locked sections (Scan, Plan) require the password and a non-expired session.
  // The 8h timer is fixed from unlock time — NOT refreshed on use — so the
  // session truly expires 8h after the password was entered (one per shift).
  if (LOCKED_TABS.has(tab) && !isUnlocked()) { showLockModal(tab); return; }
  // Stop the camera/decoders when leaving — MUST include quaggaActive: Quagga
  // runs its own stream (scannerStream stays null), so without this it keeps
  // decoding video forever in the background and overheats the device.
  if (scannerStream || html5Scanner || quaggaActive) await stopCamera();
  setActiveTabUi(tab);
  if (tab === 'add') loadMapEditor();
  if (tab === 'search') {
    if (document.getElementById('searchInput')?.value.trim()) doSearch();
    refreshProductsCache().then(() => {
      if (document.getElementById('search').classList.contains('active') && document.getElementById('searchInput')?.value.trim()) doSearch();
    });
  }
  if (tab === 'scan') { if (typeof populateRayonAisleList === 'function') populateRayonAisleList(); window.setTimeout(focusScanInput, 50); }
  if (tab === 'client') window.setTimeout(() => document.getElementById('clientQuestion')?.focus(), 50);
  if (tab === 'search') window.setTimeout(() => document.getElementById('searchInput')?.focus(), 50);
}

// ── Boot ──────────────────────────────────────────────────────────────────────
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    const assetVersion = document.querySelector('meta[name="app-asset-version"]')?.content || 'current';
    navigator.serviceWorker.register(`/service-worker.js?v=${encodeURIComponent(assetVersion)}`).then(registration => {
      registration.update().catch(() => {});
    }).catch(() => {});
  });
}

document.addEventListener('keydown', handleHardwareScannerKey);
document.addEventListener('focusin', event => {
  if (event.target instanceof HTMLInputElement && event.target.type === 'number') {
    selectNumericField(event.target);
  }
});
document.getElementById('mapContent').addEventListener('toggle', event => {
  const node = event.target;
  if (!(node instanceof HTMLDetailsElement)) return;
  const nodeId = node.dataset.nodeId;
  if (!nodeId) return;
  if (node.open) { openPlanNodes.delete('--closed--' + nodeId); openPlanNodes.add(nodeId); }
  else openPlanNodes.delete(nodeId);
  if (node.open && typeof hydratePlanNode === 'function') hydratePlanNode(node);
}, true);
document.getElementById('scanInput').addEventListener('input', persistScanDraft);
document.getElementById('clientQuestion').addEventListener('input', persistClientDraft);
['mapAisle','mapLeftSections','mapRightSections','mapInitialShelves','mapInitialPositions'].forEach(id => {
  document.getElementById(id).addEventListener('input', persistAddDraft);
  document.getElementById(id).addEventListener('change', persistAddDraft);
});
window.addEventListener('online', updateNetworkStatus);
window.addEventListener('offline', updateNetworkStatus);
document.addEventListener('visibilitychange', () => {
  // Include quaggaActive — Quagga has no scannerStream, so otherwise it keeps
  // running (and draining/heating the phone) while the app is backgrounded.
  if (document.hidden && (scannerStream || html5Scanner || quaggaActive)) stopCamera();
});
window.addEventListener('pageshow', () => { updateAppShellState(); updateNetworkStatus(); });
window.addEventListener('pagehide', () => {
  persistScanDraft(); persistAddDraft(); persistClientDraft();
  if (scannerStream || html5Scanner || quaggaActive) stopCamera();
});

function getStartupTab() {
  const savedTab = localStorage.getItem(STORAGE_KEYS.activeTab);
  const validTabs = ['scan','search','client','add'];
  const preferred = (savedTab && validTabs.includes(savedTab)) ? savedTab : 'search';
  return (LOCKED_TABS.has(preferred) && !isUnlocked()) ? 'search' : preferred;
}

function paintStartupTab(tab) {
  setActiveTabUi(tab);
  if (tab === 'add') {
    if (mapLayouts.length) {
      normalizeCursorToLayout();
      updateCursorUi();
      renderPlanStartEditor();
      refreshPlanUi();
    } else if (typeof showPlanLoading === 'function') {
      showPlanLoading();
    }
  }
}

function runImmediateStartupEffects(tab) {
  if (tab === 'add' && mapLayouts.length) refreshPlanUi();
  if (tab === 'search' && document.getElementById('searchInput')?.value.trim()) doSearch();
  if (tab === 'scan') {
    if (typeof populateRayonAisleList === 'function') populateRayonAisleList();
    window.setTimeout(focusScanInput, 50);
  }
  if (tab === 'client') window.setTimeout(() => document.getElementById('clientQuestion')?.focus(), 50);
  if (tab === 'search') window.setTimeout(() => document.getElementById('searchInput')?.focus(), 50);
}

function runStartupTabEffects(tab) {
  if (tab === 'add') {
    refreshPlanUi();
    loadPlanogramHistory();
    loadReferenceCount();
  }
  if (tab === 'search') {
    if (document.getElementById('searchInput')?.value.trim()) doSearch();
    refreshProductsCache().then(() => {
      if (document.getElementById('search').classList.contains('active') && document.getElementById('searchInput')?.value.trim()) doSearch();
    });
  }
  if (tab === 'scan') {
    if (typeof populateRayonAisleList === 'function') populateRayonAisleList();
    window.setTimeout(focusScanInput, 50);
  }
  if (tab === 'client') window.setTimeout(() => document.getElementById('clientQuestion')?.focus(), 50);
  if (tab === 'search') window.setTimeout(() => document.getElementById('searchInput')?.focus(), 50);
}

async function bootApp() {
  loadCursor();
  loadScanDraft();
  loadAddDraft();
  loadClientDraft();
  updateDeviceSupport();
  updateAppShellState();
  updateNetworkStatus();
  updateLockUi();
  ensureStoreSelected();

  // Every employee tab uses the same mapped products. Restore the last compact
  // server snapshot before the first network await so Plan, Search and Client
  // are useful immediately even while Render is waking up.
  if (typeof restorePlanSnapshot === 'function') restorePlanSnapshot();

  const startTab = getStartupTab();
  paintStartupTab(startTab);
  runImmediateStartupEffects(startTab);

  await Promise.allSettled([
    refreshProductsCache(true),
    refreshLayoutsCache(true),
  ]);

  // Diagnostics include duplicate/reference counts. Start them only after the
  // plan requests so a first-time device gives its database connection to the
  // employee-facing data first.
  void apiGetSystemInfo().then(info => {
    backendInfo = {...backendInfo, ...info};
    updateAppShellState();
    updateNetworkStatus();
  }).catch(() => {});
  normalizeCursorToLayout();
  updateCursorUi();
  renderPlanStartEditor();
  updateAppShellState();
  updateNetworkStatus();
  updateLockUi();
  savePlanSnapshot();
  const activeTabAfterLoad = localStorage.getItem(STORAGE_KEYS.activeTab) || startTab;
  runStartupTabEffects(activeTabAfterLoad);
  if (activeTabAfterLoad === 'scan') focusScanInput();
}

bootApp();
// Scanner code is large and comes from a CDN. Warm it only after the browser is
// idle so it cannot compete with the plan snapshot, search, or first interaction.
if ('requestIdleCallback' in window) {
  window.requestIdleCallback(() => ensureQuaggaLoaded(), {timeout: 5000});
} else {
  window.setTimeout(() => ensureQuaggaLoaded(), 3000);
}

// Enforce session expiry even with no navigation: every 30s, if the session has
// expired while the user sits on a locked tab, re-lock the UI and leave the tab.
window.setInterval(() => {
  const activeTab = localStorage.getItem(STORAGE_KEYS.activeTab);
  if (LOCKED_TABS.has(activeTab) && !isUnlocked()) {
    updateLockUi();
    switchTab('search');
  }
}, 30000);

window.AppMain = { switchTab, bootApp };
