// ── Product normalization ─────────────────────────────────────────────────────
function normalizeProduct(product) {
  return {
    id: Number(product.id),
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
    aisle: String(product.aisle || '').trim(),
    side: String(product.side || 'Gauche').trim() || 'Gauche',
    section: String(product.section || '1').trim() || '1',
    shelf: String(product.shelf || '').trim(),
    position: String(product.position || '').trim(),
    is_plano: Number(product.is_plano) ? 1 : 0,
    in_stock: (product.in_stock === 0 || product.in_stock === '0') ? 0 : 1,
    linked_position: String(product.linked_position || '').trim(),
    flipped_label: Number(product.flipped_label) ? 1 : 0,
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
  lastProductsRefreshAt = Date.now();
}

function removeCachedProduct(productId) {
  allProductsCache = allProductsCache.filter(item => Number(item.id) !== Number(productId));
  lastProductsRefreshAt = Date.now();
}

// ── Utilities ────────────────────────────────────────────────────────────────
function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function sideDisplayLabel(side) {
  return side === 'Gauche' ? 'Côté A' : side === 'Droite' ? 'Côté B' : String(side || '');
}
function sideStaffLabel(side) {
  return String(side || '');
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

// ── Tab switching ─────────────────────────────────────────────────────────────
async function switchTab(tab) {
  // Locked sections (Scan, Plan) require the password and a non-expired session.
  // The 4h timer is fixed from unlock time — NOT refreshed on use — so the
  // session truly expires 4h after the password was entered.
  if (LOCKED_TABS.has(tab) && !isUnlocked()) { showLockModal(tab); return; }
  if (scannerStream || html5Scanner) await stopCamera();
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.getElementById(tab).classList.add('active');
  const tabs = ['search','client','scan','add'];
  document.querySelectorAll('.tab')[tabs.indexOf(tab)].classList.add('active');
  localStorage.setItem(STORAGE_KEYS.activeTab, tab);
  if (tab === 'add') loadMapEditor();
  if (tab === 'search') {
    if (document.getElementById('searchInput')?.value.trim()) doSearch();
    refreshProductsCache().then(() => {
      if (document.getElementById('search').classList.contains('active') && document.getElementById('searchInput')?.value.trim()) doSearch();
    });
  }
  if (tab === 'client') {
    runClientSearch(false);
    refreshProductsCache().then(() => {
      if (document.getElementById('client').classList.contains('active')) runClientSearch(false);
    });
  }
  if (tab === 'scan') window.setTimeout(focusScanInput, 50);
  if (tab === 'client') window.setTimeout(() => document.getElementById('clientQuestion')?.focus(), 50);
  if (tab === 'search') window.setTimeout(() => document.getElementById('searchInput')?.focus(), 50);
}

// ── Boot ──────────────────────────────────────────────────────────────────────
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('/service-worker.js').then(registration => {
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
  if (node.open) openPlanNodes.add(nodeId);
  else openPlanNodes.delete(nodeId);
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
  if (document.hidden && (scannerStream || html5Scanner)) stopCamera();
});
window.addEventListener('pageshow', () => { updateAppShellState(); updateNetworkStatus(); });
window.addEventListener('pagehide', () => {
  persistScanDraft(); persistAddDraft(); persistClientDraft();
  if (scannerStream || html5Scanner) stopCamera();
});

async function bootApp() {
  const [systemResult, productsResult, layoutsResult] = await Promise.allSettled([
    apiGetSystemInfo(), apiGetProducts(), apiGetLayoutAisles()
  ]);
  if (systemResult.status === 'fulfilled') backendInfo = {...backendInfo, ...systemResult.value};
  if (productsResult.status === 'fulfilled') {
    allProductsCache = productsResult.value;
    lastProductsRefreshAt = Date.now();
  } else {
    allProductsCache = [];
  }
  if (layoutsResult.status === 'fulfilled') {
    mapLayouts = layoutsResult.value.map(layout => syncLayoutRecord({
      ...layout,
      config: normalizeLayoutConfig(layout.config, layout.max_section, layout.max_shelf, layout.max_position)
    }));
    sortMapLayouts();
    dirtyLayoutAisles = new Set();
    lastLayoutsRefreshAt = Date.now();
  } else {
    mapLayouts = [];
  }
  loadCursor();
  normalizeCursorToLayout();
  updateCursorUi();
  renderPlanStartEditor();
  loadScanDraft();
  loadAddDraft();
  loadClientDraft();
  updateDeviceSupport();
  updateAppShellState();
  updateNetworkStatus();
  updateLockUi();
  ensureStoreSelected();
  const savedTab = localStorage.getItem(STORAGE_KEYS.activeTab);
  const validTabs = ['scan','search','client','add'];
  const preferred = (savedTab && validTabs.includes(savedTab)) ? savedTab : 'scan';
  const startTab  = (LOCKED_TABS.has(preferred) && !isUnlocked()) ? 'search' : preferred;
  switchTab(startTab);
  runClientSearch(false);
  if (startTab === 'scan') focusScanInput();
}

bootApp().then(() => {
  ensureQuaggaLoaded();
});

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
