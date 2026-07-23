// ── Product normalization ─────────────────────────────────────────────────────
function normalizeProduct(product) {
  return {
    id: (product.id === undefined || product.id === null || product.id === '') ? null : Number(product.id),
    client_id: String(product.client_id || '').trim(),
    catalog_only: product.catalog_only ? true : false,
    name: String(product.name || '').trim(),
    brand: String(product.brand || '').trim(),
    description: String(product.description || '').trim(),
    image_url: safeHttpUrl(product.image_url),
    source_url: safeHttpUrl(product.source_url),
    search_terms: String(product.search_terms || '').trim(),
    usage_notes: String(product.usage_notes || '').trim(),
    alternative_suggestions: String(product.alternative_suggestions || '').trim(),
    barcode: String(product.barcode || '').trim(),
    product_code: String(product.product_code || '').trim(),
    data_status: String(product.data_status || 'complete_unverified').trim(),
    identity_status: String(product.identity_status || 'unverified').trim(),
    name_status: String(product.name_status || 'unverified').trim(),
    description_status: String(product.description_status || 'unverified').trim(),
    image_status: String(product.image_status || 'unverified').trim(),
    quality_issue_count: Number(product.quality_issue_count || 0),
    image_available_unverified: Boolean(product.image_available_unverified),
    description_available_unverified: Boolean(product.description_available_unverified),
    category: String(product.category || '').trim(),
    package_size: String(product.package_size || '').trim(),
    package_unit: String(product.package_unit || '').trim(),
    variant: String(product.variant || '').trim(),
    flavour: String(product.flavour || '').trim(),
    colour: String(product.colour || '').trim(),
    strength: String(product.strength || '').trim(),
    dosage_form: String(product.dosage_form || '').trim(),
    manufacturer: String(product.manufacturer || '').trim(),
    ingredients: String(product.ingredients || '').trim(),
    compatibility: String(product.compatibility || '').trim(),
    purpose: String(product.purpose || '').trim(),
    route_of_administration: String(product.route_of_administration || '').trim(),
    regulatory_identifiers: (Array.isArray(product.regulatory_identifiers)
      ? product.regulatory_identifiers : []).slice(0, 12).map(identifier => ({
        type: String(identifier?.type || '').trim(),
        value: String(identifier?.value || '').trim(),
        authority: String(identifier?.authority || '').trim(),
        source: String(identifier?.source || '').trim(),
        status: String(identifier?.status || 'probable').trim(),
        label: String(identifier?.label || 'À confirmer').trim(),
        match_method: String(identifier?.match_method || '').trim(),
        confidence: Number(identifier?.confidence || 0),
      })).filter(identifier => identifier.type && identifier.value),
    identifiers: (Array.isArray(product.identifiers)
      ? product.identifiers : []).slice(0, 40).map(identifier => ({
        type: String(identifier?.type || '').trim().toUpperCase().replace(/-/g, '_'),
        value: String(identifier?.value || '').trim(),
        authority: String(identifier?.authority || '').trim(),
        source: String(identifier?.source || '').trim(),
        status: String(identifier?.status || 'probable').trim(),
        label: String(identifier?.label || 'À confirmer').trim(),
        match_method: String(identifier?.match_method || '').trim(),
        confidence: Number(identifier?.confidence || 0),
      })).filter(identifier => identifier.type && identifier.value),
    primary_source: String(product.primary_source || '').trim(),
    primary_source_url: safeHttpUrl(product.primary_source_url),
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

// Escape a value for a single-quoted JavaScript argument inside an HTML
// attribute. HTML escaping alone is insufficient because entities are decoded
// before an inline handler is compiled.
function jsq(value) {
  const javascriptSafe = String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/'/g, '\\x27')
    .replace(/\r/g, '\\r')
    .replace(/\n/g, '\\n')
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');
  return esc(javascriptSafe);
}

function safeHttpUrl(value) {
  const raw = String(value || '').trim().slice(0, 2048);
  if (!raw) return '';
  try {
    const parsed = new URL(raw, window.location.origin);
    const sameOrigin = parsed.origin === window.location.origin;
    if (parsed.protocol === 'https:' || (sameOrigin && parsed.protocol === 'http:')) return parsed.href;
  } catch (_) {}
  return '';
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

async function saveEditorName() {
  const username = document.getElementById('editorName').value.trim() || 'appareil';
  localStorage.setItem(STORAGE_KEYS.editorSession, JSON.stringify({username}));
  if (isUnlocked()) {
    try {
      const {res, data} = await apiFetch('/api/auth/profile', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({username}),
      });
      if (res.ok && data?.username) window.AppLock?.setAuthenticatedUsername?.(data.username);
    } catch (_) {}
  }
  updateAppShellState();
}

function requireEditorSession() {
  if (isUnlocked()) return true;
  showLockModal(localStorage.getItem(STORAGE_KEYS.activeTab) || 'search');
  return false;
}

function getEditorHeaders() { return {}; }

function setActiveTabUi(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
  document.getElementById(tab)?.classList.add('active');
  const tabs = ['search','client','scan','add'];
  const button = document.querySelectorAll('.tab')[tabs.indexOf(tab)];
  if (button) button.classList.add('active');
  const moveReceipt = document.getElementById('planMoveReceipt');
  if (moveReceipt) moveReceipt.hidden = tab !== 'add';
  localStorage.setItem(STORAGE_KEYS.activeTab, tab);
}

// ── Tab switching ─────────────────────────────────────────────────────────────
async function switchTab(tab) {
  // All store views require the server-verified, non-expired employee session.
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
  persistClientDraft();
  if (isUnlocked()) { persistScanDraft(); persistAddDraft(); }
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

let _appDataLoaded = false;
let _appLoadPromise = null;

async function _loadAppData(preferredTab=null) {
  loadCursor();
  loadClientDraft();
  if (isUnlocked()) {
    loadScanDraft();
    loadAddDraft();
  }
  ensureStoreSelected();

  // Every employee tab uses the same mapped products. Restore the last compact
  // server snapshot before the first network await so Plan, Search and Client
  // are useful immediately even while Render is waking up.
  const restoredPlanSnapshot = typeof restorePlanSnapshot === 'function'
    ? restorePlanSnapshot() : false;
  if (restoredPlanSnapshot && typeof restoreProductMediaSnapshot === 'function') {
    await restoreProductMediaSnapshot();
  }

  const startTab = preferredTab && LOCKED_TABS.has(preferredTab) ? preferredTab : getStartupTab();
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
  const activeTabAfterLoad = preferredTab || localStorage.getItem(STORAGE_KEYS.activeTab) || startTab;
  runStartupTabEffects(activeTabAfterLoad);
  if (activeTabAfterLoad === 'scan') focusScanInput();
}

async function resumeAuthenticatedApp(preferredTab=null) {
  if (!isUnlocked()) {
    showLockModal(preferredTab);
    return false;
  }
  if (_appDataLoaded) {
    loadScanDraft();
    loadAddDraft();
    if (preferredTab) await switchTab(preferredTab);
    return true;
  }
  if (_appLoadPromise) {
    // Open from the saved Plan while the large public catalog refresh keeps
    // running. The refresh reconciles this view in the background.
    if (preferredTab) await switchTab(preferredTab);
    return true;
  }
  _appLoadPromise = (async () => {
    await _loadAppData(preferredTab);
    _appDataLoaded = true;
    return true;
  })().finally(() => { _appLoadPromise = null; });
  return _appLoadPromise;
}

function resetAuthenticatedAppState() {
  dirtyLayoutAisles.clear();
  for (const id of ['mapContent', 'scanResult']) {
    const element = document.getElementById(id);
    if (element) element.textContent = '';
  }
  if (scannerStream || html5Scanner || quaggaActive) void stopCamera();
  const activeTab = localStorage.getItem(STORAGE_KEYS.activeTab) || 'search';
  if (LOCKED_TABS.has(activeTab)) setActiveTabUi('search');
}

async function bootApp() {
  updateDeviceSupport();
  updateAppShellState();
  updateNetworkStatus();
  updateLockUi();
  paintStartupTab('search');
  await initializeAuth();
  _appLoadPromise = _loadAppData().then(() => {
    _appDataLoaded = true;
    return true;
  }).finally(() => { _appLoadPromise = null; });
  await _appLoadPromise;
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
  window.AppLock?.enforceSessionExpiry?.();
  updateLockUi();
}, 30000);

window.resumeAuthenticatedApp = resumeAuthenticatedApp;
window.resetAuthenticatedAppState = resetAuthenticatedAppState;
window.AppMain = { switchTab, bootApp, resumeAuthenticatedApp, resetAuthenticatedAppState };
