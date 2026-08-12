const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('static/expiry-ui.js', 'utf8');

function makeElement(id, value='') {
  const classes = new Set();
  return {
    id,
    value,
    innerHTML: '',
    textContent: '',
    hidden: false,
    disabled: false,
    style: {},
    className: '',
    classList: {
      toggle(name, enabled) { if (enabled) classes.add(name); else classes.delete(name); },
      contains(name) { return classes.has(name); },
    },
    focus() {},
    scrollIntoView() {},
  };
}

function createContext() {
  const ids = [
    'expiryInitials', 'expiryScanInput', 'expiryLookupResult',
    'expiryDateInput', 'expiryNoteInput', 'expirySaveStatus', 'expirySaveButton',
    'expiryClearButton', 'expiryBoardList', 'expiryBoardSearch', 'expiryBoardFilter',
    'expirySummary', 'expiryBoardUpdated', 'expiryScanView', 'expiryBoardView',
    'expiryModeScan', 'expiryModeBoard', 'expiryScannerStatus',
  ];
  const elements = new Map(ids.map(id => [id, makeElement(id)]));
  elements.get('expiryBoardFilter').value = 'all';
  const storage = new Map();
  let savedPayload = null;
  const product = {
    found: true,
    in_plan: true,
    barcode: '063848907665',
    product_code: '123456',
    name: 'BIOMEDIC GEL ANALG GLACE 255G',
    brand: 'Biomedic',
    image_url: 'https://example.com/gel.jpg',
    locations: [{label: 'Allée Labo · Côté A · S2 T3 P4'}],
  };
  const current = {
    gtin_key: 'gtin:00063848907665',
    barcode: product.barcode,
    product_name: product.name,
    earliest_expiry_date: '2026-10-01',
    checked_at: '2026-08-12T12:00:00+00:00',
    checked_by: 'AM',
    revision: 3,
    urgency: 'watch',
    days_remaining: 50,
    locations: product.locations,
  };
  const lookup = {success: true, product, current, history: []};
  const context = {
    console,
    Intl,
    Date,
    Object,
    Array,
    Number,
    String,
    Math,
    Promise,
    STORAGE_KEYS: {expiryInitials: 'expiry-initials', editorSession: 'editor-session'},
    allProductsCache: [],
    cameraUsageMode: 'scan',
    scannerStream: null,
    html5Scanner: null,
    quaggaActive: false,
    nativeScanActive: false,
    zxingActive: false,
    scanPaused: false,
    localStorage: {
      getItem(key) { return storage.has(key) ? storage.get(key) : null; },
      setItem(key, value) { storage.set(key, String(value)); },
    },
    document: {
      getElementById(id) { return elements.get(id) || null; },
    },
    getCurrentStore() { return {id: 'richelieu'}; },
    sideDisplayLabel(side) { return side === 'Gauche' ? 'Côté A' : 'Côté B'; },
    safeHttpUrl(value) { return String(value).startsWith('https://') ? value : ''; },
    requireEditorSession() { return true; },
    resetCameraCandidate() {},
    resumeScanning() {},
    stopCamera: async () => {},
    esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, character => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
      })[character]);
    },
    async apiGetExpiryProduct() { return lookup; },
    async apiGetExpiryBoard() { return {items: [], summary: {}}; },
    async apiSetExpiryDate(payload) {
      savedPayload = payload;
      return {ok: true, status: 200, data: {current: {...current, earliest_expiry_date: payload.earliest_expiry_date, revision: 4}}};
    },
    async apiClearExpiryDate() { return {ok: true, status: 200, data: {success: true}}; },
    getSavedPayload: () => savedPayload,
    elements,
    lookup,
  };
  context.window = {
    confirm() { return true; },
    setTimeout(callback) { callback(); return 1; },
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  return context;
}

(async () => {
  const context = createContext();
  context.elements.get('expiryScanInput').value = '063848907665';
  await vm.runInContext('lookupExpiryFromInput(false)', context);

  const markup = context.elements.get('expiryLookupResult').innerHTML;
  assert(markup.includes('BIOMEDIC GEL ANALG GLACE 255G'));
  assert(markup.includes('2026-10-01'));
  assert(markup.includes('Allée Labo'));

  context.elements.get('expiryInitials').value = 'jl';
  context.elements.get('expiryDateInput').value = '2026-11-01';
  context.elements.get('expiryNoteInput').value = 'Présentoir avant';
  await vm.runInContext('saveCurrentExpiry()', context);

  const payload = context.getSavedPayload();
  assert.strictEqual(payload.store, 'richelieu');
  assert.strictEqual(payload.barcode, '063848907665');
  assert.strictEqual(payload.initials, 'JL');
  assert.strictEqual(payload.expected_revision, 3);
  assert.strictEqual(context.elements.get('expiryScanInput').value, '');
  assert(context.elements.get('expiryLookupResult').innerHTML.includes('Prêt pour le produit suivant'));

  vm.runInContext(`
    expiryBoardItems = [
      {gtin_key:'a', barcode:'1', product_name:'Dépassé', urgency:'expired', days_remaining:-2,
       earliest_expiry_date:'2026-08-10', checked_by:'AM', checked_at:'2026-08-12T12:00:00Z', locations:[]},
      {gtin_key:'b', barcode:'2', product_name:'Plus tard', urgency:'later', days_remaining:90,
       earliest_expiry_date:'2026-11-10', checked_by:'JL', checked_at:'2026-08-12T12:00:00Z', locations:[]}
    ];
  `, context);
  context.elements.get('expiryBoardFilter').value = 'expired';
  vm.runInContext('renderExpiryBoard()', context);
  const board = context.elements.get('expiryBoardList').innerHTML;
  assert(board.includes('Dépassé'));
  assert(!board.includes('Plus tard'));

  console.log('Expiry UI tests passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
