// ── App globals ──────────────────────────────────────────────────────────────
let allProductsCache = [];
let mapLayouts = [];
let searchTimer = null;
let clientSearchTimer = null;
let currentClientMatches = [];
let lastProductsRefreshAt = 0;
let lastLayoutsRefreshAt = 0;
let dirtyLayoutAisles = new Set();
let openPlanNodes = new Set();
let planStartDraft = null;
let currentScanProduct = null;
let pendingLookupProduct = null;
let pendingLookupAssist = null;
let lastLookedUpBarcode = '';
let activeLookupBarcode = '';

const STORAGE_KEYS = {
  cursor: 'familiprixCursor',
  activeTab: 'familiprixActiveTab',
  scanDraft: 'familiprixScanDraft',
  addDraft: 'familiprixAddDraft',
  clientDraft: 'familiprixClientDraft',
  editorSession: 'familiprixEditorSession'
};

const LOCK_HASH = 'VGFoaXJpYTIh';
const LOCK_TTL_MS = 4 * 60 * 60 * 1000; // 4 heures
const LOCKED_TABS = new Set(['scan', 'add']);

let backendInfo = {
  backend: 'sqlite',
  shared_sync: false,
  label: 'SQLite locale',
  needs_shared_database: true,
  ai_enabled: false,
  ai_provider: '',
  ai_provider_label: '',
  duplicate_slots: 0,
  duplicate_barcodes: 0
};

const cursor = {
  aisle: 1,
  facing: 'Avant',
  side: 'Gauche',
  section: 1,
  shelf: 1,
  position: 1,
  maxSection: 1,
  maxPosition: 8,
  maxShelf: 5
};

const SEARCH_STOPWORDS = new Set([
  'a','an','and','au','aux','avec','ce','ces','cette','client','comme',
  'dans','de','des','du','en','et','for','how','i','il','ils','je',
  'la','le','les','mais','mon','my','of','on','or','ou','par','pas',
  'pour','que','qui','sans','si','son','sur','the','to','un','une',
  'with','without','y'
]);

window.AppConfig = { STORAGE_KEYS, LOCK_HASH, LOCK_TTL_MS, LOCKED_TABS, SEARCH_STOPWORDS };
