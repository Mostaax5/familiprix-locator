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
  editorSession: 'familiprixEditorSession',
  store: 'familiprixStore'
};

// Known stores. Add new ones here as they come online. `pass` is a simple
// "right store" confirmation code (not security) — currently '0'.
const STORES = [
  {
    id: 'richelieu',
    name: 'Familiprix Richelieu',
    address: '1111 Chemin des Patriotes, Richelieu, Quebec J3L 4W6',
    pass: '0'
  }
];

// SHA-256 of the access password (one-way hash — the password is NOT recoverable
// from this source, unlike the old base64 which was trivially reversible).
const LOCK_HASH = '1158a3823fa4014569a2b5f7f475a5539429ca8d6abcbab1d1cc7972470982e8';
const LOCK_TTL_MS = 8 * 60 * 60 * 1000; // 8 heures — un mot de passe par quart de
                                        // travail, FIXE depuis le déverrouillage
                                        // (jamais prolongé par l'activité)
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
  'with','without','y',
  // Filler words of a spoken client request — mirror of routes/products.py.
  'besoin','cherche','cherchez','chose','choses','conseil','conseillez',
  'contre','donner','faudrait','faut','madame','medicament','medicaments',
  'meilleur','meilleure','monsieur','peut','peux','plait','prendre',
  'produit','produits','quelque','quelques','quoi','recommande',
  'recommandez','suggestion','svp','veut','veux','voudrais'
]);

window.AppConfig = { STORAGE_KEYS, LOCK_HASH, LOCK_TTL_MS, LOCKED_TABS, SEARCH_STOPWORDS, STORES };
