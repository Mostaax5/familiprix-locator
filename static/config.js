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
  planSnapshot: 'familiprixPlanSnapshot',
  store: 'familiprixStore'
};

// Known stores. Add new ones here as they come online.
const STORES = [
  {
    id: 'richelieu',
    name: 'Familiprix Richelieu',
    address: '1111 Chemin des Patriotes, Richelieu, Quebec J3L 4W6'
  }
];

// Customer service stays immediately available. Only the two tabs that can
// change product locations or the store plan require a server-verified session.
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
  'la','le','les','mais','me','moi','mon','my','nous','of','on','or','ou','par','pas',
  'pour','que','qui','sans','si','son','sur','the','to','un','une',
  'with','without','y',
  // Filler words of a spoken client request — mirror of routes/products.py.
  'besoin','cherche','cherchez','chose','choses','conseil','conseillez',
  'contre','donner','faudrait','faut','madame','medicament','medicaments',
  'est','meilleur','meilleure','monsieur','peut','peux','plait','pourquoi','prendre',
  'produit','produits','quel','quelle','quels','quelles','quelque','quelques',
  'quoi','recommande',
  'recommandez','suggestion','svp','veut','veux','voudrais',
  // Request-shaping words affect presentation, not product retrieval.
  'all','available','avoir','avons','context','contexts','contexte','contextes',
  'difference','differences','different','differents','differentes','dire','dis','dit',
  'explain','explique','expliquer','flavor','flavors','flavour','flavours','gout','gouts',
  'have','laquelle','lesquelles','lequel','lesquels','magasin','montrer','montre','qu',
  'saveur','saveurs','show','sorte','sortes','store','te','tell','tout','tous','toute',
  'should','sont','toutes','tu','type','types','usage','usages','use','uses',
  'utiliser','vous','why','you'
]);

window.AppConfig = { STORAGE_KEYS, LOCKED_TABS, SEARCH_STOPWORDS, STORES };
