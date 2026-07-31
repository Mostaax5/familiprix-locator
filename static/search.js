// ── Search/scoring ─────────────────────────────────────────────────────────
function normalizedDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function tokenizeSearchQuery(query) {
  return normalizeSearchText(query)
    .split(/\s+/)
    .filter(token => token.length >= 2 && !SEARCH_STOPWORDS.has(token));
}

function querySearchVariants(query) {
  const variants = [];
  const seen = new Set();
  const add = value => {
    const cleaned = String(value || '').trim();
    if (!cleaned || seen.has(cleaned)) return;
    seen.add(cleaned);
    variants.push(cleaned);
  };
  const normalized = normalizeSearchText(query);
  const digits = normalizedDigits(query);
  const tokens = tokenizeSearchQuery(query);
  add(normalized);
  if (tokens.length) { add(tokens.join(' ')); tokens.forEach(add); }
  if (digits.length >= 4) add(digits);
  return variants;
}

// ── Intent lexicon (mirror of INTENT_LEXICON in routes/products.py) ──────────────
// Maps a customer's problem (how a client speaks) to the product/ingredient/brand
// words that appear in the store data, so a symptom query reaches the right products
// with no AI. Keep in sync with the server copy.
const INTENT_LEXICON = [
  {triggers:['mal de tete','mal a la tete','male a la tete','mal tete','maux de tete','maux tete','headache','migraine','cephalee','fievre','douleur','douleurs','courbature','courbatures','mal de dos','arthrite','menstruel','menstruelle','regles'],
   expand:['acetaminophene','tylenol','advil','motrin','ibuprofene','aspirine','analgesique','antidouleur','naproxene','aleve','atasol','tempra']},
  {triggers:['rhume','congestion','nez bouche','sinus','grippe','decongestionnant','mouchoir'],
   expand:['decongestionnant','rhume','sinus','sudafed','otrivin','tylenol rhume','advil rhume','dristan','vicks','sirop']},
  {triggers:['toux','gorge','mal de gorge','expectorant','enrouement'],
   expand:['sirop','toux','dextromethorphane','guaifenesine','benylin','buckley','gorge','strepsils','halls','fisherman']},
  {triggers:['allergie','allergies','urticaire','eternuement','rhinite','allergique'],
   expand:['antihistaminique','allergie','reactine','cetirizine','claritin','loratadine','aerius','benadryl','allegra','blexten']},
  {triggers:['brulure d estomac','brulures d estomac','reflux','acidite','indigestion','aigreur','estomac'],
   expand:['antiacide','tums','gaviscon','rolaids','omeprazole','pepto','famotidine','pantoloc']},
  {triggers:['constipation','diarrhee','nausee','ballonnement','ballonnements','gaz','crampes','mal de ventre','digestion'],
   expand:['laxatif','metamucil','senokot','imodium','gravol','probiotique','lax a day','restoralax','ovol','gaz','pepto']},
  {triggers:['vitamine','vitamines','supplement','supplements','fer','calcium','magnesium','multivitamine','immunite','fatigue','energie'],
   expand:['vitamine','multivitamine','centrum','jamieson','webber','fer','calcium','magnesium','vitamine d','vitamine c','zinc','omega','probiotique']},
  {triggers:['peau','eczema','secheresse','hydratant','creme','demangeaison','demangeaisons','piqure','piqures','brulure','coup de soleil','acne','psoriasis','feu sauvage'],
   expand:['creme','hydratant','cortisone','cortate','lubriderm','aveeno','cerave','calamine','polysporin','onguent','vaseline','abreva']},
  {triggers:['yeux','oeil','secheresse oculaire','conjonctivite','larmes','oculaire'],
   expand:['gouttes','yeux','larmes artificielles','visine','systane','collyre','refresh']},
  {triggers:['bebe','couche','couches','poussee dentaire','colique','coliques','erytheme fessier','biberon','nourrisson'],
   expand:['bebe','couche','pampers','huggies','tempra','tylenol bebe','penaten','creme fesses','lingette','ovol']},
  {triggers:['pansement','coupure','plaie','desinfectant','bandage','ampoule','echarde','eraflure','saignement'],
   expand:['pansement','band aid','polysporin','peroxyde','alcool','gaze','bandage','antiseptique','diachylon']},
  {triggers:['membrane transparente','membrane transparent','pansement transparent','film transparent','opsite','upsite','upside'],
   expand:['pansement transparent','film transparent','opsite','tegaderm','paramedic pans transp','transp']},
  {triggers:['watte','ouate','boule de coton','boules de coton','cotton balls'],
   expand:['ouate','boule coton','boules coton','coton','cotton']},
  {triggers:['sommeil','dormir','insomnie','stress','anxiete','relaxation','nervosite'],
   expand:['sommeil','melatonine','nytol','sleep','valeriane','unisom','tylenol nuit']},
];

// Mirror of SEARCH_ABBREVIATIONS in routes/products.py — full word -> planogram short form.
const SEARCH_ABBREVIATIONS = {
  shampoing:['shp','shampooing'], shampooing:['shp','shampoing'],
  revitalisant:['rev','revit','apres'], apres:['apres'],
  poudre:['pdre','pdr','pou'], sirop:['sir'],
  comprime:['co','compr','com'], comprimes:['co','compr','com'],
  capsule:['caps','gel'], capsules:['caps','gel'],
  creme:['cr','crm'], cremes:['cr','crm'], onguent:['ong'],
  lotion:['lot','lotn'], solution:['sol','soln'],
  decongestionnant:['decong','dec'], congestion:['decong','cong'],
  enfant:['enf'], enfants:['enf'], savon:['sav'], deodorant:['deo'],
  antisudorifique:['antisud','a sud'], dentifrice:['dent'],
  brosse:['bross','bro'], rasoir:['ras'], rasage:['ras'],
  vaporisateur:['vapo','vap'], nettoyant:['nett','net'],
  traitement:['trait','trmt'], vitamine:['vit'], vitamines:['vit'],
  gouttes:['gtte','gttes','got'], goutte:['gtte','got'], toux:['tx'],
  pastille:['past'], pastilles:['past'], protection:['prot'],
  feminine:['fem'], feminin:['fem'], quotidien:['quot'],
  naturel:['nat'], naturels:['nat'], naturelle:['nat'],
  supplement:['suppl','supp'], supplements:['suppl','supp'],
  hydratant:['hydr','hyd'], hydratante:['hydr','hyd'],
  maquillage:['maq','maquill'], coloration:['color','col'], biberon:['bib'],
  serviette:['serv'], serviettes:['serv'], tampon:['tamp'], tampons:['tamp'],
  transparent:['transp'], transparente:['transp'],
  charbon:['charb'], charcoal:['charb'],
};

const ELECTRIC_TOOTHBRUSH_EXPANSIONS = [
  'elec', 'pile', 'sonicare', 'philips one', 'tete br dent',
];

function abbreviationTerms(query) {
  const terms = [], seen = new Set();
  for (const token of tokenizeSearchQuery(query)) {
    for (const short of (SEARCH_ABBREVIATIONS[token] || [])) {
      if (!seen.has(short)) { seen.add(short); terms.push(short); }
    }
  }
  return terms;
}

function abbreviationHit(nameNorm, abbrevs) {
  if (!nameNorm || !abbrevs.length) return false;
  for (const t of nameNorm.split(' ')) {
    for (const a of abbrevs) {
      if (t === a || (t.startsWith(a) && /^\d+$/.test(t.slice(a.length)))) return true;
    }
  }
  return false;
}

function isElectricToothbrushRequest(query) {
  const tokens = new Set(normalizeSearchText(query).split(' ').filter(Boolean));
  const electric = [...tokens].some(token => token.startsWith('elect') || token === 'elec');
  const compound = tokens.has('toothbrush') || tokens.has('toothbrushes');
  const brush = compound || [...tokens].some(token => token.startsWith('bross') || token === 'brush');
  const tooth = compound || [...tokens].some(token => token.startsWith('dent') || token.startsWith('tooth'));
  return electric && brush && tooth;
}

const HEADACHE_PRODUCT_TERMS = [
  'acetaminophene', 'paracetamol', 'acet', 'tylenol', 'tempra', 'atasol',
  'ibuprofene', 'ibup', 'advil', 'motrin', 'naproxene', 'naprox', 'aleve',
  'aspirine', 'aspirin', 'aas', 'asa', 'analgesique', 'antidouleur',
  'pain reliever', 'pain relief', 'soul douleur', 'soul m tete', 'migraine',
];

function searchConceptMatches(text, terms) {
  const hayTokens = normalizeSearchText(text).split(/\s+/).filter(Boolean);
  return terms.some(term => {
    const expected = normalizeSearchText(term).split(/\s+/).filter(Boolean);
    if (!expected.length || expected.length > hayTokens.length) return false;
    for (let start = 0; start <= hayTokens.length - expected.length; start += 1) {
      if (expected.every((token, offset) => {
        const actual = hayTokens[start + offset];
        return actual === token || (token.length >= 4 && actual.startsWith(token));
      })) return true;
    }
    return false;
  });
}

function productMatchesHighPrecisionQuery(product, query) {
  const normalized = normalizeSearchText(query);
  const tokens = new Set(normalized.split(/\s+/).filter(Boolean));
  const hay = productSearchText(product);
  const name = `${product?.name || ''} ${product?.brand || ''}`;
  const headache = (
    ['headache', 'migraine', 'cephalee'].some(token => tokens.has(token))
    || (tokens.has('tete') && ['mal', 'male', 'maux'].some(token => tokens.has(token)))
  );
  const fever = ['fievre', 'fever', 'febrile'].some(token => tokens.has(token));
  if ((headache || fever) && !searchConceptMatches(name, HEADACHE_PRODUCT_TERMS)) {
    return false;
  }

  const cottonBalls = ['watte', 'ouate'].some(token => tokens.has(token)) || (
    ['coton', 'cotton'].some(token => tokens.has(token))
    && ['boule', 'boules', 'ball', 'balls'].some(token => tokens.has(token))
  );
  if (cottonBalls && !(
    searchConceptMatches(hay, ['coton', 'cotons', 'cotton', 'ouate', 'watte'])
    && searchConceptMatches(hay, ['boule', 'boules', 'ball', 'balls', 'ouate'])
  )) return false;

  const transparentDressing = [
    'membrane transparent', 'pansement transparent', 'film transparent',
  ].some(marker => normalized.includes(marker))
    || ['opsite', 'upsite', 'upside'].some(token => tokens.has(token));
  if (transparentDressing && !(
    searchConceptMatches(hay, ['transparent', 'transparente', 'transp', 'opsite', 'tegaderm'])
    && searchConceptMatches(hay, ['pansement', 'pans', 'diach', 'bandage', 'band aid', 'opsite', 'tegaderm'])
  )) return false;

  const oralCharcoal = ['charbon', 'charcoal', 'charb'].some(token => tokens.has(token))
    && [
      'pilule', 'pilules', 'capsule', 'capsules', 'gelule', 'gelules',
      'comprime', 'comprimes', 'tablet', 'tablets', 'caplet', 'caplets',
    ].some(token => tokens.has(token));
  if (oralCharcoal && !(
    searchConceptMatches(hay, ['charb', 'charcoal'])
    && (
      searchConceptMatches(hay, [
        'pilule', 'capsule', 'caps', 'gelule', 'comprime', 'tablet', 'caplet',
      ])
      || /(?:^| )(?:ca|co) ?\d+(?: |$)/.test(hay)
    )
  )) return false;

  if (isElectricToothbrushRequest(normalized)) {
    if (!(
      searchConceptMatches(hay, [
        'brosse dent', 'brosse dents', 'br dent', 'br dents', 'toothbrush',
        'rech bros', 'recharge bros', 'soni rech', 'tete br dent',
      ])
      && searchConceptMatches(hay, [
        'electrique', 'electric', 'elec', 'pile', 'sonicare', 'philips one',
        'tete br dent',
      ])
    )) return false;
    if (searchConceptMatches(name, [
      'irr', 'irrigateur', 'hydropulseur', 'airfloss', 'water flosser', 's fil',
    ])) return false;
  }
  return true;
}

function intentExpansionTerms(query) {
  const norm = normalizeSearchText(query);
  if (!norm) return [];
  const tokens = new Set(norm.split(' '));
  const terms = [], seen = new Set();
  for (const entry of INTENT_LEXICON) {
    const hit = entry.triggers.some(t => t.includes(' ') ? norm.includes(t) : tokens.has(t));
    if (hit) for (const term of entry.expand) { if (!seen.has(term)) { seen.add(term); terms.push(term); } }
  }
  if (isElectricToothbrushRequest(norm)) {
    for (const term of ELECTRIC_TOOTHBRUSH_EXPANSIONS) {
      if (!seen.has(term)) { seen.add(term); terms.push(term); }
    }
  }
  return terms;
}

// Normalized search fields, computed ONCE per product and cached on the object.
// The catalog is re-scored on every (debounced) keystroke and for each query
// variant — without this cache that re-ran ~8 regex normalizations per product
// every time, which pegged the CPU and heated the device while typing.
// The cache lives on the cached product object; upsertCachedProduct rebuilds the
// object (fresh normalizeProduct) so edits naturally invalidate it.
function productSearchFields(product) {
  if (!product._sf) {
    const name = normalizeSearchText(product.name);
    const brand = normalizeSearchText(product.brand);
    const description = normalizeSearchText(product.description);
    const searchTerms = normalizeSearchText(product.search_terms);
    const usageNotes = normalizeSearchText(product.usage_notes);
    const alternatives = normalizeSearchText(product.alternative_suggestions);
    const barcode = normalizedDigits(product.barcode);
    const rawIdentifiers = [
      ...(Array.isArray(product.identifiers) ? product.identifiers : []),
      ...(Array.isArray(product.regulatory_identifiers) ? product.regulatory_identifiers : []),
    ];
    const identifierValuesByType = {};
    const identifierValues = [];
    for (const identifier of rawIdentifiers) {
      const type = String(identifier?.type || '').trim().toUpperCase().replace(/-/g, '_');
      const value = normalizeSearchText(identifier?.value || '');
      if (!type || !value) continue;
      if (!identifierValues.includes(value)) identifierValues.push(value);
      if (!identifierValuesByType[type]) identifierValuesByType[type] = [];
      if (!identifierValuesByType[type].includes(value)) identifierValuesByType[type].push(value);
    }
    const regulatoryValues = ['DIN', 'NPN', 'DIN_HM']
      .flatMap(type => identifierValuesByType[type] || []);
    const haystack = [name, brand, description, searchTerms, usageNotes, alternatives,
      identifierValues.join(' ')].join(' ');
    const nameTokens = name ? name.split(' ') : [];
    // non-enumerable so it never gets copied into API payloads (e.g. {...product})
    Object.defineProperty(product, '_sf', {
      value: {name, brand, description, searchTerms, usageNotes, alternatives,
        barcode, regulatoryValues, identifierValues, identifierValuesByType,
        haystack, nameTokens},
      enumerable: false, writable: true, configurable: true,
    });
  }
  return product._sf;
}

function productSearchText(product) {
  return productSearchFields(product).haystack;
}

function productQueryRoleAdjustment(product, query) {
  if (!isElectricToothbrushRequest(query)) return 0;
  const name = productSearchFields(product).name;
  const replacement = [
    'tete br dent', 'tete dent', 'rech bros', 'recharge bros',
    'soni rech', 'refill', 'replacement head',
  ].some(marker => name.includes(marker));
  const poweredBrush = [
    'br dent', 'brosse dent', 'toothbrush',
  ].some(marker => name.includes(marker)) && [
    ' elec', ' pile', 'sonicare', 'philips one',
  ].some(marker => ` ${name}`.includes(marker));
  if (poweredBrush && !replacement) return 260;
  return replacement ? -40 : 0;
}

function scoreProductForQuery(product, query) {
  const loweredQuery = normalizeSearchText(query);
  const digitsQuery = normalizedDigits(query);
  if (!loweredQuery && !digitsQuery) return 0;
  const f = productSearchFields(product);
  const {barcode, regulatoryValues, name, brand, description, searchTerms, usageNotes, alternatives, haystack} = f;
  let score = 0;
  if (digitsQuery && barcode) {
    if (barcode === digitsQuery) score += 1200;
    else if (digitsQuery.length >= 4 && barcode.endsWith(digitsQuery)) score += 900;
    else if (barcode.includes(digitsQuery)) score += 500;
  }
  if (digitsQuery && regulatoryValues.includes(digitsQuery)) score += 1100;
  if (loweredQuery === name) score += 800;
  else if (name.startsWith(loweredQuery)) score += 650;
  else if (loweredQuery && name.includes(loweredQuery)) score += 450;
  else if (loweredQuery && !loweredQuery.includes(' ')) {
    // Planogram names are abbreviated: a name token that PREFIXES the query word
    // is that word abbreviated ('MELAT' ⊂ 'melatonine'). Mirror of the server rule.
    for (const tok of f.nameTokens) {
      if (tok.length >= 4 && loweredQuery.startsWith(tok)) { score += 440; break; }
    }
  }
  if (loweredQuery === brand) score += 300;
  else if (loweredQuery && brand.includes(loweredQuery)) score += 180;
  if (loweredQuery && description.includes(loweredQuery)) score += 150;
  if (loweredQuery && searchTerms.includes(loweredQuery)) score += 240;
  if (loweredQuery && usageNotes.includes(loweredQuery)) score += 170;
  if (loweredQuery && alternatives.includes(loweredQuery)) score += 120;
  const uniqueTokens = [...new Set(tokenizeSearchQuery(query))];
  if (uniqueTokens.length) {
    const matchedTokens = uniqueTokens.filter(token => haystack.includes(token)).length;
    if (matchedTokens === uniqueTokens.length) score += 100 + (20 * matchedTokens);
    else if (matchedTokens > 0) score += 25 * matchedTokens;
  }
  return score;
}

// minScore: 0 keeps every hit (Search tab — employees may type fragments);
// the Client tab passes 100 so partial-coverage-only noise never shows to a client.
let _barcodeIndexSource = null;
let _barcodeExactIndex = new Map();
let _barcodeSuffixIndex = new Map();

function invalidateProductSearchIndexes() {
  _barcodeIndexSource = null;
}

function addBarcodeIndexValue(index, key, product) {
  if (!key) return;
  const values = index.get(key);
  if (values) values.push(product);
  else index.set(key, [product]);
}

function ensureBarcodeSearchIndexes() {
  if (_barcodeIndexSource === allProductsCache) return;
  _barcodeExactIndex = new Map();
  _barcodeSuffixIndex = new Map();
  for (const product of allProductsCache) {
    const barcode = normalizedDigits(product.barcode);
    if (!barcode) continue;
    addBarcodeIndexValue(_barcodeExactIndex, barcode, product);
    for (let length = 4; length <= 6 && length <= barcode.length; length++) {
      addBarcodeIndexValue(_barcodeSuffixIndex, `${length}:${barcode.slice(-length)}`, product);
    }
  }
  _barcodeIndexSource = allProductsCache;
}

function barcodeExactVariants(digits) {
  const values = new Set([digits]);
  if (digits.length === 13 && digits.startsWith('0')) values.add(digits.slice(1));
  if (digits.length === 12) values.add(`0${digits}`);
  if (digits.length === 14 && digits.startsWith('00')) values.add(digits.slice(2));
  const stripped = digits.replace(/^0+/, '');
  if (stripped) {
    values.add(stripped);
    if (stripped.length === 12) values.add(`0${stripped}`);
  }
  return values;
}

function productsByBarcodeFromCache(query) {
  const digits = normalizedDigits(query);
  if (digits.length < 4) return [];
  ensureBarcodeSearchIndexes();
  if (digits.length <= 6) {
    return (_barcodeSuffixIndex.get(`${digits.length}:${digits}`) || []).slice();
  }
  const products = [];
  const seen = new Set();
  for (const variant of barcodeExactVariants(digits)) {
    for (const product of (_barcodeExactIndex.get(variant) || [])) {
      const key = product.id ?? product;
      if (!seen.has(key)) {
        seen.add(key);
        products.push(product);
      }
    }
  }
  return products;
}

function searchProductsFromCache(query, limit=40, minScore=0, predicate=null) {
  if (/^\d{4,}$/.test(String(query || '').trim())) {
    const barcodeMatches = productsByBarcodeFromCache(query);
    if (barcodeMatches.length) return barcodeMatches.slice(0, limit);
  }
  const variants = querySearchVariants(query);
  const intentTerms = intentExpansionTerms(query);
  const abbrevs = abbreviationTerms(query);
  if (!variants.length && !intentTerms.length) return [];
  const ranked = [];
  for (const product of allProductsCache) {
    if (typeof predicate === 'function' && !predicate(product)) continue;
    if (!productMatchesHighPrecisionQuery(product, query)) continue;
    let bestScore = 0;
    for (const variant of variants) bestScore = Math.max(bestScore, scoreProductForQuery(product, variant));
    if (intentTerms.length) {
      let intentHit = 0;
      for (const term of intentTerms) intentHit = Math.max(intentHit, scoreProductForQuery(product, term));
      // Capped so a symptom→category match never outranks a direct name/UPC match.
      bestScore = Math.max(bestScore, Math.min(intentHit, 300));
    }
    if (abbrevs.length && abbreviationHit(productSearchFields(product).name, abbrevs)) {
      bestScore = Math.max(bestScore, 430);   // full word matched an abbreviated name
    }
    bestScore += productQueryRoleAdjustment(product, query);
    if (bestScore > 0 && bestScore >= minScore) ranked.push({score: bestScore, product});
  }
  // Tiebreak: in-stock before ruptures, then by name.
  const outOf = p => (p.in_stock === 0 ? 1 : 0);
  ranked.sort((a, b) => (b.score - a.score)
    || (outOf(a.product) - outOf(b.product))
    || String(a.product.name || '').localeCompare(String(b.product.name || '')));
  return ranked.slice(0, limit).map(item => item.product);
}

// Search strictly on the Familiprix/pharmacy code — never on barcode or name —
// so this "Code" mode can never be confused with a UPC search.
function searchProductsByCodeFromCache(query, limit=40) {
  const needle = normalizedDigits(query) || normalizeSearchText(query);
  if (!needle) return [];
  const ranked = [];
  for (const product of allProductsCache) {
    const code = String(product.product_code || '').trim();
    if (!code) continue;
    const haystack = normalizedDigits(code) || normalizeSearchText(code);
    if (!haystack) continue;
    let score = 0;
    if (haystack === needle) score = 1000;
    else if (haystack.startsWith(needle)) score = 700;
    else if (haystack.includes(needle)) score = 400;
    if (score) ranked.push({score, product});
  }
  ranked.sort((a, b) => (b.score - a.score) || String(a.product.name || '').localeCompare(String(b.product.name || '')));
  return ranked.slice(0, limit).map(item => item.product);
}

const IDENTIFIER_FIELD_TYPES = {
  din: ['DIN'], npn: ['NPN'], din_hm: ['DIN_HM'],
  pin: ['PIN'], nip: ['NIP'], pseudo_din: ['PSEUDO_DIN'],
  manufacturer_part_number: ['MANUFACTURER_PART_NUMBER'],
  supplier_item_number: ['SUPPLIER_ITEM_NUMBER'],
  wholesaler_item_number: ['WHOLESALER_ITEM_NUMBER'],
  case_gtin: ['CASE_GTIN'], inner_gtin: ['INNER_GTIN'],
  ramq_billing_code: ['RAMQ_BILLING_CODE'],
  insurer_billing_code: ['INSURER_BILLING_CODE'],
  health_canada_id: ['HEALTH_CANADA_ID'], clinical_id: ['CLINICAL_ID'],
};

function strictSearchValues(product, field) {
  const f = productSearchFields(product);
  if (field === 'name') return [f.name, f.brand].filter(Boolean);
  if (field === 'upc' || field === 'gtin') {
    return [f.barcode, ...(f.identifierValuesByType.UPC || []),
      ...(f.identifierValuesByType.GTIN || [])].filter(Boolean);
  }
  if (field === 'code' || field === 'familiprix_code') {
    return [normalizeSearchText(product.product_code),
      ...(f.identifierValuesByType.FAMILIPRIX_CODE || [])].filter(Boolean);
  }
  if (field === 'identifier' || field === 'all_identifiers') {
    return [f.barcode, normalizeSearchText(product.product_code),
      ...f.identifierValues].filter(Boolean);
  }
  const types = IDENTIFIER_FIELD_TYPES[field] || [];
  return types.flatMap(type => f.identifierValuesByType[type] || []);
}

function strictSearchScore(value, query) {
  const rawQuery = String(query || '').trim();
  const numeric = /^[\d\s.\-]+$/.test(rawQuery);
  const needle = numeric ? normalizedDigits(rawQuery) : normalizeSearchText(rawQuery);
  const haystack = numeric ? normalizedDigits(value) : normalizeSearchText(value);
  if (!needle || !haystack) return 0;
  if (haystack === needle) return 1200;
  if (numeric && needle.length >= 4 && haystack.endsWith(needle)) return 900;
  if (haystack.startsWith(needle)) return 700;
  if (haystack.includes(needle)) return 400;
  return 0;
}

function searchProductsByFieldFromCache(query, field, limit=40) {
  if (!field) return searchProductsFromCache(query, limit);
  const ranked = [];
  for (const product of allProductsCache) {
    let score = 0;
    for (const value of strictSearchValues(product, field)) {
      score = Math.max(score, strictSearchScore(value, query));
    }
    if (score) ranked.push({score, product});
  }
  ranked.sort((a, b) => (b.score - a.score)
    || String(a.product.name || '').localeCompare(String(b.product.name || '')));
  return ranked.slice(0, limit).map(item => item.product);
}

function mergeIndexedSearchResults(indexed, cached, limit=40) {
  const merged = new Map();
  const keyFor = product => {
    if (product?.id !== undefined && product?.id !== null) return `id:${product.id}`;
    return [
      String(product?.barcode || ''), String(product?.aisle || ''),
      String(product?.side || ''), String(product?.section || ''),
      String(product?.shelf || ''), String(product?.position || ''),
      String(product?.name || ''),
    ].join('|');
  };
  // Indexed rows contain the newest identifier links and review status. Cached
  // rows still give the employee an immediate first paint while the request runs.
  for (const product of [...(indexed || []), ...(cached || [])]) {
    const key = keyFor(product);
    if (!merged.has(key)) merged.set(key, product);
  }
  return [...merged.values()].slice(0, limit);
}

// Which field the search box targets. Empty means the broad employee search.
function getSearchField() {
  return document.getElementById('searchField')?.value || '';
}

function onSearchFieldChange() {
  const input = document.getElementById('searchInput');
  if (input) {
    const placeholders = {
      name: 'Nom ou marque du produit…', upc: 'UPC / GTIN ou derniers chiffres…',
      code: 'Code Familiprix…', identifier: 'N’importe quel identifiant…',
      din: 'DIN (8 chiffres)…', npn: 'NPN (8 chiffres)…',
      din_hm: 'DIN-HM (8 chiffres)…', pin: 'PIN…', nip: 'NIP…',
      pseudo_din: 'Pseudo-DIN…', manufacturer_part_number: 'Numéro du fabricant…',
      supplier_item_number: 'Numéro du fournisseur…',
      wholesaler_item_number: 'Numéro du grossiste…', case_gtin: 'GTIN de caisse…',
      inner_gtin: 'GTIN de l’emballage intérieur…', ramq_billing_code: 'Code RAMQ…',
      insurer_billing_code: 'Code assureur…', health_canada_id: 'ID Santé Canada…',
      clinical_id: 'Identifiant clinique…',
    };
    input.placeholder = placeholders[getSearchField()]
      || 'Nom, identifiant ou derniers chiffres…';
  }
  doSearch();
}

// ── Search tab ────────────────────────────────────────────────────────────────
let _searchImagePollTimer = null;
let _searchImagePollGeneration = 0;
let _referenceImagePollTimer = null;
let _referenceImagePollGeneration = 0;
let _searchRequestGeneration = 0;

function cancelSearchImagePolling() {
  _searchImagePollGeneration += 1;
  window.clearTimeout(_searchImagePollTimer);
  _searchImagePollTimer = null;
}

function cancelReferenceImagePolling() {
  _referenceImagePollGeneration += 1;
  window.clearTimeout(_referenceImagePollTimer);
  _referenceImagePollTimer = null;
}

function startSearchImagePolling(products) {
  cancelSearchImagePolling();
  const ids = [...new Set((products || [])
    .filter(product => product?.id && !product.image_url)
    .map(product => Number(product.id))
    .filter(Number.isInteger))].slice(0, 12);
  if (!ids.length || typeof apiGetProductImages !== 'function') return;

  const generation = _searchImagePollGeneration;
  const pending = new Set(ids);
  const poll = async attempt => {
    if (generation !== _searchImagePollGeneration) return;
    const data = await apiGetProductImages(ids);
    if (generation !== _searchImagePollGeneration) return;
    const images = data?.images || {};
    for (const [rawId, imageUrl] of Object.entries(images)) {
      const id = Number(rawId);
      if (!imageUrl || !Number.isInteger(id)) continue;
      pending.delete(id);
      for (const product of allProductsCache) {
        if (Number(product.id) === id && !product.image_url) product.image_url = imageUrl;
      }
      document.querySelectorAll(`[data-product-image-id="${id}"]`).forEach(placeholder => {
        const img = document.createElement('img');
        img.className = 'product-thumb';
        img.src = imageUrl;
        img.alt = 'Image produit';
        img.loading = 'lazy';
        img.decoding = 'async';
        img.onerror = () => img.remove();
        placeholder.replaceWith(img);
      });
    }
    if (pending.size && attempt < 9) {
      _searchImagePollTimer = window.setTimeout(() => poll(attempt + 1), 3000);
    }
  };
  poll(0);
}

function startReferenceImagePolling(products) {
  cancelReferenceImagePolling();
  const visibleProducts = Array.isArray(products) ? products : [];
  const barcodes = [...new Set(visibleProducts
    .filter(product => product?.catalog_only && !product.image_url)
    .map(product => normalizedDigits(product.barcode))
    .filter(Boolean))].slice(0, 12);
  if (!barcodes.length || typeof apiGetReferenceProductImages !== 'function') return;

  const generation = _referenceImagePollGeneration;
  const pending = new Set(barcodes);
  const poll = async attempt => {
    if (generation !== _referenceImagePollGeneration) return;
    const data = await apiGetReferenceProductImages(barcodes);
    if (generation !== _referenceImagePollGeneration) return;
    const images = data?.images || {};
    for (const [rawBarcode, imageUrl] of Object.entries(images)) {
      const barcode = normalizedDigits(rawBarcode);
      if (!barcode || !imageUrl) continue;
      pending.delete(barcode);
      for (const product of visibleProducts) {
        if (normalizedDigits(product.barcode) === barcode && !product.image_url) {
          product.image_url = imageUrl;
        }
      }
      document.querySelectorAll(`[data-reference-image-barcode="${barcode}"]`).forEach(placeholder => {
        const img = document.createElement('img');
        img.className = 'product-thumb';
        img.src = imageUrl;
        img.alt = 'Image produit';
        img.loading = 'lazy';
        img.decoding = 'async';
        img.onerror = () => img.remove();
        placeholder.replaceWith(img);
      });
    }
    if (pending.size && attempt < 9) {
      _referenceImagePollTimer = window.setTimeout(() => poll(attempt + 1), 3000);
    }
  };
  poll(0);
}

function filterByHomeBrand(brand) {
  const products = allProductsCache.filter(p => brand ? p.brand?.toLowerCase().startsWith(brand.toLowerCase()) : isHomeBrand(p.brand));
  const div = document.getElementById('searchResults');
  if (!products.length) {
    div.innerHTML = `<div class="empty">Aucun produit ${esc(brand || 'marque maison')} cartographie pour le moment.</div>`;
    return;
  }
  const sorted = [...products].sort((a, b) => {
    const aKey = [a.aisle, a.side, a.section, a.shelf, a.position].join('-');
    const bKey = [b.aisle, b.side, b.section, b.shelf, b.position].join('-');
    return aKey.localeCompare(bKey);
  });
  div.innerHTML = `<div class="card"><div class="section-title">★ ${esc(brand || 'Marques maison')} — ${sorted.length} produit${sorted.length > 1 ? 's' : ''} cartographie${sorted.length > 1 ? 's' : ''}</div>${sorted.map(p => productCard(p, false, false)).join('')}</div>`;
  startSearchImagePolling(sorted);
}

async function doSearch() {
  const q = document.getElementById('searchInput').value.trim();
  return doSearchValue(q);
}

function scheduleSearch() {
  window.clearTimeout(searchTimer);
  searchTimer = window.setTimeout(() => doSearch(), 70);
}

async function doSearchValue(q) {
  const requestGeneration = ++_searchRequestGeneration;
  const div = document.getElementById('searchResults');
  if (!q) {
    cancelSearchImagePolling();
    cancelReferenceImagePolling();
    div.innerHTML = '';
    return;
  }
  cancelReferenceImagePolling();

  const field = getSearchField();
  if (field) {
    const cachedByField = searchProductsByFieldFromCache(q, field, 40);
    div.innerHTML = cachedByField.length
      ? groupAndRenderSearchResults(cachedByField)
      : '<div class="empty">Recherche des identifiants du magasin…</div>';
    try {
      const indexed = await apiSearchProducts(q, field);
      if (requestGeneration !== _searchRequestGeneration) return;
      const data = mergeIndexedSearchResults(indexed, cachedByField, 40);
      div.innerHTML = data.length
        ? groupAndRenderSearchResults(data)
        : '<div class="empty">Aucun produit placé. Recherche dans le catalogue…</div>';
      appendReferenceMatches(q, div, data, field);
    } catch (e) {
      if (requestGeneration !== _searchRequestGeneration) return;
      if (cachedByField.length) {
        appendReferenceMatches(q, div, cachedByField, field);
      } else {
        div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
      }
    }
    return;
  }

  if (looksLikeCompleteRetailBarcode(q)) {
    // Show ALL locations for this barcode from cache
    const allByBarcode = productsByBarcodeFromCache(q);
    if (allByBarcode.length) {
      div.innerHTML = productCardMultiLocation(allByBarcode);
      startSearchImagePolling(allByBarcode);
      return;
    }
    try {
      const product = await apiGetProductByBarcode(q);
      div.innerHTML = productCard(product, false);
      startSearchImagePolling([product]);
      return;
    } catch (e) {
      if (e.status && e.status !== 404) {
        div.innerHTML = '<div class="msg error">Impossible de joindre la base pour le moment.</div>';
        return;
      }
    }
  }
  const cached = searchProductsFromCache(q, 40);
  const identifierLikeQuery = /^[\d\s.\-]+$/.test(q)
    && normalizedDigits(q).length >= 7
    && normalizedDigits(q).length <= 18;
  if (identifierLikeQuery) {
    div.innerHTML = cached.length
      ? groupAndRenderSearchResults(cached)
      : '<div class="empty">Recherche des identifiants du magasin…</div>';
    try {
      const indexed = await apiSearchProducts(q, 'identifier');
      if (requestGeneration !== _searchRequestGeneration) return;
      const data = mergeIndexedSearchResults(indexed, cached, 40);
      div.innerHTML = data.length
        ? groupAndRenderSearchResults(data)
        : '<div class="empty">Aucun produit placé. Recherche dans le catalogue…</div>';
      appendReferenceMatches(q, div, data);
    } catch (e) {
      if (requestGeneration !== _searchRequestGeneration) return;
      if (cached.length) appendReferenceMatches(q, div, cached);
      else div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
    }
    return;
  }
  const shortDigits = /^\d{4,6}$/.test(q);
  const renderPlaced = products => {
    const hint = (shortDigits && products.length > 1)
      ? `<div class="msg info" style="margin-bottom:8px">${products.length} produits se terminent par <b>${esc(q)}</b>. Vérifiez le code-barres complet ci-dessous pour choisir le bon.</div>`
      : '';
    div.innerHTML = products.length
      ? (hint + groupAndRenderSearchResults(products))
      : '<div class="empty">Aucun produit placé. Recherche dans le catalogue…</div>';
  };
  if (cached.length) {
    // Instant first paint from the phone snapshot, followed by an authoritative
    // server reconciliation below. A partial snapshot must never become a false
    // “aucun produit” result after a restart or refresh.
    renderPlaced(cached);
  } else {
    div.innerHTML = '<div class="empty">Recherche dans le plan actuel…</div>';
  }
  try {
    const indexed = await apiSearchProducts(q);
    if (requestGeneration !== _searchRequestGeneration) return;
    const data = mergeIndexedSearchResults(indexed, cached, 40);
    renderPlaced(data);
    appendReferenceMatches(q, div, data);
  } catch (e) {
    if (requestGeneration !== _searchRequestGeneration) return;
    if (cached.length) {
      appendReferenceMatches(q, div, cached);
    } else {
      div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
    }
  }
}

// Fetch catalogue-only products (imported planograms, not placed yet) and append them
// below the placed results. Server-side search, so it's light on the device.
async function appendReferenceMatches(q, div, placed, field='') {
  let ref = [];
  try { ref = await apiSearchReference(q, 30, field); } catch (_) {}
  const current = document.getElementById('searchInput')?.value.trim();
  if (current !== q || getSearchField() !== field) return;
  if (!ref.length) {
    if (!placed.length) div.innerHTML = '<div class="empty">Aucun produit trouvé.</div>';
    return;
  }
  const html = `<div class="card" style="margin-top:10px">
    <div class="section-title">📦 Aussi en magasin — position à confirmer</div>
    <div class="section-note">Produits importés des planogrammes, pas encore placés sur le plan.</div>
    ${ref.map(p => productCard(p, false, false)).join('')}
  </div>`;
  if (!placed.length) div.innerHTML = html;       // replace the "searching…" placeholder
  else div.insertAdjacentHTML('beforeend', html);
  startReferenceImagePolling(ref);
}

function groupAndRenderSearchResults(products) {
  startSearchImagePolling(products);
  // Group products by barcode; products without barcode are shown individually
  const groups = [];
  const seenBarcodes = new Set();
  for (const p of products) {
    const bc = String(p.barcode || '').trim();
    if (!bc) { groups.push([p]); continue; }
    if (seenBarcodes.has(bc)) continue;
    seenBarcodes.add(bc);
    // Find all entries with same barcode in the result set
    const group = products.filter(x => String(x.barcode || '').trim() === bc);
    groups.push(group);
  }
  return groups.map((g, index) => g.length > 1
    ? productCardMultiLocation(g, index < 3)
    : productCard(g[0], true, true, index < 3)).join('');
}

function productCardMultiLocation(entries, imagePriority=false) {
  const primary = entries[0];
  const locBadges = entries.map(p => {
    return `<span style="display:inline-block;background:#fff0f0;color:#c8102e;border-radius:12px;padding:3px 9px;font-size:11px;font-weight:600;margin:2px">
      Allée ${esc(p.aisle)} · ${esc(sideDisplayLabel(p.side))} · S${esc(p.section||'1')} T${esc(p.shelf)} P${esc(p.position)}
    </span>`;
  }).join('');
  return `<div class="card">
    ${entries.some(p => isHomeBrand(p.brand)) ? `<div class="home-badge">★ Marque maison Familiprix</div>` : ''}
    <div class="product-layout">
      ${primary.image_url
        ? `<img class="product-thumb" src="${esc(primary.image_url)}" alt="Image produit" loading="${imagePriority ? 'eager' : 'lazy'}" decoding="async" fetchpriority="${imagePriority ? 'high' : 'low'}">`
        : (primary.id ? `<span class="product-thumb product-thumb-placeholder" data-product-image-id="${Number(primary.id)}" aria-label="Photo en attente"></span>` : '')}
      <div class="product-info">
        <div class="name">${esc(primary.name)}</div>
        ${primary.brand ? `<div class="product-brand">${esc(primary.brand)}</div>` : ''}
        <div style="margin-top:4px;display:flex;flex-wrap:wrap;gap:2px">${locBadges}</div>
      </div>
    </div>
    <div class="product-footer">
      ${primary.barcode ? `<div class="meta-row"><span class="meta-label">Code-barres</span><span class="barcode-text">${esc(primary.barcode)}</span></div>` : ''}
      ${typeof regulatoryIdentifiersMarkup === 'function' ? regulatoryIdentifiersMarkup(primary) : ''}
      ${primary.description ? `<div class="desc-text">${esc(primary.description)}</div>` : ''}
      ${primary.usage_notes ? `<div class="desc-text">${esc(primary.usage_notes)}</div>` : ''}
    </div>
  </div>`;
}

window.AppSearch = {
  doSearch, doSearchValue, filterByHomeBrand, scheduleSearch, onSearchFieldChange,
  searchProductsFromCache, searchProductsByFieldFromCache, mergeIndexedSearchResults,
  productsByBarcodeFromCache, invalidateProductSearchIndexes,
  startSearchImagePolling, cancelSearchImagePolling,
  startReferenceImagePolling, cancelReferenceImagePolling,
};
