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
  {triggers:['mal de tete','maux de tete','tete','migraine','cephalee','fievre','douleur','douleurs','courbature','courbatures','mal de dos','arthrite','menstruel','menstruelle','regles'],
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
  gouttes:['gtte','gttes','got'], goutte:['gtte','got'],
  pastille:['past'], pastilles:['past'], protection:['prot'],
  feminine:['fem'], feminin:['fem'], quotidien:['quot'],
  naturel:['nat'], naturels:['nat'], naturelle:['nat'],
  supplement:['suppl','supp'], supplements:['suppl','supp'],
  hydratant:['hydr','hyd'], hydratante:['hydr','hyd'],
  maquillage:['maq','maquill'], coloration:['color','col'], biberon:['bib'],
  serviette:['serv'], serviettes:['serv'], tampon:['tamp'], tampons:['tamp'],
};

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

function intentExpansionTerms(query) {
  const norm = normalizeSearchText(query);
  if (!norm) return [];
  const tokens = new Set(norm.split(' '));
  const terms = [], seen = new Set();
  for (const entry of INTENT_LEXICON) {
    const hit = entry.triggers.some(t => t.includes(' ') ? norm.includes(t) : tokens.has(t));
    if (hit) for (const term of entry.expand) { if (!seen.has(term)) { seen.add(term); terms.push(term); } }
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
    const haystack = [name, brand, description, searchTerms, usageNotes, alternatives].join(' ');
    // non-enumerable so it never gets copied into API payloads (e.g. {...product})
    Object.defineProperty(product, '_sf', {
      value: {name, brand, description, searchTerms, usageNotes, alternatives, barcode, haystack},
      enumerable: false, writable: true, configurable: true,
    });
  }
  return product._sf;
}

function productSearchText(product) {
  return productSearchFields(product).haystack;
}

function scoreProductForQuery(product, query) {
  const loweredQuery = normalizeSearchText(query);
  const digitsQuery = normalizedDigits(query);
  if (!loweredQuery && !digitsQuery) return 0;
  const f = productSearchFields(product);
  const {barcode, name, brand, description, searchTerms, usageNotes, alternatives, haystack} = f;
  let score = 0;
  if (digitsQuery && barcode) {
    if (barcode === digitsQuery) score += 1200;
    else if (digitsQuery.length >= 4 && barcode.endsWith(digitsQuery)) score += 900;
    else if (barcode.includes(digitsQuery)) score += 500;
  }
  if (loweredQuery === name) score += 800;
  else if (name.startsWith(loweredQuery)) score += 650;
  else if (loweredQuery && name.includes(loweredQuery)) score += 450;
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
function searchProductsFromCache(query, limit=40, minScore=0) {
  const variants = querySearchVariants(query);
  const intentTerms = intentExpansionTerms(query);
  const abbrevs = abbreviationTerms(query);
  if (!variants.length && !intentTerms.length) return [];
  const ranked = [];
  for (const product of allProductsCache) {
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

// Which field the search box targets: '' = name/brand/UPC (default), 'code' = pharmacy code.
function getSearchField() {
  return document.getElementById('searchField')?.value || '';
}

function onSearchFieldChange() {
  const input = document.getElementById('searchInput');
  if (input) {
    input.placeholder = getSearchField() === 'code'
      ? 'Code pharmacie (ex: 123456)…'
      : 'Nom, code-barres ou 4 derniers chiffres...';
  }
  doSearch();
}

// ── Search tab ────────────────────────────────────────────────────────────────
function filterByHomeBrand(brand) {
  const products = allProductsCache.filter(p => brand ? p.brand?.toLowerCase().startsWith(brand.toLowerCase()) : isHomeBrand(p.brand));
  const div = document.getElementById('searchResults');
  if (!products.length) {
    div.innerHTML = `<div class="empty">Aucun produit ${brand || 'marque maison'} cartographie pour le moment.</div>`;
    return;
  }
  const sorted = [...products].sort((a, b) => {
    const aKey = [a.aisle, a.side, a.section, a.shelf, a.position].join('-');
    const bKey = [b.aisle, b.side, b.section, b.shelf, b.position].join('-');
    return aKey.localeCompare(bKey);
  });
  div.innerHTML = `<div class="card"><div class="section-title">★ ${brand || 'Marques maison'} — ${sorted.length} produit${sorted.length > 1 ? 's' : ''} cartographie${sorted.length > 1 ? 's' : ''}</div>${sorted.map(p => productCard(p, false, false)).join('')}</div>`;
}

async function doSearch() {
  const q = document.getElementById('searchInput').value.trim();
  return doSearchValue(q);
}

function scheduleSearch() {
  window.clearTimeout(searchTimer);
  searchTimer = window.setTimeout(() => doSearch(), 180);
}

async function doSearchValue(q) {
  const div = document.getElementById('searchResults');
  if (!q) { div.innerHTML = ''; return; }

  // Explicit "Code" mode: match only the pharmacy code, never barcode/name.
  if (getSearchField() === 'code') {
    const cachedByCode = searchProductsByCodeFromCache(q, 40);
    if (cachedByCode.length || allProductsCache.length) {
      div.innerHTML = cachedByCode.length
        ? groupAndRenderSearchResults(cachedByCode)
        : '<div class="empty">Aucun produit avec ce code.</div>';
      return;
    }
    try {
      const data = await apiSearchProducts(q, 'code');
      div.innerHTML = data.length ? groupAndRenderSearchResults(data) : '<div class="empty">Aucun produit avec ce code.</div>';
    } catch (e) {
      div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
    }
    return;
  }

  if (looksLikeCompleteRetailBarcode(q)) {
    // Show ALL locations for this barcode from cache
    const byCodes = build_barcode_candidates_js(q);
    const allByBarcode = allProductsCache.filter(p => p.barcode && byCodes.includes(String(p.barcode).replace(/\s/g,'')));
    if (allByBarcode.length) {
      div.innerHTML = productCardMultiLocation(allByBarcode);
      return;
    }
    try {
      const product = await apiGetProductByBarcode(q);
      div.innerHTML = productCard(product, false);
      return;
    } catch (e) {
      if (e.status && e.status !== 404) {
        div.innerHTML = '<div class="msg error">Impossible de joindre la base pour le moment.</div>';
        return;
      }
    }
  }
  const cached = searchProductsFromCache(q, 40);
  if (cached.length || allProductsCache.length) {
    // A short digit query (last digits of a UPC) is ambiguous — several products can
    // share the same ending. Show every match with its full code so the user can pick.
    const shortDigits = /^\d{4,6}$/.test(q);
    const hint = (shortDigits && cached.length > 1)
      ? `<div class="msg info" style="margin-bottom:8px">${cached.length} produits se terminent par <b>${esc(q)}</b>. Vérifiez le code-barres complet ci-dessous pour choisir le bon.</div>`
      : '';
    // Group results by barcode — if a barcode appears at multiple locations, merge them
    div.innerHTML = cached.length ? (hint + groupAndRenderSearchResults(cached)) : '<div class="empty">Aucun produit placé. Recherche dans le catalogue…</div>';
    // Also search the imported-planogram catalogue for products we carry but that
    // aren't placed on a shelf yet (server-side, so it doesn't tax the phone).
    appendReferenceMatches(q, div, cached);
    return;
  }
  try {
    const data = await apiSearchProducts(q);
    div.innerHTML = data.length ? groupAndRenderSearchResults(data) : '<div class="empty">Aucun produit trouve.</div>';
  } catch (e) {
    div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
  }
}

// Fetch catalogue-only products (imported planograms, not placed yet) and append them
// below the placed results. Server-side search, so it's light on the device.
async function appendReferenceMatches(q, div, placed) {
  let ref = [];
  try { ref = await apiSearchReference(q, 30); } catch (_) {}
  const current = document.getElementById('searchInput')?.value.trim();
  if (current !== q) return;                      // user moved on — ignore stale results
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
}

function groupAndRenderSearchResults(products) {
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
  return groups.map(g => g.length > 1 ? productCardMultiLocation(g) : productCard(g[0])).join('');
}

function productCardMultiLocation(entries) {
  const primary = entries[0];
  const locBadges = entries.map(p => {
    return `<span style="display:inline-block;background:#fff0f0;color:#c8102e;border-radius:12px;padding:3px 9px;font-size:11px;font-weight:600;margin:2px">
      Allée ${esc(p.aisle)} · ${esc(sideDisplayLabel(p.side))} · S${esc(p.section||'1')} T${esc(p.shelf)} P${esc(p.position)}
    </span>`;
  }).join('');
  return `<div class="card">
    ${entries.some(p => isHomeBrand(p.brand)) ? `<div class="home-badge">★ Marque maison Familiprix</div>` : ''}
    <div class="product-layout">
      ${primary.image_url ? `<img class="product-thumb" src="${esc(primary.image_url)}" alt="">` : ''}
      <div class="product-info">
        <div class="name">${esc(primary.name)}</div>
        ${primary.brand ? `<div class="product-brand">${esc(primary.brand)}</div>` : ''}
        <div style="margin-top:4px;display:flex;flex-wrap:wrap;gap:2px">${locBadges}</div>
      </div>
    </div>
    <div class="product-footer">
      ${primary.barcode ? `<div class="meta-row"><span class="meta-label">Code-barres</span><span class="barcode-text">${esc(primary.barcode)}</span></div>` : ''}
      ${primary.description ? `<div class="desc-text">${esc(primary.description)}</div>` : ''}
      ${primary.usage_notes ? `<div class="desc-text">${esc(primary.usage_notes)}</div>` : ''}
    </div>
  </div>`;
}

window.AppSearch = { doSearch, doSearchValue, filterByHomeBrand, scheduleSearch, onSearchFieldChange };
