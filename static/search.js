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

function searchProductsFromCache(query, limit=40) {
  const variants = querySearchVariants(query);
  if (!variants.length) return [];
  const ranked = [];
  for (const product of allProductsCache) {
    let bestScore = 0;
    for (const variant of variants) bestScore = Math.max(bestScore, scoreProductForQuery(product, variant));
    if (bestScore > 0) ranked.push({score: bestScore, product});
  }
  ranked.sort((a, b) => (b.score - a.score) || String(a.product.name || '').localeCompare(String(b.product.name || '')));
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
    // Group results by barcode — if a barcode appears at multiple locations, merge them
    div.innerHTML = cached.length ? groupAndRenderSearchResults(cached) : '<div class="empty">Aucun produit trouve.</div>';
    return;
  }
  try {
    const data = await apiSearchProducts(q);
    div.innerHTML = data.length ? groupAndRenderSearchResults(data) : '<div class="empty">Aucun produit trouve.</div>';
  } catch (e) {
    div.innerHTML = '<div class="msg error">Impossible de rechercher pour le moment.</div>';
  }
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
