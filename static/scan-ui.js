// ── Rayon context ─────────────────────────────────────────────────────────────
let rayonCtx = {aisle:'', side:'Gauche', section:'1', shelf:''};

// Fill the allée autocomplete with every existing allée (numbers + names), so
// the user can pick the right one instead of retyping it (and avoid typos like
// "tet" never matching "test"). Names sort after numbers.
function populateRayonAisleList() {
  const dl = document.getElementById('rayonAisleList');
  if (!dl) return;
  const aisles = mapLayouts.map(l => String(l.aisle))
    .sort((a, b) => (Number(a) || 1e9) - (Number(b) || 1e9) || a.localeCompare(b));
  dl.innerHTML = aisles.map(a => `<option value="${esc(a)}"></option>`).join('');
}

function updateRayonSideOptions() {
  populateRayonAisleList();
  const aisle = (document.getElementById('rayonAisle')?.value || '').trim();
  const select = document.getElementById('rayonSide');
  if (!select) return;
  const current = select.value;
  const config = aisle ? getAisleLayoutConfig(aisle) : null;
  let html = '<option value="Gauche">Côté A</option><option value="Droite">Côté B</option>';
  // Façades (end caps)
  if ((config?.facade_a?.shelves || []).length > 0) html += '<option value="Façade A">🔲 Façade A</option>';
  if ((config?.facade_b?.shelves || []).length > 0) html += '<option value="Façade B">🔲 Façade B</option>';
  // Présentoirs — each façade is selectable
  (config?.presentoirs || []).forEach(p => {
    (p.facades || []).forEach(f => {
      const sv = `${p.name} - ${f.name}`;
      html += `<option value="${esc(sv)}">📦 ${esc(p.name)} · ${esc(f.name)}</option>`;
    });
  });
  select.innerHTML = html;
  if ([...select.options].some(o => o.value === current)) select.value = current;
}

function updateRayonCtx() {
  rayonCtx.aisle   = (document.getElementById('rayonAisle')?.value   || '').trim();
  rayonCtx.side    =  document.getElementById('rayonSide')?.value    || 'Gauche';
  rayonCtx.section = (document.getElementById('rayonSection')?.value || '1').trim() || '1';
  rayonCtx.shelf   = (document.getElementById('rayonShelf')?.value   || '').trim();
  const badge = document.getElementById('rayonBadge');
  if (!badge) return;
  if (rayonCtx.aisle && rayonCtx.shelf) {
    badge.style.background = '#fff0f0';
    badge.style.color = '#c8102e';
    const shelfLabel = getShelfLabel(rayonCtx.aisle, rayonCtx.side, rayonCtx.section, rayonCtx.shelf);
    const isLibreShelf = _isLibreShelf(rayonCtx.aisle, rayonCtx.side, rayonCtx.section, rayonCtx.shelf);
    const labelSuffix = shelfLabel ? ` — <em style="color:${isLibreShelf?'#8b5cf6':'#92400e'}">${isLibreShelf?'📦':'📎'} ${esc(shelfLabel)}</em>` : (isLibreShelf ? ' — <em style="color:#8b5cf6">📦 Mode libre</em>' : '');
    const nextPos = isLibreShelf ? '' : ` <span style="background:#c8102e;color:#fff;border-radius:6px;padding:2px 8px;font-size:13px;font-weight:800;margin-left:6px">P${nextRayonPosition()}</span>`;
    badge.innerHTML = `📍 <strong>Allée ${esc(rayonCtx.aisle)}</strong> — ${esc(rayonLabel().split(' — ').slice(1).join(' — '))}${labelSuffix}${nextPos}`;
    refreshRayonList();
  } else {
    badge.style.background = '#f8fafc';
    badge.style.color = '#94a3b8';
    badge.textContent = 'Choisissez une allée et une tablette pour commencer';
    document.getElementById('rayonListCard').style.display = 'none';
  }
}

function rayonLabel() {
  if (!rayonCtx.aisle || !rayonCtx.shelf) return 'emplacement non défini';
  const sideDisplay = rayonCtx.side === 'Gauche' ? 'Côté A' : rayonCtx.side === 'Droite' ? 'Côté B' : rayonCtx.side;
  const sectionPart = (rayonCtx.side === 'Gauche' || rayonCtx.side === 'Droite') ? ` — Section ${rayonCtx.section}` : '';
  return `Allée ${rayonCtx.aisle} — ${sideDisplay}${sectionPart} — Tablette ${rayonCtx.shelf}`;
}

// Open the Scan tab pre-aimed at an exact slot — works for côté sections,
// accroches, façades and présentoirs. Position auto-starts at the next free spot.
function startScanAt(aisle, side, section, shelf) {
  if (typeof requireEditorSession === 'function' && !requireEditorSession('scanner')) return;
  if (typeof switchTab === 'function') switchTab('scan');
  const aEl = document.getElementById('rayonAisle'); if (aEl) aEl.value = String(aisle);
  updateRayonSideOptions();   // rebuild côté/façade/présentoir options for this allée
  const sEl  = document.getElementById('rayonSide');    if (sEl)  sEl.value  = String(side);
  const secEl = document.getElementById('rayonSection'); if (secEl) secEl.value = String(section || '1');
  const shEl  = document.getElementById('rayonShelf');   if (shEl)  shEl.value  = String(shelf);
  updateRayonCtx();
}

function nextRayonPosition() {
  const {aisle, side, section, shelf} = rayonCtx;
  if (!aisle || !shelf) return '1';
  const taken = allProductsCache
    .filter(p => String(p.aisle) === aisle && p.side === side &&
                 String(p.section || '1') === section && String(p.shelf) === shelf)
    .map(p => parseInt(p.position) || 0);
  return taken.length ? String(Math.max(...taken) + 1) : '1';
}

// Number of fixed positions on the current tablette (0 = mode libre / illimité).
function rayonShelfPositionCount() {
  const {aisle, side, section, shelf} = rayonCtx;
  const config = getAisleLayoutConfig(aisle);
  const ti = parseInt(shelf) - 1;
  if (side === 'Gauche' || side === 'Droite')
    return Number(config?.sides?.[side]?.sections?.[parseInt(section) - 1]?.shelves?.[ti]) || 0;
  if (side === 'Façade A') return Number(config?.facade_a?.shelves?.[ti]) || 0;
  if (side === 'Façade B') return Number(config?.facade_b?.shelves?.[ti]) || 0;
  for (const pres of (config?.presentoirs || []))
    for (const f of (pres.facades || []))
      if (side === `${pres.name} - ${f.name}`) return Number(f.shelves?.[ti]) || 0;
  return 0;
}

// The next tablette to scan after the current one is full, following the plan:
// next tablette in the section, then next section, then (Côté A → Côté B).
// Returns {side, section, shelf} or null at the end of the plan.
function nextRayonShelf() {
  const {aisle, side, section, shelf} = rayonCtx;
  const config = getAisleLayoutConfig(aisle);
  const ti = parseInt(shelf) - 1;
  if (side === 'Gauche' || side === 'Droite') {
    const sections = config?.sides?.[side]?.sections || [];
    const si = parseInt(section) - 1;
    if (sections[si] && ti + 1 < sections[si].shelves.length)
      return {side, section: String(si + 1), shelf: String(ti + 2)};
    for (let ns = si + 1; ns < sections.length; ns++)
      if ((sections[ns].shelves || []).length) return {side, section: String(ns + 1), shelf: '1'};
    if (side === 'Gauche') {                 // roll over to Côté B
      const right = config?.sides?.Droite?.sections || [];
      for (let ns = 0; ns < right.length; ns++)
        if ((right[ns].shelves || []).length) return {side: 'Droite', section: String(ns + 1), shelf: '1'};
    }
    return null;
  }
  // Flat fixtures (façade / présentoir): just advance to the next tablette.
  let shelves = [];
  if (side === 'Façade A') shelves = config?.facade_a?.shelves || [];
  else if (side === 'Façade B') shelves = config?.facade_b?.shelves || [];
  else for (const pres of (config?.presentoirs || []))
    for (const f of (pres.facades || []))
      if (side === `${pres.name} - ${f.name}`) shelves = f.shelves || [];
  if (ti + 1 < shelves.length) return {side, section: '1', shelf: String(ti + 2)};
  return null;
}

// After a scan fills the LAST position of a fixed-count tablette, jump the rayon
// to the next tablette/section automatically so mapping flows without manual
// changes. No-op on libre shelves or at the end of the plan. Returns true if it
// moved (and then it has refreshed the badge/list).
function maybeAdvanceRayon(filledPos) {
  const count = rayonShelfPositionCount();
  if (count <= 0) return false;                          // libre = unlimited, stay
  if ((parseInt(filledPos) || 0) < count) return false;  // tablette not full yet
  const next = nextRayonShelf();
  if (!next) return false;                               // end of plan — stay put
  if (next.side !== rayonCtx.side) {
    const s = document.getElementById('rayonSide');
    if (s) s.value = next.side;
  }
  const secEl = document.getElementById('rayonSection'); if (secEl) secEl.value = next.section;
  const shEl  = document.getElementById('rayonShelf');   if (shEl)  shEl.value  = next.shelf;
  updateRayonCtx();   // syncs rayonCtx, badge and list to the new tablette
  const res = document.getElementById('scanResult');
  if (res) res.insertAdjacentHTML('beforeend',
    `<div class="msg info" style="margin-top:6px">➡ Tablette pleine — passage à <strong>${esc(rayonLabel().split(' — ').slice(1).join(' — '))}</strong></div>`);
  return true;
}

function refreshRayonList() {
  const {aisle, side, section, shelf} = rayonCtx;
  if (!aisle || !shelf) return;
  const products = allProductsCache
    .filter(p => String(p.aisle) === aisle && p.side === side &&
                 String(p.section || '1') === section && String(p.shelf) === shelf)
    .sort((a, b) => (parseInt(a.position) || 0) - (parseInt(b.position) || 0));
  const card  = document.getElementById('rayonListCard');
  const list  = document.getElementById('rayonProductList');
  const count = document.getElementById('rayonCount');
  if (!card) return;
  card.style.display = '';
  count.textContent = products.length;
  if (!products.length) {
    list.innerHTML = '<div class="empty">Aucun produit scanné sur ce rayon.</div>';
    return;
  }
  list.innerHTML = products.map(p => `
    <div style="display:flex;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid #f1f5f9">
      <div style="flex:none;width:30px;height:30px;background:#f1f5f9;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#64748b">P${esc(p.position)}</div>
      <div style="flex:1;min-width:0">
        <div style="font-weight:600;font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(p.name)}</div>
        ${p.brand ? `<div style="font-size:11px;color:#64748b">${esc(p.brand)}</div>` : ''}
        ${p.barcode ? `<div style="font-size:10px;color:#94a3b8;font-family:monospace">${esc(p.barcode)}</div>` : ''}
      </div>
      <button class="btn btn-outline btn-inline" style="flex:none;font-size:11px;padding:3px 8px;color:#c8102e;border-color:#c8102e" onclick="deleteProduct(${p.id})" title="Retirer">✕</button>
    </div>`).join('');
}

async function addProductToCurrentRayon(productId, position) {
  if (!requireEditorSession('ajouter un emplacement')) return;
  const existing = allProductsCache.find(p => p.id === productId);
  if (!existing) return;
  const payload = {
    name: existing.name, brand: existing.brand || '', description: existing.description || '',
    image_url: existing.image_url || '', source_url: existing.source_url || '',
    search_terms: existing.search_terms || '', usage_notes: existing.usage_notes || '',
    alternative_suggestions: existing.alternative_suggestions || '', barcode: existing.barcode || '',
    product_code: existing.product_code || '',
    aisle: rayonCtx.aisle, side: rayonCtx.side, section: rayonCtx.section || '1',
    shelf: rayonCtx.shelf, position
  };
  const data = await apiAddProduct(payload);
  if (data.success !== false && !data.error) {
    if (data.product) upsertCachedProduct(normalizeProduct(data.product));
    // The server already returned the updated row (upserted above) — no need to
    // re-download the whole product list. This keeps rapid scanning instant and
    // cool on phones; the periodic soft refresh resyncs anything else.
    finishConfirmed(`"${existing.name}" ajouté à ${rayonLabel()} — Pos. ${position}.`, '', position);
  } else {
    document.getElementById('scanResult').innerHTML = `<div class="msg error">${esc(data.error || 'Erreur d’ajout.')}</div>`;
  }
}

async function moveProductToCurrentRayon(productId, position) {
  if (!requireEditorSession('déplacer un produit')) return;
  const existing = allProductsCache.find(p => p.id === productId);
  if (!existing) return;
  const payload = {
    ...existing,
    aisle: rayonCtx.aisle, side: rayonCtx.side, section: rayonCtx.section || '1',
    shelf: rayonCtx.shelf, position
  };
  const data = await apiUpdateProduct(payload);
  if (data.success !== false && !data.error) {
    if (data.product) upsertCachedProduct(normalizeProduct(data.product));
    // The server already returned the updated row (upserted above) — no need to
    // re-download the whole product list. This keeps rapid scanning instant and
    // cool on phones; the periodic soft refresh resyncs anything else.
    finishConfirmed(`"${existing.name}" déplacé à ${rayonLabel()} — Pos. ${position}.`, '', position);
  } else {
    document.getElementById('scanResult').innerHTML = `<div class="msg error">${esc(data.error || 'Erreur de déplacement.')}</div>`;
  }
}

// ── Scan lookup ───────────────────────────────────────────────────────────────
function currentLookupAssistPayload() {
  return {
    name: document.getElementById('scanProductName')?.value.trim() || '',
    brand: document.getElementById('scanProductBrand')?.value.trim() || '',
    description: document.getElementById('scanProductDescription')?.value.trim() || '',
    barcode: document.getElementById('scanInput')?.value.trim() || ''
  };
}

function renderLookupAssistPreview() {
  const preview = document.getElementById('lookupAssistPreview');
  const status = document.getElementById('lookupAssistStatus');
  if (!preview || !status) return;
  if (!pendingLookupAssist) {
    preview.innerHTML = '';
    status.textContent = backendInfo.ai_enabled
      ? `Optionnel: genere des mots-clés clients, une explication simple et des alternatives via ${aiProviderLabel()}.`
      : 'IA non configurée sur le serveur. Ajoutez GEMINI_API_KEY sur Render.';
    return;
  }
  status.textContent = 'Aide client generee. Elle sera sauvegardee avec ce produit.';
  preview.innerHTML = `
    ${pendingLookupAssist.search_terms ? `<div class="barcode-text">Mots-clés clients: ${esc(pendingLookupAssist.search_terms)}</div>` : ''}
    ${pendingLookupAssist.usage_notes ? `<div class="desc-text">${esc(pendingLookupAssist.usage_notes)}</div>` : ''}
    ${pendingLookupAssist.alternative_suggestions ? `<div class="barcode-text">Alternatives possibles: ${esc(pendingLookupAssist.alternative_suggestions)}</div>` : ''}
  `;
}

async function lookupScanFromInput(force=false, barcodeOverride='') {
  if (!requireEditorSession('utiliser le scan')) return;
  const barcode = String(barcodeOverride || document.getElementById('scanInput').value || '').trim();
  const div = document.getElementById('scanResult');
  if (!barcode) return;
  if (document.getElementById('scanInput').value.trim() !== barcode) setScannedBarcode(barcode);
  activeLookupBarcode = barcode;
  if (!force && barcode === lastLookedUpBarcode) return;
  lastLookedUpBarcode = barcode;

  // Require rayon context
  if (!rayonCtx.aisle || !rayonCtx.shelf) {
    div.innerHTML = `<div class="msg error" style="font-weight:600">
      Définissez d’abord l Allée et la Tablette dans "Rayon en cours" ci-dessus, puis scannez.
    </div>`;
    return;
  }

  // Find all entries with this barcode in the local cache
  const allMatches = allProductsCache.filter(p =>
    p.barcode && build_barcode_candidates_js(barcode).includes(String(p.barcode).replace(/\s/g,''))
  );

  const atCurrentRayon = allMatches.find(p =>
    String(p.aisle) === rayonCtx.aisle && p.side === rayonCtx.side &&
    String(p.section || '1') === rayonCtx.section && String(p.shelf) === rayonCtx.shelf
  );

  if (atCurrentRayon) {
    // Already on this shelf — but allow adding it AGAIN (a second facing/spot on
    // the same tablette is valid). Offer a one-tap "add again" at the next spot.
    const againPos = nextRayonPosition();
    div.innerHTML = `<div class="card" style="border-left:4px solid #16a34a;padding:12px 16px">
      <div style="font-size:12px;font-weight:700;color:#16a34a;margin-bottom:4px">✓ Déjà sur ce rayon — Position ${esc(atCurrentRayon.position)}</div>
      <div class="name">${esc(atCurrentRayon.name)}</div>
      ${atCurrentRayon.brand ? `<div class="barcode-text">${esc(atCurrentRayon.brand)}</div>` : ''}
      <div class="btn-row" style="margin-top:8px">
        <button class="btn" onclick="addProductToCurrentRayon(${atCurrentRayon.id},'${againPos}')">+ Ajouter encore ici — Pos. ${againPos}</button>
      </div>
    </div>`;
    if (navigator.vibrate) navigator.vibrate([40, 20, 40]);
    // Resume scanning shortly (so a stray re-scan isn't stuck), but keep the
    // "Ajouter encore" button visible so they can add another facing anytime.
    window.setTimeout(() => { lastLookedUpBarcode = ''; resumeScanning(); }, 2500);
    return;
  }

  if (allMatches.length > 0) {
    // Found at other location(s) — offer multi-location options
    currentScanProduct = allMatches[0];
    const nextPos = nextRayonPosition();
    const otherLocs = allMatches.map(p =>
      `<div class="barcode-text">→ Allée ${esc(p.aisle)} — ${esc(sideStaffLabel(p.side))} — Section ${esc(p.section||'1')} — Tablette ${esc(p.shelf)} — Pos. ${esc(p.position)}</div>`
    ).join('');
    div.innerHTML = `<div class="card">
      <div class="pill">Produit trouvé — autre emplacement</div>
      ${allMatches[0].image_url ? `<img class="lookup-image" src="${esc(allMatches[0].image_url)}" alt="" style="margin-top:8px">` : ''}
      <div class="name" style="margin-top:8px">${esc(allMatches[0].name)}</div>
      ${allMatches[0].brand ? `<div class="barcode-text">${esc(allMatches[0].brand)}</div>` : ''}
      ${allMatches[0].description ? `<div class="desc-text">${esc(allMatches[0].description)}</div>` : ''}
      <div style="margin-top:8px;padding:8px;background:#f8fafc;border-radius:8px">
        <div style="font-size:11px;font-weight:700;color:#64748b;margin-bottom:4px">EMPLACEMENT(S) ACTUEL(S)</div>
        ${otherLocs}
      </div>
      <div class="msg info" style="margin-top:8px">Rayon cible: <strong>${esc(rayonLabel())} — Pos. ${nextPos}</strong></div>
      <div class="btn-row">
        <button class="btn" onclick="addProductToCurrentRayon(${allMatches[0].id},'${nextPos}')">+ Ajouter ici aussi</button>
        <button class="btn btn-outline" onclick="moveProductToCurrentRayon(${allMatches[0].id},'${nextPos}')">Déplacer ici</button>
      </div>
    </div>`;
    return;
  }

  // Not in DB anywhere — online lookup form
  showOnlineLookupForm(barcode);
}

function build_barcode_candidates_js(barcode) {
  const raw = String(barcode || '').trim();
  const digits = raw.replace(/\D/g, '');
  const candidates = new Set([raw, digits]);
  if (digits.length === 13 && digits.startsWith('0')) candidates.add(digits.slice(1));
  if (digits.length === 12) candidates.add('0' + digits);
  return [...candidates].filter(Boolean);
}

function showOnlineLookupForm(barcode) {
  const div = document.getElementById('scanResult');
  currentScanProduct = null;
  pendingLookupProduct = null;
  pendingLookupAssist = null;
  div.innerHTML = `<div class="card">
    <div class="pill">Nouveau produit</div>
    <div id="lookupStatus" class="msg info">Recherche dans UPC Item DB, EAN Search, Open Food/Beauty/Drug/Products Facts, Barcode Lookup, Go UPC et sites pharmacies...</div>
    <div style="display:flex;gap:10px;align-items:flex-start;margin-top:10px">
      <div id="lookupImageWrap"></div>
      <div style="flex:1">
        <div class="field">
          <label class="label" for="scanProductName">Nom du produit</label>
          <input type="text" id="scanProductName" value="" placeholder="Nom a confirmer"/>
        </div>
        <div class="field">
          <label class="label" for="scanProductBrand">Marque</label>
          <input type="text" id="scanProductBrand" value="" placeholder="Marque"/>
          <div class="tool-row" style="margin-top:4px;gap:4px">
            <button class="btn btn-outline btn-inline" style="font-size:11px;padding:2px 8px;border-color:#c8102e;color:#c8102e" onclick="document.getElementById('scanProductBrand').value='Biomedic'">★ Biomedic</button>
            <button class="btn btn-outline btn-inline" style="font-size:11px;padding:2px 8px;border-color:#c8102e;color:#c8102e" onclick="document.getElementById('scanProductBrand').value='Essentiel'">★ Essentiel</button>
          </div>
        </div>
      </div>
    </div>
    <div class="field">
      <label class="label" for="scanProductDescription">Description</label>
      <input type="text" id="scanProductDescription" value="" placeholder="Description"/>
    </div>
    <div id="lookupSource" class="barcode-text"></div>
    <div class="tool-row">
      <button class="btn btn-outline btn-inline" onclick="generateLookupAssist()">Générer aide client (IA)</button>
      <span id="lookupAssistStatus" class="small"></span>
    </div>
    <div id="lookupAssistPreview"></div>
    <div class="msg info">Emplacement: <strong>${esc(rayonLabel())}</strong> — Pos. <strong>${nextRayonPosition()}</strong></div>
    <div class="btn-row">
      <button class="btn" onclick="confirmNewProduct()">Enregistrer ici</button>
      <button class="btn btn-outline" onclick="confirmUnknownProduct()">Ajouter à identifier</button>
    </div>
  </div>`;
  renderLookupAssistPreview();
  hydrateOnlineLookup(barcode);
}

async function hydrateOnlineLookup(barcode) {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 9000);
  let lookupProduct = null;
  try {
    const lookupData = await apiLookupOnline(barcode, controller.signal);
    if (lookupData.found) lookupProduct = lookupData.product;
  } catch (e) {}
  window.clearTimeout(timeoutId);
  if (activeLookupBarcode !== barcode) return;
  const status = document.getElementById('lookupStatus');
  if (!status) return;
  if (lookupProduct) {
    pendingLookupProduct = lookupProduct;
    if (!document.getElementById('scanProductName').value.trim()) document.getElementById('scanProductName').value = lookupProduct.name || '';
    if (!document.getElementById('scanProductBrand').value.trim()) document.getElementById('scanProductBrand').value = lookupProduct.brand || '';
    if (!document.getElementById('scanProductDescription').value.trim()) document.getElementById('scanProductDescription').value = lookupProduct.description || '';
    status.className = 'msg success';
    status.textContent = `Trouve dans ${lookupProduct.source}. Verifiez avant de confirmer.`;
    document.getElementById('lookupSource').innerHTML = lookupProduct.source_url ? `Source: <a href="${esc(lookupProduct.source_url)}" target="_blank" rel="noopener noreferrer">${esc(lookupProduct.source_url)}</a>` : '';
    if (lookupProduct.image_url) document.getElementById('lookupImageWrap').innerHTML = `<img class="lookup-image" src="${esc(lookupProduct.image_url)}" alt="Image produit">`;
  } else {
    pendingLookupProduct = null;
    status.className = 'msg error';
    status.textContent = 'Introuvable dans toutes les bases. Remplissez manuellement — marque maison? Utilisez les boutons Biomedic / Essentiel.';
  }
}

async function generateLookupAssist() {
  const status = document.getElementById('lookupAssistStatus');
  const payload = currentLookupAssistPayload();
  if (!payload.name && !payload.description) {
    if (status) status.textContent = 'Entrez au moins un nom ou une description avant de lancer l aide client.';
    return;
  }
  if (status) status.textContent = 'Generation en cours...';
  const result = await apiGenerateProductAssist(payload);
  if (!result.success || !result.assist) {
    pendingLookupAssist = null;
    renderLookupAssistPreview();
    if (status) status.textContent = result.error || 'Aide client indisponible.';
    return;
  }
  pendingLookupAssist = result.assist;
  if (!document.getElementById('scanProductDescription').value.trim() && pendingLookupAssist.usage_notes) {
    document.getElementById('scanProductDescription').value = pendingLookupAssist.usage_notes;
  }
  renderLookupAssistPreview();
}

async function confirmNewProduct() {
  if (!requireEditorSession('ajouter un produit')) return;
  const barcode = document.getElementById('scanInput').value.trim();
  const name = document.getElementById('scanProductName')?.value.trim() || `A identifier - ${barcode}`;
  const brand = document.getElementById('scanProductBrand')?.value.trim() || '';
  const description = document.getElementById('scanProductDescription')?.value.trim() || '';
  if (!barcode) {
    document.getElementById('scanResult').insertAdjacentHTML('beforeend', '<div class="msg error">Code requis.</div>');
    return;
  }
  const pos = nextRayonPosition();
  const data = await apiAddProduct({
    name, brand, description,
    image_url: pendingLookupProduct?.image_url || '',
    source_url: pendingLookupProduct?.source_url || '',
    // Carry the auto-enriched fields the online lookup (and AI) produced.
    search_terms: pendingLookupAssist?.search_terms || pendingLookupProduct?.search_terms || '',
    usage_notes: pendingLookupAssist?.usage_notes || pendingLookupProduct?.usage_notes || '',
    alternative_suggestions: pendingLookupAssist?.alternative_suggestions || pendingLookupProduct?.alternative_suggestions || '',
    barcode,
    aisle: rayonCtx.aisle, side: rayonCtx.side, section: rayonCtx.section || '1',
    shelf: rayonCtx.shelf, position: pos
  });
  if (data.success !== false && !data.error) {
    if (data.product) upsertCachedProduct(normalizeProduct(data.product));
    // The server already returned the updated row (upserted above) — no need to
    // re-download the whole product list. This keeps rapid scanning instant and
    // cool on phones; the periodic soft refresh resyncs anything else.
    finishConfirmed(`"${name}" enregistré à ${rayonLabel()} — Pos. ${pos}.`, brand, pos);
  } else {
    document.getElementById('scanResult').innerHTML = `<div class="msg error">${esc(data.error || 'Erreur pendant l’ajout.')}</div>`;
  }
}

async function confirmUnknownProduct() {
  if (!requireEditorSession('ajouter un produit')) return;
  const barcode = document.getElementById('scanInput').value.trim();
  if (!barcode) return;
  const pos = nextRayonPosition();
  const placeholderName = `A identifier - ${barcode}`;
  const data = await apiAddProduct({
    name: placeholderName, brand: '',
    description: 'A identifier',
    image_url: pendingLookupProduct?.image_url || '',
    source_url: pendingLookupProduct?.source_url || '',
    search_terms: '', usage_notes: '', alternative_suggestions: '',
    barcode,
    aisle: rayonCtx.aisle, side: rayonCtx.side, section: rayonCtx.section || '1',
    shelf: rayonCtx.shelf, position: pos
  });
  if (data.success !== false && !data.error) {
    if (data.product) upsertCachedProduct(normalizeProduct(data.product));
    // The server already returned the updated row (upserted above) — no need to
    // re-download the whole product list. This keeps rapid scanning instant and
    // cool on phones; the periodic soft refresh resyncs anything else.
    finishConfirmed(`"${placeholderName}" ajouté à ${rayonLabel()} — Pos. ${pos}.`, '', pos);
  } else {
    document.getElementById('scanResult').innerHTML = `<div class="msg error">${esc(data.error || 'Erreur pendant l’ajout.')}</div>`;
  }
}

function finishConfirmed(message, brand, filledPos) {
  document.getElementById('scanInput').value = '';
  persistScanDraft();
  lastLookedUpBarcode = '';
  activeLookupBarcode = '';
  currentScanProduct = null;
  pendingLookupProduct = null;
  pendingLookupAssist = null;
  const homeTip = isHomeBrand(brand)
    ? `<div class="msg warning" style="margin-top:6px">★ Marque maison — tablette 2 ou 3, positions 1-3 recommandées.</div>`
    : '';
  document.getElementById('scanResult').innerHTML = `<div class="msg success">${esc(message)}</div>${homeTip}`;
  // When the tablette is now full, jump to the next tablette/section automatically
  // (it refreshes the badge/list itself). Otherwise just refresh in place.
  const advanced = (filledPos != null) && maybeAdvanceRayon(filledPos);
  if (!advanced) {
    refreshRayonList();
    updateRayonCtx(); // refresh P→N indicator in badge
  }
  window.setTimeout(() => {
    resetCameraCandidate();
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
    resumeScanning();   // confirmed → resume decoding for the next item
    focusScanInput();
  }, 1200);
}

window.AppScan = { lookupScanFromInput, updateRayonCtx, updateRayonSideOptions, refreshRayonList, finishConfirmed };
