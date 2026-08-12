let currentExpiryData = null;
let expiryBoardItems = [];
let expiryBoardLoadedAt = 0;
let expiryLookupGeneration = 0;
let expiryBoardLoading = false;
let expiryViewMode = 'scan';
let expiryBoardStore = '';

function expiryStoreKey() {
  const store = typeof getCurrentStore === 'function' ? getCurrentStore() : null;
  return String(store?.id || 'default');
}

function expiryDigits(value) {
  return String(value || '').replace(/\D/g, '');
}

function expiryInitialsValue() {
  return String(document.getElementById('expiryInitials')?.value || '').trim().toUpperCase();
}

function suggestedExpiryInitials() {
  try {
    const stored = localStorage.getItem(STORAGE_KEYS.expiryInitials);
    if (stored) return String(stored).slice(0, 12).toUpperCase();
    const editor = JSON.parse(localStorage.getItem(STORAGE_KEYS.editorSession) || '{}');
    const name = String(editor.username || '').trim();
    if (!name || name.toLowerCase() === 'appareil') return '';
    return name.split(/\s+/).map(part => part[0] || '').join('').slice(0, 6).toUpperCase();
  } catch (_) {
    return '';
  }
}

function loadExpiryInitials() {
  const input = document.getElementById('expiryInitials');
  if (input && !input.value) input.value = suggestedExpiryInitials();
}

function saveExpiryInitials() {
  const input = document.getElementById('expiryInitials');
  if (!input) return;
  input.value = input.value.toUpperCase().slice(0, 12);
  localStorage.setItem(STORAGE_KEYS.expiryInitials, input.value.trim());
}

async function setExpiryMode(mode) {
  expiryViewMode = mode === 'board' ? 'board' : 'scan';
  const scanView = document.getElementById('expiryScanView');
  const boardView = document.getElementById('expiryBoardView');
  const scanButton = document.getElementById('expiryModeScan');
  const boardButton = document.getElementById('expiryModeBoard');
  if (scanView) scanView.hidden = expiryViewMode !== 'scan';
  if (boardView) boardView.hidden = expiryViewMode !== 'board';
  scanButton?.classList.toggle('active', expiryViewMode === 'scan');
  boardButton?.classList.toggle('active', expiryViewMode === 'board');
  if (expiryViewMode === 'board') {
    if (cameraUsageMode === 'expiry' && (scannerStream || html5Scanner || quaggaActive)) {
      await stopCamera();
    }
    await loadExpiryBoard();
  } else {
    window.setTimeout(() => document.getElementById('expiryScanInput')?.focus(), 50);
  }
}

function onExpiryTabActivated() {
  loadExpiryInitials();
  if (expiryViewMode === 'board') void loadExpiryBoard();
  else window.setTimeout(() => document.getElementById('expiryScanInput')?.focus(), 50);
}

function localExpiryProduct(barcode) {
  const digits = expiryDigits(barcode);
  if (!digits || !Array.isArray(allProductsCache)) return null;
  const matches = allProductsCache.filter(product => {
    const candidate = expiryDigits(product?.barcode);
    return candidate === digits
      || (candidate.length === 13 && candidate.startsWith('0') && candidate.slice(1) === digits)
      || (digits.length === 13 && digits.startsWith('0') && digits.slice(1) === candidate);
  });
  if (!matches.length) return null;
  const primary = matches.find(product => product.in_stock !== 0) || matches[0];
  return {
    found: true,
    in_plan: true,
    barcode: primary.barcode || digits,
    product_code: primary.product_code || '',
    name: primary.name || 'Produit sans nom',
    brand: primary.brand || '',
    description: primary.description || '',
    image_url: primary.image_url || '',
    locations: matches.map(product => ({
      aisle: product.aisle,
      side: product.side,
      section: product.section || '1',
      shelf: product.shelf,
      position: product.position,
      label: `Allée ${product.aisle} · ${sideDisplayLabel(product.side)} · S${product.section || '1'} T${product.shelf} P${product.position}`,
    })),
  };
}

function handleExpiryInputKey(event) {
  if (event.key !== 'Enter' && event.key !== 'Tab') return;
  const value = document.getElementById('expiryScanInput')?.value.trim();
  if (!value) return;
  event.preventDefault();
  lookupExpiryFromInput(true);
}

async function lookupExpiryFromInput(focusDate=true, barcodeOverride='') {
  if (!requireEditorSession('utiliser le suivi des dates')) return;
  const input = document.getElementById('expiryScanInput');
  const result = document.getElementById('expiryLookupResult');
  const barcode = expiryDigits(barcodeOverride || input?.value);
  if (!barcode || barcode.length < 6 || barcode.length > 14) {
    if (result) result.innerHTML = '<div class="msg error">Entrez ou scannez un UPC valide.</div>';
    return;
  }
  cameraUsageMode = 'expiry';
  if (input) input.value = barcode;
  const generation = ++expiryLookupGeneration;
  const localProduct = localExpiryProduct(barcode);
  if (localProduct) {
    renderExpiryProduct({product: localProduct, current: null, history: []}, true);
  } else if (result) {
    result.innerHTML = '<div class="card"><div class="small">Recherche du produit…</div></div>';
  }
  try {
    const data = await apiGetExpiryProduct(barcode, expiryStoreKey());
    if (generation !== expiryLookupGeneration) return;
    currentExpiryData = data;
    renderExpiryProduct(data, false);
    if (focusDate) {
      window.setTimeout(() => document.getElementById('expiryDateInput')?.focus(), 50);
    }
  } catch (error) {
    if (generation !== expiryLookupGeneration) return;
    if (result) {
      result.innerHTML = `<div class="msg error">${esc(error.message || 'Impossible de charger ce produit.')}</div>`;
    }
  }
}

function expirySafeImage(value) {
  return typeof safeHttpUrl === 'function' ? safeHttpUrl(value) : '';
}

function formatExpiryDate(value) {
  const parts = String(value || '').split('-').map(Number);
  if (parts.length !== 3 || parts.some(part => !Number.isFinite(part))) return String(value || '');
  return new Intl.DateTimeFormat('fr-CA', {
    day: 'numeric', month: 'long', year: 'numeric',
  }).format(new Date(parts[0], parts[1] - 1, parts[2], 12));
}

function formatExpiryTimestamp(value) {
  const parsed = new Date(String(value || ''));
  if (Number.isNaN(parsed.getTime())) return String(value || '');
  return new Intl.DateTimeFormat('fr-CA', {
    day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit',
  }).format(parsed);
}

function expiryHistoryLabel(event) {
  if (event.action === 'created') return `Date ajoutée : ${formatExpiryDate(event.expiry_date)}`;
  if (event.action === 'updated') {
    return `${formatExpiryDate(event.previous_expiry_date)} → ${formatExpiryDate(event.expiry_date)}`;
  }
  if (event.action === 'confirmed') return `Date reconfirmée : ${formatExpiryDate(event.expiry_date)}`;
  if (event.action === 'cleared') return `Retirée du suivi : ${formatExpiryDate(event.previous_expiry_date)}`;
  return 'Modification';
}

function expiryHistoryMarkup(history) {
  if (!Array.isArray(history) || !history.length) return '';
  return `<details class="expiry-history">
    <summary>Historique · ${history.length} action${history.length > 1 ? 's' : ''}</summary>
    <div class="expiry-history-list">
      ${history.map(event => `<div class="expiry-history-row">
        <div><strong>${esc(expiryHistoryLabel(event))}</strong>${event.note ? `<div>${esc(event.note)}</div>` : ''}</div>
        <div>${esc(event.initials || event.recorded_by || '')}<br>${esc(formatExpiryTimestamp(event.created_at))}</div>
      </div>`).join('')}
    </div>
  </details>`;
}

function renderExpiryProduct(data, loading=false) {
  const target = document.getElementById('expiryLookupResult');
  if (!target) return;
  const product = data?.product || {};
  const current = data?.current || null;
  const locations = Array.isArray(product.locations) ? product.locations : [];
  const imageUrl = expirySafeImage(product.image_url);
  const locationMarkup = locations.length
    ? `<div class="expiry-location-list">${locations.map(location => `<span class="expiry-location-chip">${esc(location.label || '')}</span>`).join('')}</div>`
    : `<div class="expiry-unmapped">${product.found ? 'Position à confirmer dans le magasin' : 'UPC absent du catalogue — suivi conservé sous ce code'}</div>`;
  const currentMarkup = current
    ? `<div class="expiry-current-banner">
        <div><span>Date enregistrée</span><strong>${esc(formatExpiryDate(current.earliest_expiry_date))}</strong></div>
        <span>${esc(current.checked_by || '')} · ${esc(formatExpiryTimestamp(current.checked_at))}</span>
      </div>`
    : '';
  const saveLabel = current ? 'Mettre à jour et suivant' : 'Enregistrer et suivant';
  target.innerHTML = `<div class="card expiry-product-card">
    <div class="expiry-product-main">
      ${imageUrl
        ? `<img class="expiry-product-image" src="${esc(imageUrl)}" alt="${esc(product.name || 'Produit')}" loading="eager" decoding="async" onerror="this.replaceWith(Object.assign(document.createElement('span'),{className:'expiry-product-image-placeholder',textContent:'□'}))">`
        : '<span class="expiry-product-image-placeholder" aria-label="Photo non disponible">□</span>'}
      <div>
        <div class="expiry-product-name">${esc(product.name || `Produit à identifier · ${product.barcode || ''}`)}</div>
        ${product.brand ? `<div class="expiry-product-brand">${esc(product.brand)}</div>` : ''}
        <div class="expiry-product-code">UPC ${esc(product.barcode || '')}${product.product_code ? ` · Code ${esc(product.product_code)}` : ''}</div>
        ${locationMarkup}
      </div>
    </div>
    ${loading ? '<div class="msg info">Vérification de la date enregistrée…</div>' : `
      ${currentMarkup}
      <div class="expiry-entry-grid">
        <div class="field">
          <label class="label" for="expiryDateInput">Date la plus proche trouvée</label>
          <input id="expiryDateInput" type="date" value="${esc(current?.earliest_expiry_date || '')}"/>
        </div>
        <div class="field">
          <label class="label" for="expiryNoteInput">Note optionnelle</label>
          <input id="expiryNoteInput" type="text" maxlength="300" value="${esc(current?.note || '')}" placeholder="Ex: présentoir avant"/>
        </div>
      </div>
      <div id="expirySaveStatus"></div>
      <div class="expiry-save-row">
        <button type="button" id="expirySaveButton" class="btn" onclick="saveCurrentExpiry()">${saveLabel}</button>
        ${current ? '<button type="button" id="expiryClearButton" class="btn btn-outline btn-inline btn-danger" onclick="clearCurrentExpiry()">Retirer</button>' : ''}
      </div>
      ${expiryHistoryMarkup(data?.history || [])}
    `}
  </div>`;
}

async function saveCurrentExpiry() {
  if (!currentExpiryData?.product) return;
  const initials = expiryInitialsValue();
  const expiryDate = document.getElementById('expiryDateInput')?.value || '';
  const note = document.getElementById('expiryNoteInput')?.value.trim() || '';
  const status = document.getElementById('expirySaveStatus');
  if (!initials) {
    if (status) status.innerHTML = '<div class="msg error">Entrez vos initiales.</div>';
    document.getElementById('expiryInitials')?.focus();
    return;
  }
  if (!expiryDate) {
    if (status) status.innerHTML = '<div class="msg error">Choisissez la date la plus proche.</div>';
    document.getElementById('expiryDateInput')?.focus();
    return;
  }
  const previous = currentExpiryData.current?.earliest_expiry_date || '';
  if (previous && expiryDate > previous) {
    const confirmed = window.confirm(
      `La nouvelle date (${formatExpiryDate(expiryDate)}) est plus tard que la date enregistrée (${formatExpiryDate(previous)}). Confirmez que les produits de la date précédente ont été retirés ou revérifiés.`
    );
    if (!confirmed) return;
  }
  saveExpiryInitials();
  const button = document.getElementById('expirySaveButton');
  if (button) { button.disabled = true; button.textContent = 'Enregistrement…'; }
  const payload = {
    store: expiryStoreKey(),
    barcode: currentExpiryData.product.barcode,
    earliest_expiry_date: expiryDate,
    initials,
    note,
    expected_revision: currentExpiryData.current?.revision || 0,
  };
  try {
    const response = await apiSetExpiryDate(payload);
    if (!response.ok) {
      if (response.status === 409 && response.data?.current) {
        currentExpiryData.current = response.data.current;
        renderExpiryProduct(currentExpiryData, false);
      }
      const nextStatus = document.getElementById('expirySaveStatus');
      if (nextStatus) nextStatus.innerHTML = `<div class="msg error">${esc(response.data?.error || 'Enregistrement impossible.')}</div>`;
      return;
    }
    upsertExpiryBoardItem(response.data.current);
    const productName = currentExpiryData.product.name || currentExpiryData.product.barcode;
    finishExpiryEntry(`Date enregistrée pour ${productName}.`);
  } catch (_) {
    if (status) status.innerHTML = '<div class="msg error">Impossible de joindre le serveur.</div>';
  } finally {
    const activeButton = document.getElementById('expirySaveButton');
    if (activeButton) {
      activeButton.disabled = false;
      activeButton.textContent = currentExpiryData?.current
        ? 'Mettre à jour et suivant'
        : 'Enregistrer et suivant';
    }
  }
}

async function clearCurrentExpiry() {
  const current = currentExpiryData?.current;
  const product = currentExpiryData?.product;
  if (!current || !product) return;
  const initials = expiryInitialsValue();
  if (!initials) {
    document.getElementById('expiryInitials')?.focus();
    return;
  }
  if (!window.confirm(`Retirer ${product.name || product.barcode} du tableau des dates? L’action restera dans l’historique.`)) return;
  const button = document.getElementById('expiryClearButton');
  if (button) { button.disabled = true; button.textContent = 'Retrait…'; }
  let response;
  try {
    response = await apiClearExpiryDate(product.barcode, {
      store: expiryStoreKey(), initials,
      expected_revision: current.revision,
    });
  } catch (_) {
    const status = document.getElementById('expirySaveStatus');
    if (status) status.innerHTML = '<div class="msg error">Impossible de joindre le serveur.</div>';
    if (button) { button.disabled = false; button.textContent = 'Retirer'; }
    return;
  }
  if (!response.ok) {
    const status = document.getElementById('expirySaveStatus');
    if (status) status.innerHTML = `<div class="msg error">${esc(response.data?.error || 'Retrait impossible.')}</div>`;
    if (button) { button.disabled = false; button.textContent = 'Retirer'; }
    return;
  }
  expiryBoardItems = expiryBoardItems.filter(item => item.gtin_key !== current.gtin_key);
  finishExpiryEntry(`${product.name || product.barcode} a été retiré du tableau.`);
}

function finishExpiryEntry(message) {
  currentExpiryData = null;
  expiryLookupGeneration += 1;
  const input = document.getElementById('expiryScanInput');
  if (input) input.value = '';
  const result = document.getElementById('expiryLookupResult');
  if (result) result.innerHTML = `<div class="msg success">${esc(message)} Prêt pour le produit suivant.</div>`;
  cameraUsageMode = 'expiry';
  scanPaused = false;
  resetCameraCandidate();
  resumeScanning();
  const status = document.getElementById('expiryScannerStatus');
  if (status && (scannerStream || html5Scanner || quaggaActive || nativeScanActive || zxingActive)) {
    status.textContent = 'Cadrez le prochain code-barres';
  }
  window.setTimeout(() => input?.focus({preventScroll: true}), 50);
}

function clearExpiryLookup() {
  currentExpiryData = null;
  expiryLookupGeneration += 1;
  const input = document.getElementById('expiryScanInput');
  if (input) input.value = '';
  const result = document.getElementById('expiryLookupResult');
  if (result) result.innerHTML = '';
  cameraUsageMode = 'expiry';
  scanPaused = false;
  resetCameraCandidate();
  resumeScanning();
  window.setTimeout(() => input?.focus({preventScroll: true}), 50);
}

function upsertExpiryBoardItem(item) {
  if (!item?.gtin_key) return;
  const store = expiryStoreKey();
  if (expiryBoardStore && expiryBoardStore !== store) expiryBoardItems = [];
  expiryBoardStore = store;
  const index = expiryBoardItems.findIndex(existing => existing.gtin_key === item.gtin_key);
  if (index >= 0) expiryBoardItems[index] = item;
  else expiryBoardItems.push(item);
  expiryBoardItems.sort((a, b) => String(a.earliest_expiry_date).localeCompare(String(b.earliest_expiry_date)));
}

async function loadExpiryBoard(force=false) {
  if (expiryBoardLoading) return;
  const store = expiryStoreKey();
  if (expiryBoardStore !== store) {
    expiryBoardItems = [];
    expiryBoardLoadedAt = 0;
    expiryBoardStore = store;
  }
  if (!force && expiryBoardItems.length && Date.now() - expiryBoardLoadedAt < 15000) {
    renderExpirySummary();
    renderExpiryBoard();
    return;
  }
  const list = document.getElementById('expiryBoardList');
  if (list && !expiryBoardItems.length) list.innerHTML = '<div class="empty">Chargement des dates…</div>';
  expiryBoardLoading = true;
  try {
    const data = await apiGetExpiryBoard(store);
    expiryBoardItems = Array.isArray(data?.items) ? data.items : [];
    expiryBoardLoadedAt = Date.now();
    const updated = document.getElementById('expiryBoardUpdated');
    if (updated) updated.textContent = `Actualisé à ${new Intl.DateTimeFormat('fr-CA', {hour:'2-digit', minute:'2-digit'}).format(new Date())}`;
    renderExpirySummary(data?.summary);
    renderExpiryBoard();
  } catch (error) {
    if (list) list.innerHTML = `<div class="msg error">${esc(error.message || 'Impossible de charger les dates.')}</div>`;
  } finally {
    expiryBoardLoading = false;
  }
}

function computeExpirySummary() {
  const summary = {total: expiryBoardItems.length, expired: 0, critical: 0, soon: 0, watch: 0, later: 0};
  expiryBoardItems.forEach(item => {
    if (Object.prototype.hasOwnProperty.call(summary, item.urgency)) summary[item.urgency] += 1;
  });
  return summary;
}

function renderExpirySummary(provided=null) {
  const target = document.getElementById('expirySummary');
  if (!target) return;
  const summary = provided || computeExpirySummary();
  const cells = [
    ['expired', summary.expired || 0, 'Dépassées'],
    ['critical', summary.critical || 0, '≤ 7 jours'],
    ['soon', summary.soon || 0, '8–30 jours'],
    ['watch', summary.watch || 0, '31–60 jours'],
    ['later', summary.later || 0, '> 60 jours'],
  ];
  target.innerHTML = cells.map(([kind, count, label]) => `<div class="expiry-summary-item" data-kind="${kind}"><strong>${count}</strong><span>${label}</span></div>`).join('');
}

function expiryUrgencyLabel(item) {
  const days = Number(item.days_remaining);
  if (item.urgency === 'expired') return `Dépassée depuis ${Math.abs(days)} jour${Math.abs(days) > 1 ? 's' : ''}`;
  if (days === 0) return 'Aujourd’hui';
  if (days === 1) return 'Dans 1 jour';
  return `Dans ${days} jours`;
}

function expiryBoardMatchesFilter(item, filter) {
  const days = Number(item.days_remaining);
  if (filter === 'all') return true;
  if (filter === 'expired') return item.urgency === 'expired';
  if (filter === 'later') return days > 60;
  const limit = Number(filter);
  return Number.isFinite(days) && days >= 0 && days <= limit;
}

function renderExpiryBoard() {
  const target = document.getElementById('expiryBoardList');
  if (!target) return;
  const query = String(document.getElementById('expiryBoardSearch')?.value || '').trim().toLowerCase();
  const filter = document.getElementById('expiryBoardFilter')?.value || 'all';
  const items = expiryBoardItems.filter(item => {
    if (!expiryBoardMatchesFilter(item, filter)) return false;
    if (!query) return true;
    const locations = (item.locations || []).map(location => location.label || '').join(' ');
    return [item.product_name, item.brand, item.barcode, item.product_code, locations]
      .join(' ').toLowerCase().includes(query);
  });
  if (!items.length) {
    target.innerHTML = '<div class="empty">Aucun produit pour ce filtre.</div>';
    return;
  }
  target.className = 'expiry-board-list';
  target.innerHTML = items.map(item => {
    const imageUrl = expirySafeImage(item.image_url);
    const locations = Array.isArray(item.locations) ? item.locations : [];
    const location = locations[0]?.label || 'Position à confirmer';
    const extraLocations = locations.length > 1 ? ` · ${locations.length} emplacements` : '';
    return `<article class="expiry-board-item" data-urgency="${esc(item.urgency || 'later')}">
      ${imageUrl
        ? `<img class="expiry-board-image" src="${esc(imageUrl)}" alt="${esc(item.product_name || 'Produit')}" loading="lazy" decoding="async" onerror="this.remove()">`
        : '<span class="expiry-board-image-placeholder" aria-label="Photo non disponible">□</span>'}
      <div>
        <div class="expiry-board-name">${esc(item.product_name || item.barcode)}</div>
        <div class="expiry-board-meta">${item.brand ? `${esc(item.brand)} · ` : ''}UPC ${esc(item.barcode || '')}</div>
        <div class="expiry-board-meta">${esc(location)}${esc(extraLocations)}</div>
        <div class="expiry-board-meta">Vérifié par ${esc(item.checked_by || '—')} · ${esc(formatExpiryTimestamp(item.checked_at))}</div>
      </div>
      <div class="expiry-board-date">
        <div><strong>${esc(formatExpiryDate(item.earliest_expiry_date))}</strong><span class="expiry-urgency-label">${esc(expiryUrgencyLabel(item))}</span></div>
        <button type="button" class="expiry-board-action" data-barcode="${esc(item.barcode || '')}" onclick="editExpiryFromBoard(this.dataset.barcode)">Modifier</button>
      </div>
    </article>`;
  }).join('');
}

async function editExpiryFromBoard(barcode) {
  await setExpiryMode('scan');
  const input = document.getElementById('expiryScanInput');
  if (input) input.value = barcode;
  await lookupExpiryFromInput(false, barcode);
  document.getElementById('expiryLookupResult')?.scrollIntoView({behavior: 'smooth', block: 'start'});
}

window.onExpiryTabActivated = onExpiryTabActivated;
window.AppExpiry = {
  setExpiryMode, lookupExpiryFromInput, saveCurrentExpiry, clearCurrentExpiry,
  loadExpiryBoard, renderExpiryBoard, clearExpiryLookup,
};
