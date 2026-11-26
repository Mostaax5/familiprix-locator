// ── Store selection ───────────────────────────────────────────────────────────
// Each deployment serves one database-bound store; localStorage cannot select another store.

function getCurrentStore() {
  return STORES[0] || null;
}

function getCurrentStoreName() {
  const s = getCurrentStore();
  return s ? s.name : '';
}

function _renderStoreOptions() {
  const sel = document.getElementById('storeSelect');
  if (!sel) return;
  sel.innerHTML = STORES.map(s => `<option value="${esc(s.id)}">${esc(s.name)} — ${esc(s.address)}</option>`).join('');
}

function showStoreModal() {
  const modal = document.getElementById('storeModal');
  if (!modal) return;
  _renderStoreOptions();
  const current = getCurrentStore();
  if (current) {
    const sel = document.getElementById('storeSelect');
    if (sel && [...sel.options].some(o => o.value === current.id)) sel.value = current.id;
  }
  const err = document.getElementById('storeError');
  if (err) err.textContent = '';
  modal.style.display = 'flex';
  window.setTimeout(() => document.getElementById('storeSelect')?.focus(), 60);
}

function confirmStore() {
  const sel = document.getElementById('storeSelect');
  const err = document.getElementById('storeError');
  if (!sel) return;
  const store = STORES.find(s => s.id === sel.value);
  if (!store) { if (err) err.textContent = 'Choisissez un magasin.'; return; }
  localStorage.setItem(STORAGE_KEYS.store, JSON.stringify({id: store.id, name: store.name, address: store.address}));
  const modal = document.getElementById('storeModal');
  if (modal) modal.style.display = 'none';
  updateStoreLabel();
}

// Show the modal on boot only if no store has been chosen yet.
function ensureStoreSelected() {
  if (!getCurrentStore() && STORES.length === 1) {
    const store = STORES[0];
    localStorage.setItem(STORAGE_KEYS.store, JSON.stringify({
      id: store.id,
      name: store.name,
      address: store.address,
    }));
  }
  updateStoreLabel();
  if (!getCurrentStore()) showStoreModal();
}

function updateStoreLabel() {
  const el = document.getElementById('storeLabel');
  if (el) el.textContent = getCurrentStoreName() || 'Aucun magasin sélectionné';
}

window.AppStore = { getCurrentStore, getCurrentStoreName, showStoreModal, confirmStore, ensureStoreSelected, updateStoreLabel };
