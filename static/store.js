// ── Store selection ───────────────────────────────────────────────────────────
// On first open the employee picks which store they are in and confirms with a
// simple code (currently '0'). The choice is remembered. The selected store is
// attached to AI training logs so future data is grouped per location.

function getCurrentStore() {
  try {
    const raw = localStorage.getItem(STORAGE_KEYS.store);
    return raw ? JSON.parse(raw) : null;
  } catch (_) { return null; }
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
  const pwd = document.getElementById('storePassword');
  if (pwd) pwd.value = '';
  modal.style.display = 'flex';
  window.setTimeout(() => pwd?.focus(), 60);
}

function confirmStore() {
  const sel = document.getElementById('storeSelect');
  const pwd = document.getElementById('storePassword');
  const err = document.getElementById('storeError');
  if (!sel) return;
  const store = STORES.find(s => s.id === sel.value);
  if (!store) { if (err) err.textContent = 'Choisissez un magasin.'; return; }
  if ((pwd?.value || '') !== store.pass) {
    if (err) err.textContent = 'Code incorrect.';
    if (pwd) { pwd.value = ''; pwd.focus(); }
    return;
  }
  localStorage.setItem(STORAGE_KEYS.store, JSON.stringify({id: store.id, name: store.name, address: store.address}));
  const modal = document.getElementById('storeModal');
  if (modal) modal.style.display = 'none';
  updateStoreLabel();
}

// Show the modal on boot only if no store has been chosen yet.
function ensureStoreSelected() {
  updateStoreLabel();
  if (!getCurrentStore()) showStoreModal();
}

function updateStoreLabel() {
  const el = document.getElementById('storeLabel');
  if (el) el.textContent = getCurrentStoreName() || 'Aucun magasin sélectionné';
}

window.AppStore = { getCurrentStore, getCurrentStoreName, showStoreModal, confirmStore, ensureStoreSelected, updateStoreLabel };
