// ── Lock / unlock ─────────────────────────────────────────────────────────────
let _pendingLockedTab = null;

// SHA-256 of a string, hex — Web Crypto (available on HTTPS and localhost).
async function _sha256Hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

// Locked unless: session marker === the password hash AND set < 4h ago.
// Expired or tampered sessions are cleared and treated as locked.
function isUnlocked() {
  if (localStorage.getItem('familiprixSession') !== LOCK_HASH) return false;
  const unlockedAt = parseInt(localStorage.getItem('familiprixSessionAt') || '0', 10);
  if (!unlockedAt || Date.now() - unlockedAt > LOCK_TTL_MS) {
    localStorage.removeItem('familiprixSession');
    localStorage.removeItem('familiprixSessionAt');
    return false;
  }
  return true;
}

function updateLockUi() {
  const unlocked = isUnlocked();
  const scanBtn = document.getElementById('tabBtn-scan');
  const addBtn  = document.getElementById('tabBtn-add');
  if (scanBtn) scanBtn.textContent = unlocked ? 'Scan' : 'Scan 🔒';
  if (addBtn)  addBtn.textContent  = unlocked ? 'Plan' : 'Plan 🔒';
  const lockBtn = document.getElementById('lockButton');
  if (lockBtn) lockBtn.style.display = unlocked ? '' : 'none';
}

function showLockModal(pendingTab) {
  _pendingLockedTab = pendingTab || null;
  const modal = document.getElementById('lockModal');
  if (!modal) return;
  modal.style.display = 'flex';
  window.setTimeout(() => document.getElementById('lockPasswordInput')?.focus(), 60);
}

function closeLockModal() {
  const modal = document.getElementById('lockModal');
  if (modal) modal.style.display = 'none';
  const inp = document.getElementById('lockPasswordInput');
  if (inp) inp.value = '';
  const err = document.getElementById('lockError');
  if (err) err.textContent = '';
  _pendingLockedTab = null;
}

async function unlockApp() {
  const inp = document.getElementById('lockPasswordInput');
  const err = document.getElementById('lockError');
  if (!inp) return;
  let entered = '';
  try {
    entered = await _sha256Hex(inp.value);
  } catch (_) {
    if (err) err.textContent = "Sécurité indisponible (utilisez HTTPS).";
    return;
  }
  if (entered === LOCK_HASH) {
    localStorage.setItem('familiprixSession', LOCK_HASH);
    localStorage.setItem('familiprixSessionAt', String(Date.now()));
    const pending = _pendingLockedTab;
    closeLockModal();
    updateLockUi();
    if (pending) switchTab(pending);
  } else {
    if (err) err.textContent = 'Mot de passe incorrect.';
    inp.value = '';
    inp.focus();
  }
}

function lockApp() {
  localStorage.removeItem('familiprixSession');
  localStorage.removeItem('familiprixSessionAt');
  updateLockUi();
  if (LOCKED_TABS.has(localStorage.getItem(STORAGE_KEYS.activeTab))) switchTab('search');
}

// Sliding expiry: ACTIVE use silently renews the 4h session, so the password is
// only asked again after 4 hours of real inactivity — never in the middle of a
// work session (mapping an aisle or watching the enrichment used to get kicked
// out at exactly 4h). An ALREADY-expired session is never resurrected here.
let _lockRenewAt = 0;
function renewLockSession() {
  const now = Date.now();
  if (now - _lockRenewAt < 60000) return;   // at most one localStorage write per minute
  if (localStorage.getItem('familiprixSession') !== LOCK_HASH) return;
  const at = parseInt(localStorage.getItem('familiprixSessionAt') || '0', 10);
  if (!at || now - at > LOCK_TTL_MS) return;
  localStorage.setItem('familiprixSessionAt', String(now));
  _lockRenewAt = now;
}

window.AppLock = { isUnlocked, updateLockUi, showLockModal, closeLockModal, unlockApp, lockApp };
