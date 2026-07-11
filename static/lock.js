// ── Lock / unlock ─────────────────────────────────────────────────────────────
let _pendingLockedTab = null;

// SHA-256 of a string, hex — Web Crypto (available on HTTPS and localhost).
async function _sha256Hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(str));
  return Array.from(new Uint8Array(buf)).map(b => b.toString(16).padStart(2, '0')).join('');
}

// Locked unless: session marker === the password hash AND set < 8h ago.
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

// SECURITY DECISION (2026-07-11): the session window is FIXED from the moment
// the password was entered — activity never extends it. A sliding renewal was
// tried and made sessions effectively immortal on any actively-used device.
// At 8h, one unlock covers a full shift without ever compromising the expiry.

window.AppLock = { isUnlocked, updateLockUi, showLockModal, closeLockModal, unlockApp, lockApp };
