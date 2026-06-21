// ── Lock / unlock ─────────────────────────────────────────────────────────────
let _pendingLockedTab = null;

function isUnlocked() {
  if (localStorage.getItem('familiprixSession') !== LOCK_HASH) return false;
  const unlockedAt = parseInt(localStorage.getItem('familiprixSessionAt') || '0');
  if (Date.now() - unlockedAt > LOCK_TTL_MS) {
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

function unlockApp() {
  const inp = document.getElementById('lockPasswordInput');
  const err = document.getElementById('lockError');
  if (!inp) return;
  if (btoa(inp.value) === LOCK_HASH) {
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

window.AppLock = { isUnlocked, updateLockUi, showLockModal, closeLockModal, unlockApp, lockApp };
