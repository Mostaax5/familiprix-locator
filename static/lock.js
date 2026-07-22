// Server-backed protection for the Scan and Plan tabs. Search and Client are
// public; the password and session token never enter localStorage or source JS.
let _pendingLockedTab = null;
const _authState = {
  authenticated: false,
  csrfToken: '',
  expiresAt: 0,
  username: '',
};

function csrfToken() {
  return _authState.csrfToken;
}

function isUnlocked() {
  return Boolean(
    _authState.authenticated &&
    _authState.csrfToken &&
    Date.now() < (_authState.expiresAt * 1000)
  );
}

function _applyAuthState(data) {
  _authState.authenticated = Boolean(data?.authenticated);
  _authState.csrfToken = String(data?.csrf_token || '');
  _authState.expiresAt = Number(data?.expires_at || 0);
  _authState.username = String(data?.username || '');
  updateLockUi();
}

function _clearAuthState() {
  _authState.authenticated = false;
  _authState.csrfToken = '';
  _authState.expiresAt = 0;
  _authState.username = '';
  updateLockUi();
}

function clearSensitiveBrowserData() {
  [
    STORAGE_KEYS.scanDraft,
    STORAGE_KEYS.addDraft,
    'familiprixPlanMoveUndo',
  ].filter(Boolean).forEach(key => localStorage.removeItem(key));
  window.resetAuthenticatedAppState?.();
}

function updateLockUi() {
  const unlocked = isUnlocked();
  const labels = {
    search: 'Recherche',
    client: 'Client',
    scan: 'Scan',
    add: 'Plan',
  };
  for (const [tab, label] of Object.entries(labels)) {
    const button = document.getElementById(`tabBtn-${tab}`);
    if (button) button.textContent = `${label}${LOCKED_TABS.has(tab) && !unlocked ? ' 🔒' : ''}`;
  }
  const lockButton = document.getElementById('lockButton');
  if (lockButton) lockButton.style.display = unlocked ? '' : 'none';
}

function _savedEditorName() {
  try {
    const saved = JSON.parse(localStorage.getItem(STORAGE_KEYS.editorSession) || '{}');
    return String(saved.username || '');
  } catch (_) {
    return '';
  }
}

function showLockModal(pendingTab) {
  if (pendingTab && LOCKED_TABS.has(pendingTab)) _pendingLockedTab = pendingTab;
  const modal = document.getElementById('lockModal');
  if (!modal) return;
  modal.style.display = 'flex';
  const error = document.getElementById('lockError');
  if (error) error.textContent = '';
  window.setTimeout(() => document.getElementById('lockPasswordInput')?.focus(), 60);
}

function closeLockModal() {
  if (!isUnlocked()) return;
  dismissLockModal();
}

function dismissLockModal() {
  const modal = document.getElementById('lockModal');
  if (modal) modal.style.display = 'none';
  const input = document.getElementById('lockPasswordInput');
  if (input) input.value = '';
  const error = document.getElementById('lockError');
  if (error) error.textContent = '';
  _pendingLockedTab = null;
}

async function initializeAuth() {
  try {
    const response = await secureFetch('/api/auth/status', {
      cache: 'no-store',
      skipAuthHandling: true,
    });
    const data = await response.json();
    if (!response.ok || !data.authenticated) {
      _clearAuthState();
      clearSensitiveBrowserData();
      return false;
    }
    _applyAuthState(data);
    return true;
  } catch (_) {
    _clearAuthState();
    clearSensitiveBrowserData();
    return false;
  }
}

async function unlockApp() {
  const passwordInput = document.getElementById('lockPasswordInput');
  const error = document.getElementById('lockError');
  const button = document.getElementById('lockLoginButton');
  if (!passwordInput || !passwordInput.value) {
    if (error) error.textContent = 'Entrez le mot de passe.';
    passwordInput?.focus();
    return;
  }
  if (button) {
    button.disabled = true;
    button.textContent = 'Verification...';
  }
  if (error) error.textContent = '';
  try {
    const response = await secureFetch('/api/auth/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        password: passwordInput.value,
        username: _savedEditorName(),
      }),
      skipAuthHandling: true,
    });
    const data = await response.json();
    passwordInput.value = '';
    if (!response.ok || !data.authenticated) {
      if (error) error.textContent = data.error || 'Mot de passe incorrect.';
      passwordInput.focus();
      return;
    }
    _applyAuthState(data);
    localStorage.setItem(
      STORAGE_KEYS.editorSession,
      JSON.stringify({username: data.username || 'appareil'}),
    );
    const pending = _pendingLockedTab;
    closeLockModal();
    await window.resumeAuthenticatedApp?.(pending);
  } catch (_) {
    if (error) error.textContent = 'Impossible de joindre le serveur pour le moment.';
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = 'Deverrouiller';
    }
  }
}

async function lockApp() {
  try {
    if (isUnlocked()) {
      await secureFetch('/api/auth/logout', {method: 'POST', skipAuthHandling: true});
    }
  } catch (_) {
    // Local cleanup still runs if the network is unavailable.
  }
  _clearAuthState();
  clearSensitiveBrowserData();
}

function handleAuthFailure() {
  const activeTab = localStorage.getItem(STORAGE_KEYS.activeTab) || 'search';
  _clearAuthState();
  clearSensitiveBrowserData();
  if (LOCKED_TABS.has(activeTab)) {
    showLockModal(activeTab);
    const error = document.getElementById('lockError');
    if (error) error.textContent = 'Votre session a expire. Entrez le mot de passe de nouveau.';
  }
}

function enforceSessionExpiry() {
  if (_authState.authenticated && !isUnlocked()) handleAuthFailure();
}

function setAuthenticatedUsername(username) {
  _authState.username = String(username || 'appareil');
}

window.AppLock = {
  isUnlocked,
  csrfToken,
  initializeAuth,
  updateLockUi,
  showLockModal,
  closeLockModal,
  dismissLockModal,
  unlockApp,
  lockApp,
  handleAuthFailure,
  enforceSessionExpiry,
  clearSensitiveBrowserData,
  setAuthenticatedUsername,
};
