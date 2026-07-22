// Server-backed employee authentication. The password and session token are
// never stored in JavaScript or localStorage; the server session uses an
// HttpOnly cookie and this page keeps only the CSRF token in memory.
let _pendingLockedTab = null;
let _authFailureShown = false;
const _authState = {
  authenticated: false,
  csrfToken: '',
  expiresAt: 0,
  rotationRequired: false,
  username: '',
};

function csrfToken() {
  return _authState.csrfToken;
}

function isUnlocked() {
  const valid = Boolean(
    _authState.authenticated &&
    _authState.csrfToken &&
    Date.now() < (_authState.expiresAt * 1000)
  );
  if (!valid && _authState.authenticated) {
    window.setTimeout(() => handleAuthFailure(401, {code: 'authentication_required'}), 0);
  }
  return valid;
}

function _applyAuthState(data) {
  _authState.authenticated = Boolean(data?.authenticated);
  _authState.csrfToken = String(data?.csrf_token || '');
  _authState.expiresAt = Number(data?.expires_at || 0);
  _authState.rotationRequired = Boolean(data?.rotation_required);
  _authState.username = String(data?.username || '');
  _authFailureShown = false;
  updateLockUi();
}

function _clearAuthState() {
  _authState.authenticated = false;
  _authState.csrfToken = '';
  _authState.expiresAt = 0;
  _authState.rotationRequired = false;
  _authState.username = '';
  updateLockUi();
}

function clearSensitiveBrowserData() {
  [
    STORAGE_KEYS.planSnapshot,
    STORAGE_KEYS.clientDraft,
    STORAGE_KEYS.scanDraft,
    STORAGE_KEYS.addDraft,
    'familiprixPlanMoveUndo',
  ].filter(Boolean).forEach(key => localStorage.removeItem(key));
  window.resetAuthenticatedAppState?.();
}

function updateLockUi() {
  const unlocked = isUnlocked();
  const labels = {
    'tabBtn-search': 'Recherche',
    'tabBtn-client': 'Client',
    'tabBtn-scan': 'Scan',
    'tabBtn-add': 'Plan',
  };
  for (const [id, label] of Object.entries(labels)) {
    const button = document.getElementById(id);
    if (button) button.textContent = `${label}${unlocked ? '' : ' 🔒'}`;
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
  if (pendingTab) _pendingLockedTab = pendingTab;
  const modal = document.getElementById('lockModal');
  if (!modal) return;
  modal.style.display = 'flex';
  document.getElementById('lockLoginPanel')?.removeAttribute('hidden');
  document.getElementById('lockRotationPanel')?.setAttribute('hidden', '');
  const nameInput = document.getElementById('lockEditorName');
  if (nameInput && !nameInput.value) nameInput.value = _savedEditorName();
  window.setTimeout(() => document.getElementById('lockPasswordInput')?.focus(), 60);
}

function closeLockModal() {
  if (!isUnlocked() || _authState.rotationRequired) return;
  const modal = document.getElementById('lockModal');
  if (modal) modal.style.display = 'none';
  for (const id of ['lockPasswordInput', 'lockNewPassword', 'lockNewPasswordConfirm']) {
    const input = document.getElementById(id);
    if (input) input.value = '';
  }
  const error = document.getElementById('lockError');
  if (error) error.textContent = '';
  const rotationError = document.getElementById('lockRotationError');
  if (rotationError) rotationError.textContent = '';
}

function _showRotationPanel() {
  const modal = document.getElementById('lockModal');
  if (modal) modal.style.display = 'flex';
  document.getElementById('lockLoginPanel')?.setAttribute('hidden', '');
  document.getElementById('lockRotationPanel')?.removeAttribute('hidden');
  window.setTimeout(() => document.getElementById('lockNewPassword')?.focus(), 60);
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
      showLockModal();
      if (!response.ok) {
        const error = document.getElementById('lockError');
        if (error) error.textContent = data.error || 'Le serveur se prépare. Réessayez dans un instant.';
      }
      return false;
    }
    _applyAuthState(data);
    if (_authState.rotationRequired) {
      _showRotationPanel();
      return false;
    }
    closeLockModal();
    return true;
  } catch (_) {
    _clearAuthState();
    clearSensitiveBrowserData();
    showLockModal();
    const error = document.getElementById('lockError');
    if (error) error.textContent = 'Impossible de joindre le serveur pour le moment.';
    return false;
  }
}

async function unlockApp() {
  const passwordInput = document.getElementById('lockPasswordInput');
  const nameInput = document.getElementById('lockEditorName');
  const error = document.getElementById('lockError');
  const button = document.getElementById('lockLoginButton');
  if (!passwordInput || !passwordInput.value) {
    if (error) error.textContent = 'Entrez le mot de passe.';
    passwordInput?.focus();
    return;
  }
  if (button) { button.disabled = true; button.textContent = 'Vérification…'; }
  if (error) error.textContent = '';
  try {
    const response = await secureFetch('/api/auth/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({password: passwordInput.value, username: nameInput?.value || ''}),
      skipAuthHandling: true,
    });
    const data = await response.json();
    passwordInput.value = '';
    if (!response.ok || !data.authenticated) {
      if (error) error.textContent = data.error || 'Connexion refusée.';
      passwordInput.focus();
      return;
    }
    _applyAuthState(data);
    localStorage.setItem(STORAGE_KEYS.editorSession, JSON.stringify({username: data.username || 'appareil'}));
    if (_authState.rotationRequired) {
      _showRotationPanel();
      return;
    }
    const pending = _pendingLockedTab;
    _pendingLockedTab = null;
    closeLockModal();
    await window.resumeAuthenticatedApp?.(pending);
  } catch (_) {
    if (error) error.textContent = 'Impossible de joindre le serveur pour le moment.';
  } finally {
    if (button) { button.disabled = false; button.textContent = 'Déverrouiller'; }
  }
}

async function rotateAppPassword() {
  const password = document.getElementById('lockNewPassword');
  const confirmation = document.getElementById('lockNewPasswordConfirm');
  const error = document.getElementById('lockRotationError');
  const button = document.getElementById('lockRotationButton');
  if (!password || password.value.length < 15) {
    if (error) error.textContent = 'Utilisez au moins 15 caractères.';
    password?.focus();
    return;
  }
  if (password.value !== confirmation?.value) {
    if (error) error.textContent = 'Les deux mots de passe ne correspondent pas.';
    confirmation?.focus();
    return;
  }
  if (button) { button.disabled = true; button.textContent = 'Protection…'; }
  if (error) error.textContent = '';
  try {
    const response = await secureFetch('/api/auth/password', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({new_password: password.value}),
      skipAuthHandling: true,
    });
    const data = await response.json();
    if (!response.ok || !data.authenticated) {
      if (error) error.textContent = data.error || 'Impossible de remplacer le mot de passe.';
      return;
    }
    _applyAuthState(data);
    password.value = '';
    if (confirmation) confirmation.value = '';
    const pending = _pendingLockedTab;
    _pendingLockedTab = null;
    closeLockModal();
    await window.resumeAuthenticatedApp?.(pending);
  } catch (_) {
    if (error) error.textContent = 'Impossible de joindre le serveur pour le moment.';
  } finally {
    if (button) { button.disabled = false; button.textContent = 'Activer le nouveau mot de passe'; }
  }
}

async function lockApp() {
  try {
    if (isUnlocked()) {
      await secureFetch('/api/auth/logout', {method: 'POST', skipAuthHandling: true});
    }
  } catch (_) {
    // Local cleanup is mandatory even if the network disappeared.
  }
  _clearAuthState();
  clearSensitiveBrowserData();
  showLockModal();
}

function handleAuthFailure(status, payload={}) {
  if (status === 428 || payload.code === 'password_rotation_required') {
    _authState.rotationRequired = true;
    _showRotationPanel();
    return;
  }
  if (_authFailureShown && !_authState.authenticated) return;
  _authFailureShown = true;
  _clearAuthState();
  clearSensitiveBrowserData();
  showLockModal();
  const error = document.getElementById('lockError');
  if (error) error.textContent = 'Votre session a expiré. Déverrouillez de nouveau.';
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
  unlockApp,
  rotateAppPassword,
  lockApp,
  handleAuthFailure,
  clearSensitiveBrowserData,
  setAuthenticatedUsername,
};
