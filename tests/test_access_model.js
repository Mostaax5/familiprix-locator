const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

function element(initial = {}) {
  return {
    style: {},
    textContent: '',
    value: '',
    disabled: false,
    focus() { this.focused = true; },
    ...initial,
  };
}

const elements = new Map([
  ['tabBtn-search', element()],
  ['tabBtn-client', element()],
  ['tabBtn-scan', element()],
  ['tabBtn-add', element()],
  ['lockButton', element()],
  ['lockModal', element({style: {display: 'none'}})],
  ['lockPasswordInput', element()],
  ['lockError', element()],
  ['lockLoginButton', element()],
]);
const storage = new Map();
const resumedTabs = [];
let protectedResetCount = 0;
let authenticated = false;
let loginAttempts = 0;

const context = {
  console,
  LOCKED_TABS: new Set(['scan', 'add']),
  STORAGE_KEYS: {
    activeTab: 'activeTab',
    editorSession: 'editorSession',
    scanDraft: 'scanDraft',
    addDraft: 'addDraft',
  },
  document: {
    getElementById(id) { return elements.get(id) || null; },
  },
  localStorage: {
    getItem(key) { return storage.get(String(key)) || null; },
    setItem(key, value) { storage.set(String(key), String(value)); },
    removeItem(key) { storage.delete(String(key)); },
  },
  async secureFetch(url) {
    if (url === '/api/auth/status') {
      return {ok: true, async json() { return {authenticated}; }};
    }
    if (url === '/api/auth/login') {
      loginAttempts += 1;
      if (loginAttempts === 1) {
        return {ok: false, status: 503, async json() {
          return {code: 'auth_unavailable', error: 'temporary'};
        }};
      }
      authenticated = true;
      return {ok: true, status: 200, async json() {
        return {
          authenticated: true,
          csrf_token: 'csrf-test-token',
          expires_at: Math.floor(Date.now() / 1000) + 3600,
          username: 'appareil',
        };
      }};
    }
    if (url === '/api/auth/logout') {
      authenticated = false;
      return {ok: true, async json() { return {success: true}; }};
    }
    throw new Error(`Unexpected URL: ${url}`);
  },
  window: {
    setTimeout(callback) { callback(); },
    resetAuthenticatedAppState() { protectedResetCount += 1; },
    async resumeAuthenticatedApp(tab) { resumedTabs.push(tab); },
  },
};
vm.createContext(context);
vm.runInContext(fs.readFileSync('static/lock.js', 'utf8'), context);

async function run() {
  const lock = context.window.AppLock;
  const initiallyAuthenticated = await lock.initializeAuth();
  assert.strictEqual(initiallyAuthenticated, false);
  assert.strictEqual(elements.get('lockModal').style.display, 'none', 'public startup must stay modal-free');
  assert.strictEqual(elements.get('tabBtn-search').textContent, 'Recherche');
  assert.strictEqual(elements.get('tabBtn-client').textContent, 'Client');
  assert(elements.get('tabBtn-scan').textContent.endsWith('🔒'));
  assert(elements.get('tabBtn-add').textContent.endsWith('🔒'));

  lock.showLockModal('scan');
  assert.strictEqual(elements.get('lockModal').style.display, 'flex');
  lock.dismissLockModal();
  assert.strictEqual(elements.get('lockModal').style.display, 'none');

  lock.showLockModal('scan');
  elements.get('lockPasswordInput').value = 'test-only-password';
  await lock.unlockApp();
  assert.strictEqual(loginAttempts, 2, 'a transient startup failure should retry once');
  assert.strictEqual(elements.get('lockModal').style.display, 'none');
  assert.deepStrictEqual(resumedTabs, ['scan']);
  assert.strictEqual(elements.get('tabBtn-scan').textContent, 'Scan');

  await lock.lockApp();
  assert(elements.get('tabBtn-scan').textContent.endsWith('🔒'));
  assert(protectedResetCount >= 2, 'protected drafts should be reset without clearing public access');
  console.log('access model tests passed');
}

run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
