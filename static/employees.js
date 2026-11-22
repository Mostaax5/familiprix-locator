// Account administration never stores passwords or session tokens in browser storage.
let employeeRows = [];

function updateEmployeeAccountUi() {
  const mode = Boolean(_authState.accountMode);
  const loggedIn = isUnlocked();
  const owner = loggedIn && _authState.role === 'owner';
  const field = document.getElementById('employeeLoginField');
  if (field) field.hidden = !mode;
  const profile = document.getElementById('legacyEditorProfile');
  if (profile) profile.hidden = mode;
  const legacyInfo = document.getElementById('editorSessionInfo');
  if (legacyInfo) legacyInfo.hidden = mode;
  const info = document.getElementById('employeeAccountInfo');
  if (info) {
    info.hidden = !mode;
    info.textContent = loggedIn
      ? `${_authState.displayName || _authState.username} · ${{owner: 'Propriétaire', employee: 'Employé', viewer: 'Lecture seule'}[_authState.role] || ''}`
      : 'Connectez-vous avec votre compte pour modifier les données du magasin.';
  }
  const manage = document.getElementById('employeeManageButton');
  if (manage) manage.hidden = !owner;
  const password = document.getElementById('employeePasswordButton');
  if (password) password.hidden = !(mode && loggedIn);
  document.querySelectorAll('[data-owner-only]').forEach(element => { element.hidden = mode && !owner; });
  if (!owner) document.getElementById('employeeManagerDialog')?.close();
  if (!loggedIn) {
    employeeRows = [];
    document.getElementById('employeePasswordDialog')?.close();
    ['employeeAccountPassword', 'employeeCurrentPassword', 'employeeNewPassword'].forEach(id => {
      const input = document.getElementById(id);
      if (input) input.value = '';
    });
  }
}

function resetEmployeeForm(account = null) {
  document.getElementById('employeeEditId').value = account?.id || '';
  document.getElementById('employeeAccountLogin').value = account?.login || '';
  document.getElementById('employeeDisplayName').value = account?.display_name || '';
  document.getElementById('employeeRole').value = account?.role || 'employee';
  document.getElementById('employeeActive').checked = account ? Boolean(account.active) : true;
  const password = document.getElementById('employeeAccountPassword');
  password.value = '';
  password.required = !account;
  document.getElementById('employeeFormTitle').textContent = account ? 'Modifier le compte' : 'Ajouter un employé';
  document.getElementById('employeeFormError').textContent = '';
}

async function loadEmployees() {
  const {res, data} = await apiFetch('/api/admin/employees');
  if (!res.ok) throw new Error(data?.error || 'Impossible de charger les comptes.');
  employeeRows = data.employees || [];
  const list = document.getElementById('employeeList');
  list.replaceChildren();
  employeeRows.forEach(account => {
    const row = document.createElement('div');
    row.className = 'employee-row';
    const label = document.createElement('span');
    label.textContent = `${account.display_name} (${account.login}) · ${{owner: 'Propriétaire', employee: 'Employé', viewer: 'Lecture seule'}[account.role]}${account.active ? '' : ' · Désactivé'}`;
    const edit = document.createElement('button');
    edit.type = 'button';
    edit.textContent = 'Modifier';
    edit.addEventListener('click', () => resetEmployeeForm(account));
    row.append(label, edit);
    list.append(row);
  });
}

async function openEmployeeManager() {
  if (!isUnlocked() || _authState.role !== 'owner') return;
  resetEmployeeForm();
  document.getElementById('employeeManagerDialog').showModal();
  try { await loadEmployees(); }
  catch (error) { document.getElementById('employeeFormError').textContent = error.message; }
}

async function saveEmployeeAccount() {
  const button = document.getElementById('employeeSaveButton');
  const error = document.getElementById('employeeFormError');
  const id = document.getElementById('employeeEditId').value;
  button.disabled = true;
  error.textContent = '';
  try {
    const {res, data} = await apiFetch('/api/admin/employees' + (id ? '/' + encodeURIComponent(id) : ''), {
      method: id ? 'PATCH' : 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({login: document.getElementById('employeeAccountLogin').value,
        display_name: document.getElementById('employeeDisplayName').value,
        role: document.getElementById('employeeRole').value,
        active: document.getElementById('employeeActive').checked,
        password: document.getElementById('employeeAccountPassword').value}),
    });
    document.getElementById('employeeAccountPassword').value = '';
    if (!res.ok) throw new Error(data?.error || 'Enregistrement impossible.');
    resetEmployeeForm();
    await initializeAuth(); // Self changes revoke the old session too.
    if (isUnlocked()) await loadEmployees();
  } catch (failure) { error.textContent = failure.message; }
  finally { button.disabled = false; }
}

function openEmployeePassword() {
  if (!isUnlocked() || !_authState.accountMode) return;
  document.getElementById('employeeCurrentPassword').value = '';
  document.getElementById('employeeNewPassword').value = '';
  document.getElementById('employeePasswordError').textContent = '';
  document.getElementById('employeePasswordDialog').showModal();
}

async function saveEmployeePassword() {
  const button = document.getElementById('employeePasswordSave');
  const error = document.getElementById('employeePasswordError');
  button.disabled = true;
  error.textContent = '';
  try {
    const {res, data} = await apiFetch('/api/auth/password', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({current_password: document.getElementById('employeeCurrentPassword').value,
        new_password: document.getElementById('employeeNewPassword').value}),
    });
    if (!res.ok) throw new Error(data?.error || 'Modification impossible.');
    _applyAuthState(data);
    document.getElementById('employeeCurrentPassword').value = '';
    document.getElementById('employeeNewPassword').value = '';
    document.getElementById('employeePasswordDialog').close();
  } catch (failure) { error.textContent = failure.message; }
  finally { button.disabled = false; }
}
