// ── GitHub Gist backup ────────────────────────────────────────────────────────
async function loadGistStatus() {
  const statusEl = document.getElementById('gistStatusMsg');
  const actionsEl = document.getElementById('gistActions');
  try {
    const {res, data} = await apiFetch('/api/gist/status');
    if (!res.ok) throw new Error();
    if (data.configured) {
      statusEl.textContent = 'Sauvegarde automatique GitHub active. Chaque produit scanné est sauvegarde automatiquement.';
      if (actionsEl) actionsEl.style.display = '';
    } else if (data.has_token && !data.has_gist_id) {
      statusEl.innerHTML = 'GITHUB_TOKEN configure. Scannez un premier produit pour créer le gist, puis ajoutez <strong>GITHUB_GIST_ID</strong> dans les variables Render.';
      if (actionsEl) actionsEl.style.display = '';
    } else {
      statusEl.innerHTML = 'Sauvegarde GitHub non configurée. Ajoutez <strong>GITHUB_TOKEN</strong> et <strong>GITHUB_GIST_ID</strong> dans les variables d\'environnement Render pour activer la sauvegarde automatique.';
      if (actionsEl) actionsEl.style.display = 'none';
    }
  } catch (e) {
    statusEl.textContent = 'Impossible de vérifier le statut GitHub.';
  }
}

async function gistBackupNow() {
  const msg = document.getElementById('gistMsg');
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Sauvegarde en cours...'; }
  try {
    const {res, data} = await apiFetch('/api/gist/backup', {method: 'POST', headers: getEditorHeaders()});
    if (!res.ok || !data.success) throw new Error(data.error || 'Echec');
    if (msg) { msg.className = 'msg success'; msg.textContent = 'Sauvegarde réussie sur GitHub Gist.'; }
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de la sauvegarde GitHub.'; }
  }
}

async function gistRestoreNow() {
  const msg = document.getElementById('gistMsg');
  if (!confirm('Restaurer les données depuis GitHub Gist? Les produits existants seront conserves (fusion).')) return;
  if (msg) { msg.className = 'msg info'; msg.textContent = 'Restauration en cours...'; }
  try {
    const {res, data} = await apiFetch('/api/gist/restore', {method: 'POST', headers: getEditorHeaders()});
    if (!res.ok || !data.success) throw new Error(data.error || 'Echec');
    if (msg) { msg.className = 'msg success'; msg.textContent = `Restauration terminée: ${data.imported_products} produit(s) et ${data.imported_layouts} allée(s) importes.`; }
    await refreshProductsCache(true);
    await refreshLayoutsCache(true);
    renderMapEditor();
  } catch (e) {
    if (msg) { msg.className = 'msg error'; msg.textContent = e.message || 'Erreur lors de la restauration GitHub.'; }
  }
}

window.AppGist = { loadGistStatus, gistBackupNow, gistRestoreNow };
