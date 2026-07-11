// ── Client tab ────────────────────────────────────────────────────────────────
function scheduleClientSearch() {
  window.clearTimeout(clientSearchTimer);
  // Longer debounce: the client search hits the server, so we wait for a pause in
  // typing rather than firing a request per keystroke.
  clientSearchTimer = window.setTimeout(() => runClientSearch(false), 450);
}

function getClientQuestion() {
  return document.getElementById('clientQuestion')?.value.trim() || '';
}

function renderClientMatches(matches, question) {
  const target = document.getElementById('clientMatches');
  if (!target) return;
  if (!question) { target.innerHTML = ''; return; }
  if (!matches.length) { target.innerHTML = '<div class="empty">Aucun produit correspondant pour le moment.</div>'; return; }
  target.innerHTML = `
    <div class="card">
      <div class="section-title">Produits proposes</div>
      <div class="section-note">Resultats trouves avec la base du magasin. L IA ne remplace pas cette liste.</div>
      ${matches.map(product => productCard(product, false, false)).join('')}
    </div>
  `;
}

function renderClientAdvice(advice, hasStoreProducts) {
  const target = document.getElementById('clientAdvice');
  if (!target) return;
  if (!advice) { target.innerHTML = ''; return; }
  const generalNotice = !hasStoreProducts
    ? `<div class="msg warning" style="margin-bottom:10px;font-weight:600;">⚠ Aucun produit du magasin n a ete trouve pour cette demande. Cette réponse est basee sur des connaissances generales — verifiez l inventaire avant de conseiller.</div>`
    : '';
  target.innerHTML = `
    <div class="advice-card">
      <div class="section-title">Réponse client</div>
      ${generalNotice}
      ${advice.summary ? `<div class="advice-summary">${esc(advice.summary)}</div>` : ''}
      ${advice.recommended_products?.length ? `
        <div class="advice-section">
          <span class="advice-label">Produits recommandes (en magasin)</span>
          <div class="advice-list">${advice.recommended_products.map(p => `
            <div class="advice-item advice-item-product">
              <div style="font-weight:600">${esc(p.name)}${p.home_brand ? ' <span style="color:#c8102e">★</span>' : ''}</div>
              ${p.brand ? `<div class="small" style="color:#64748b">${esc(p.brand)}</div>` : ''}
              ${p.location ? `<div class="small" style="color:#c8102e;font-weight:600;margin-top:2px">📍 ${esc(p.location)}</div>` : ''}
            </div>`).join('')}</div>
        </div>`
      : (advice.recommended_product_names?.length ? `
        <div class="advice-section">
          <span class="advice-label">Produits a expliquer</span>
          <div class="advice-list">${advice.recommended_product_names.map(item => `<div class="advice-item advice-item-product">${esc(item)}</div>`).join('')}</div>
        </div>` : '')}
      ${advice.follow_up_questions?.length ? `
        <div class="advice-section">
          <span class="advice-label">Questions a poser</span>
          <div class="advice-list">${advice.follow_up_questions.map(item => `<div class="advice-item">${esc(item)}</div>`).join('')}</div>
        </div>` : ''}
      ${advice.safety_flags?.length ? `
        <div class="advice-section">
          <span class="advice-label">Points de vigilance</span>
          <div class="advice-list">${advice.safety_flags.map(item => `<div class="advice-item advice-item-flag">${esc(item)}</div>`).join('')}</div>
        </div>` : ''}
      ${advice.pharmacist_referral ? `<div class="msg error" style="margin-top:12px">Orienter vers le pharmacien. ${esc(advice.pharmacist_reason || '')}</div>` : ''}
      ${!advice.pharmacist_referral && advice.pharmacist_reason ? `<div class="msg info" style="margin-top:12px">${esc(advice.pharmacist_reason)}</div>` : ''}
      <div id="aiFeedbackRow" style="margin-top:12px;display:flex;align-items:center;gap:8px;color:#94a3b8;font-size:12px">
        <span>Utile ?</span>
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:3px 10px;width:auto;margin:0" onclick="sendAiFeedback('up')">👍</button>
        <button class="btn btn-outline btn-inline" style="font-size:13px;padding:3px 10px;width:auto;margin:0" onclick="sendAiFeedback('down')">👎</button>
      </div>
    </div>
  `;
}

// Optional, non-blocking. Ignoring it costs nothing; tapping it just records
// a training signal. Never interrupts the workflow.
async function sendAiFeedback(rating) {
  const row = document.getElementById('aiFeedbackRow');
  if (row) row.innerHTML = '<span style="color:#16a34a;font-size:12px">Merci 🙏</span>';
  try {
    await apiFetch('/api/ai/feedback', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({question: getClientQuestion(), rating, store: (typeof getCurrentStoreName==='function'?getCurrentStoreName():'')})
    });
  } catch (_) {}
}

// ONE ranked list from the server: placed products (with location) + catalogue products
// (position à confirmer), best match first. Fast and works with NO AI.
// The placed products are ALSO searched instantly on the device (same scorer as the
// Search tab), so the tab shows results immediately and still works when the server
// is waking up (Render free tier sleeps) or unreachable — the server response then
// replaces the local list because it adds the catalogue products.
let _clientFindController = null;

async function runClientSearch(showEmptyMessage=true) {
  const question = getClientQuestion();
  const status = document.getElementById('clientHelpStatus');
  renderClientAdvice(null);
  if (!question) {
    currentClientMatches = [];
    renderClientMatches([], '');
    if (status && showEmptyMessage) status.textContent = 'Ecrivez la demande du client pour voir les produits.';
    return [];
  }
  // minScore 100 = same noise floor as the server's /api/client/find.
  const localMatches = allProductsCache.length ? searchProductsFromCache(question, 30, 100) : [];
  currentClientMatches = localMatches;
  renderClientMatches(localMatches, question);
  if (status) {
    status.textContent = localMatches.length
      ? `${localMatches.length} produit(s) en magasin. Recherche du catalogue…`
      : 'Recherche des produits…';
  }
  // Abort the previous in-flight request so slow responses can't pile up
  // behind each other on the server while the user types.
  if (_clientFindController) _clientFindController.abort();
  const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  _clientFindController = controller;
  let matches = [];
  try { matches = await apiClientFind(question, 30, controller?.signal); } catch (_) {}
  if (getClientQuestion() !== question) return currentClientMatches;   // stale — user kept typing
  if (controller && _clientFindController !== controller) return currentClientMatches; // superseded
  if (!matches.length && localMatches.length) matches = localMatches;  // server down → keep local results
  currentClientMatches = matches;
  renderClientMatches(matches, question);
  if (status) {
    status.textContent = matches.length
      ? `${matches.length} produit(s) trouvé(s). « Réponse client (IA) » est optionnel.`
      : 'Aucun produit trouvé pour cette demande.';
  }
  return matches;
}

async function generateClientHelp() {
  const question = getClientQuestion();
  const status = document.getElementById('clientHelpStatus');
  if (!question) {
    if (status) status.textContent = 'Ecrivez d’abord la demande du client.';
    return;
  }
  if (!backendInfo.ai_enabled) {
    if (status) status.textContent = 'IA inactive. Ajoutez une clé IA sur Render pour la réponse client guidee.';
    renderClientAdvice(null);
    return;
  }
  // The product list refreshes in parallel — the SERVER builds the AI's store
  // context from the question itself (UPC-aware, full product families), so the
  // answer no longer depends on what this phone's search happened to find.
  runClientSearch(false);
  if (status) status.textContent = `Génération de la réponse via ${aiProviderLabel()}… (quelques secondes)`;
  const result = await apiGenerateClientHelp({question});
  if (!result.success || !result.advice) {
    renderClientAdvice(null);
    if (status) status.textContent = result.error || 'Reponse client indisponible.';
    return;
  }
  const hasStoreProducts = (result.advice.recommended_products || []).length > 0 || currentClientMatches.length > 0;
  renderClientAdvice(result.advice, hasStoreProducts);
  if (status) status.textContent = `Réponse generee via ${aiProviderLabel()}. Verifiez avant de conseiller.`;
}

window.AppAI = { runClientSearch, generateClientHelp, scheduleClientSearch };
