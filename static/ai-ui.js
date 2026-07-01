// ── Client tab ────────────────────────────────────────────────────────────────
function scheduleClientSearch() {
  window.clearTimeout(clientSearchTimer);
  clientSearchTimer = window.setTimeout(() => runClientSearch(false), 220);
}

function getClientQuestion() {
  return document.getElementById('clientQuestion')?.value.trim() || '';
}

function sanitizeProductForClientAi(product) {
  return {
    name: product.name || '', brand: product.brand || '', description: product.description || '',
    usage_notes: product.usage_notes || '', search_terms: product.search_terms || '',
    alternative_suggestions: product.alternative_suggestions || '', barcode: product.barcode || '',
    aisle: product.aisle || '', side: product.side || '', section: product.section || '',
    shelf: product.shelf || '', position: product.position || ''
  };
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

// Append catalogue products (imported planograms, not placed) under the client matches.
async function appendClientReferenceMatches(question) {
  const target = document.getElementById('clientMatches');
  if (!target || !question) return;
  let ref = [];
  try { ref = await apiSearchReference(question, 20); } catch (_) {}
  if (getClientQuestion() !== question) return;   // user moved on — ignore stale results
  if (!ref.length) return;
  target.insertAdjacentHTML('beforeend', `
    <div class="card" style="margin-top:10px">
      <div class="section-title">📦 Aussi en magasin — position à confirmer</div>
      <div class="section-note">Produits importés des planogrammes, pas encore placés sur le plan.</div>
      ${ref.map(p => productCard(p, false, false)).join('')}
    </div>`);
}

function runClientSearch(showEmptyMessage=true) {
  const question = getClientQuestion();
  const status = document.getElementById('clientHelpStatus');
  renderClientAdvice(null);
  if (!question) {
    currentClientMatches = [];
    renderClientMatches([], '');
    if (status && showEmptyMessage) status.textContent = 'Ecrivez la question du client pour voir les produits proposes.';
    return [];
  }
  currentClientMatches = searchProductsFromCache(question, 20);
  renderClientMatches(currentClientMatches, question);
  if (status) {
    status.textContent = currentClientMatches.length
      ? `${currentClientMatches.length} produit(s) en stock. Cliquez "Réponse client (IA)" pour obtenir des conseils.`
      : 'Aucun produit en stock pour cette demande. L IA peut quand meme repondre avec des conseils generaux.';
  }
  // Also surface catalogue products (imported planograms, not placed yet) below.
  appendClientReferenceMatches(question);
  return currentClientMatches;
}

async function generateClientHelp() {
  const question = getClientQuestion();
  const status = document.getElementById('clientHelpStatus');
  const matches = runClientSearch(false);
  if (!question) {
    if (status) status.textContent = 'Ecrivez d’abord la question du client.';
    return;
  }
  if (!backendInfo.ai_enabled) {
    if (status) status.textContent = 'IA inactive. Ajoutez GEMINI_API_KEY sur Render pour la réponse client guidee.';
    renderClientAdvice(null);
    return;
  }
  if (status) {
    status.textContent = matches.length
      ? `Generation de la réponse via ${aiProviderLabel()} (${matches.length} produit(s) en stock)...`
      : `Aucun produit en stock. L IA va repondre avec des conseils generaux...`;
  }
  const result = await apiGenerateClientHelp({question, products: matches.slice(0, 20).map(sanitizeProductForClientAi)});
  if (!result.success || !result.advice) {
    renderClientAdvice(null);
    if (status) status.textContent = result.error || 'Reponse client indisponible.';
    return;
  }
  renderClientAdvice(result.advice, matches.length > 0);
  if (status) status.textContent = `Réponse generee via ${aiProviderLabel()}. Verifiez avant de conseiller.`;
}

window.AppAI = { runClientSearch, generateClientHelp, scheduleClientSearch };
