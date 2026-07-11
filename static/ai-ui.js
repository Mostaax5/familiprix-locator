// Client tab: one click runs query planning, hybrid retrieval, AI verification,
// and the grounded answer. Typing never searches or displays products.
let _clientRagController = null;

function getClientQuestion() {
  return document.getElementById('clientQuestion')?.value.trim() || '';
}

function clientProductDomId(product) {
  const key = String(product.client_id || product.id || product.barcode || product.name || 'product');
  return `client-product-${key.replace(/[^a-zA-Z0-9_-]+/g, '-')}`;
}

function clientProductLink(product, label) {
  return `<a class="client-product-link" href="#${clientProductDomId(product)}">${esc(label || product.name)}</a>`;
}

function linkifyClientAnswer(answer, products) {
  const text = String(answer || '');
  const named = [...products]
    .filter(product => String(product.name || '').trim())
    .sort((a, b) => String(b.name).length - String(a.name).length);
  if (!named.length) return esc(text);

  const escapedNames = named.map(product => String(product.name).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const pattern = new RegExp(`(${escapedNames.join('|')})`, 'gi');
  const byName = new Map(named.map(product => [String(product.name).toLocaleLowerCase(), product]));
  return text.split(pattern).map(part => {
    const product = byName.get(String(part).toLocaleLowerCase());
    return product ? clientProductLink(product, part) : esc(part);
  }).join('');
}

function resetClientSearchResults(showStatus=true) {
  if (_clientRagController) {
    _clientRagController.abort();
    _clientRagController = null;
  }
  currentClientMatches = [];
  const advice = document.getElementById('clientAdvice');
  const matches = document.getElementById('clientMatches');
  if (advice) advice.innerHTML = '';
  if (matches) matches.innerHTML = '';
  const button = document.getElementById('clientFindButton');
  if (button) button.disabled = false;
  if (showStatus) {
    const status = document.getElementById('clientHelpStatus');
    if (status) status.textContent = getClientQuestion()
      ? 'Cliquez sur « Trouver produits » pour lancer la recherche.'
      : 'Écrivez la demande du client, puis cliquez sur « Trouver produits ».';
  }
}

function onClientQuestionInput() {
  resetClientSearchResults(true);
}

function renderClientMatches(matches) {
  const target = document.getElementById('clientMatches');
  if (!target) return;
  if (!matches.length) {
    target.innerHTML = '<div class="empty">Aucun produit suffisamment lié à cette demande.</div>';
    return;
  }
  target.innerHTML = `
    <div class="client-products-section">
      <div class="section-title">Produits proposés</div>
      <div class="section-note">Produits vérifiés par rapport à la demande et trouvés dans la base.</div>
      ${matches.map(product => `
        <div class="client-product-anchor" id="${clientProductDomId(product)}">
          ${productCard(product, false, false)}
        </div>`).join('')}
    </div>
  `;
}

function renderClientResponse(result) {
  const target = document.getElementById('clientAdvice');
  if (!target) return;
  const advice = result.advice || {};
  const products = Array.isArray(result.products) ? result.products : [];
  const answer = result.answer || advice.summary || '';
  const productLinks = products.map(product => clientProductLink(product)).join('<span class="client-link-sep"> · </span>');
  const noProductNotice = products.length ? '' : `
    <div class="msg warning" style="margin-bottom:10px;font-weight:600">
      Aucun produit de la base n’a passé la vérification de pertinence.
    </div>`;
  target.innerHTML = `
    <div class="advice-card">
      <div class="section-title">Réponse</div>
      ${noProductNotice}
      <div class="advice-summary client-answer-text">${linkifyClientAnswer(answer, products)}</div>
      ${productLinks ? `<div class="client-answer-products"><span>Produits retenus :</span> ${productLinks}</div>` : ''}
      ${advice.follow_up_questions?.length ? `
        <div class="advice-section">
          <span class="advice-label">Questions à poser</span>
          <div class="advice-list">${advice.follow_up_questions.map(item => `<div class="advice-item">${esc(item)}</div>`).join('')}</div>
        </div>` : ''}
      ${advice.safety_flags?.length ? `
        <div class="advice-section">
          <span class="advice-label">Points de vigilance</span>
          <div class="advice-list">${advice.safety_flags.map(item => `<div class="advice-item advice-item-flag">${esc(item)}</div>`).join('')}</div>
        </div>` : ''}
      ${advice.pharmacist_referral ? `<div class="msg error" style="margin-top:12px">Orienter vers le pharmacien. ${esc(advice.pharmacist_reason || '')}</div>` : ''}
      ${!advice.pharmacist_referral && advice.pharmacist_reason ? `<div class="msg info" style="margin-top:12px">${esc(advice.pharmacist_reason)}</div>` : ''}
      <div id="aiFeedbackRow" class="client-feedback-row">
        <span>Utile ?</span>
        <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('up')" aria-label="Réponse utile">Oui</button>
        <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('down')" aria-label="Réponse non utile">Non</button>
      </div>
    </div>
  `;
}

async function sendAiFeedback(rating) {
  const row = document.getElementById('aiFeedbackRow');
  if (row) row.innerHTML = '<span style="color:#16a34a;font-size:12px">Merci</span>';
  try {
    await apiFetch('/api/ai/feedback', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({question: getClientQuestion(), rating, store: (typeof getCurrentStoreName === 'function' ? getCurrentStoreName() : '')})
    });
  } catch (_) {}
}

async function findClientProducts() {
  const question = getClientQuestion();
  const status = document.getElementById('clientHelpStatus');
  const button = document.getElementById('clientFindButton');
  if (!question) {
    resetClientSearchResults(false);
    if (status) status.textContent = 'Écrivez d’abord la demande du client.';
    document.getElementById('clientQuestion')?.focus();
    return;
  }
  if (!backendInfo.ai_enabled) {
    resetClientSearchResults(false);
    if (status) status.textContent = 'IA inactive. Ajoutez une clé IA sur Render.';
    return;
  }

  resetClientSearchResults(false);
  const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  _clientRagController = controller;
  if (button) button.disabled = true;
  if (status) status.textContent = `${aiProviderLabel()} analyse la demande, cherche et vérifie les produits…`;

  const result = await apiGenerateClientHelp({question}, controller?.signal);
  if (controller && _clientRagController !== controller) return;
  _clientRagController = null;
  if (button) button.disabled = false;
  if (getClientQuestion() !== question) return;
  if (!result.success) {
    if (status) status.textContent = result.error || 'Recherche indisponible pour le moment.';
    return;
  }

  const products = Array.isArray(result.products) ? result.products.map(normalizeProduct) : [];
  result.products = products;
  currentClientMatches = products;
  renderClientResponse(result);
  renderClientMatches(products);
  if (status) {
    status.textContent = products.length
      ? `${products.length} produit(s) pertinent(s), vérifié(s) via ${aiProviderLabel()}.`
      : `Réponse générée via ${aiProviderLabel()}, sans produit suffisamment pertinent.`;
  }
}

// Kept as a no-op compatibility hook for any older installed app shell. It never
// searches while typing; only findClientProducts performs the request.
function scheduleClientSearch() {}
function runClientSearch() { return Promise.resolve([]); }
function generateClientHelp() { return findClientProducts(); }

window.AppAI = { findClientProducts, resetClientSearchResults, onClientQuestionInput };
