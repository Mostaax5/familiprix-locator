// Client tab: AI interprets the conversation; deterministic retrieval returns
// every matching product from the real mapped store plan.
let _clientRagController = null;
let _clientImagePollTimer = null;
let _latestClientResult = null;
let _clientSelectedQuote = '';
let _clientFocusedProductId = '';
let clientConversation = [];

function getClientQuestion() {
  return document.getElementById('clientQuestion')?.value.trim() || '';
}

function getClientConversationForStorage() {
  return clientConversation.slice(-12).map(message => ({
    role: message.role,
    content: message.content,
  }));
}

function restoreClientConversation(messages) {
  if (!Array.isArray(messages)) return;
  clientConversation = messages.slice(-12).filter(message =>
    message && ['user', 'assistant'].includes(message.role) && String(message.content || '').trim()
  ).map(message => ({role: message.role, content: String(message.content).trim()}));
  renderClientConversation();
}

function clientHistoryPayload() {
  return clientConversation.slice(-10).map(message => ({
    role: message.role,
    content: message.content,
  }));
}

function clientProductDomId(product) {
  const key = String(product.client_id || product.id || product.barcode || product.name || 'product');
  return `client-product-${key.replace(/[^a-zA-Z0-9_-]+/g, '-')}`;
}

function clientProductLink(product, label) {
  return `<a class="client-product-link" href="#${clientProductDomId(product)}">${esc(label || product.name)}</a>`;
}

function cleanClientAnswer(answer) {
  return String(answer || '')
    .replace(/\*\*/g, '')
    .replace(/__/g, '')
    .replace(/^#{1,6}\s*/gm, '')
    .trim();
}

function linkifyClientAnswer(answer, products) {
  const text = cleanClientAnswer(answer);
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

function highlightedClientProducts(result) {
  const products = Array.isArray(result?.products) ? result.products : [];
  const ids = new Set((result?.highlighted_product_ids || []).map(String));
  return products.filter(product => ids.has(String(product.client_id || '')));
}

function clientDirectReplyMarkup() {
  return `
    <div class="client-direct-reply">
      <div class="client-reply-title">Continuer cette conversation</div>
      <div id="clientQuotePreview" class="client-quote-preview"${_clientSelectedQuote ? '' : ' hidden'}>
        <span id="clientQuoteText">${esc(_clientSelectedQuote)}</span>
        <button type="button" class="client-quote-remove" onclick="clearClientSelectedQuote()" title="Retirer la citation" aria-label="Retirer la citation">×</button>
      </div>
      <div class="client-reply-row">
        <textarea id="clientFollowupQuestion" rows="2" placeholder="Posez une question sur cette réponse…" onkeydown="onClientFollowupKeydown(event)"></textarea>
        <button class="btn btn-inline client-reply-send" id="clientFollowupButton" onclick="submitClientFollowup()">Envoyer</button>
      </div>
    </div>`;
}

function renderLatestAssistantDetails(result) {
  const advice = result?.advice || {};
  const products = Array.isArray(result?.products) ? result.products : [];
  const highlighted = highlightedClientProducts(result);
  const links = highlighted.map(product => clientProductLink(product)).join('<span class="client-link-sep"> · </span>');
  return `
    <div class="client-response-mode ${result?.response_mode === 'lookup' ? 'is-fast' : 'is-detailed'}">
      ${result?.response_mode === 'lookup' ? 'Recherche rapide' : 'Réponse détaillée'}
    </div>
    <div class="client-chat-answer">${linkifyClientAnswer(result?.answer || advice.summary || '', products)}</div>
    ${links ? `<div class="client-answer-products"><span>Produits cités :</span> ${links}</div>` : ''}
    ${advice.follow_up_questions?.length ? `
      <div class="advice-section">
        <span class="advice-label">À préciser avec le client</span>
        <div class="advice-list">${advice.follow_up_questions.map(item => `<button type="button" class="advice-item client-followup-suggestion" data-question="${encodeURIComponent(String(item))}" onclick="useClientFollowup(decodeURIComponent(this.dataset.question))">${esc(item)}</button>`).join('')}</div>
      </div>` : ''}
    ${advice.safety_flags?.length ? `
      <div class="advice-section">
        <span class="advice-label">Points de vigilance</span>
        <div class="advice-list">${advice.safety_flags.map(item => `<div class="advice-item advice-item-flag">${esc(item)}</div>`).join('')}</div>
      </div>` : ''}
    ${advice.pharmacist_referral ? `<div class="msg error client-referral">Orienter vers le pharmacien. ${esc(advice.pharmacist_reason || '')}</div>` : ''}
    ${!advice.pharmacist_referral && advice.pharmacist_reason ? `<div class="msg info client-referral">${esc(advice.pharmacist_reason)}</div>` : ''}
    ${result?.response_mode !== 'lookup' ? `<div id="aiFeedbackRow" class="client-feedback-row">
      <span>Cette réponse aide ?</span>
      <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('up')">Oui</button>
      <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('down')">Non</button>
    </div>` : ''}
    ${clientDirectReplyMarkup()}
  `;
}

function renderClientConversation() {
  const target = document.getElementById('clientAdvice');
  if (!target) return;
  if (!clientConversation.length) {
    target.innerHTML = '';
    return;
  }
  const lastIndex = clientConversation.length - 1;
  target.innerHTML = `<div class="client-conversation" aria-live="polite">
    ${clientConversation.map((message, index) => {
      const isLatestAssistant = index === lastIndex && message.role === 'assistant';
      return `<div class="client-message client-message-${message.role}">
        <div class="client-message-label">${message.role === 'user' ? 'Demande' : 'Réponse'}</div>
        ${isLatestAssistant
          ? (_latestClientResult
              ? renderLatestAssistantDetails(_latestClientResult)
              : `<div class="client-chat-answer">${esc(cleanClientAnswer(message.content))}</div>${clientDirectReplyMarkup()}`)
          : `<div class="client-chat-answer">${esc(cleanClientAnswer(message.content))}</div>`}
      </div>`;
    }).join('')}
  </div>`;
  target.onmouseup = captureClientAnswerSelection;
  target.ontouchend = captureClientAnswerSelection;
}

function productInitials(product) {
  const words = String(product.name || 'Produit').trim().split(/\s+/).filter(Boolean);
  return words.slice(0, 2).map(word => word[0]).join('').toUpperCase() || 'P';
}

function clientProductImage(product) {
  return `<div class="client-product-media" data-client-image-id="${esc(product.id || '')}">
    <div class="client-product-image-fallback">${esc(productInitials(product))}</div>
    ${product.image_url ? `<img src="${esc(product.image_url)}" alt="${esc(product.name)}" loading="lazy" onload="this.previousElementSibling.hidden=true" onerror="this.remove()">` : ''}
  </div>`;
}

function clientProductLocations(product) {
  const locations = Array.isArray(product.locations) && product.locations.length
    ? product.locations
    : [{aisle: product.aisle, side: product.side, section: product.section, shelf: product.shelf, position: product.position}];
  return locations.map(location => `
    <span class="client-location-pill">
      Allée ${esc(location.aisle)} · ${esc(sideStaffLabel(location.side))} · S${esc(location.section || '1')} T${esc(location.shelf)} P${esc(location.position)}
    </span>`).join('');
}

function openClientProductDetails(candidateId) {
  const product = currentClientMatches.find(item => String(item.client_id || '') === String(candidateId || ''));
  const modal = document.getElementById('clientProductModal');
  const content = document.getElementById('clientProductDetailContent');
  if (!product || !modal || !content) return;
  _clientFocusedProductId = String(product.client_id || '');
  const description = product.description || product.usage_notes || 'Aucune description détaillée disponible pour ce produit.';
  const searchTerms = Array.isArray(product.search_terms) ? product.search_terms.join(', ') : String(product.search_terms || '');
  content.innerHTML = `
    <div class="client-product-detail-head">
      <div class="client-product-detail-media">
        <div class="client-product-image-fallback">${esc(productInitials(product))}</div>
        ${product.image_url ? `<img src="${esc(product.image_url)}" alt="${esc(product.name)}" onload="this.previousElementSibling.hidden=true" onerror="this.remove()">` : ''}
      </div>
      <div class="client-product-detail-title">
        <div class="client-result-badges">
          <span class="client-plan-badge ${product.is_plano ? 'is-plano' : 'is-hors-plano'}">${product.is_plano ? 'PLANO' : 'HORS-PLANO'}</span>
          ${product.in_stock === 0 ? '<span class="client-stock-badge">RUPTURE</span>' : ''}
        </div>
        <h2>${esc(product.name)}</h2>
        ${product.brand ? `<div class="client-result-brand">${esc(product.brand)}</div>` : ''}
      </div>
    </div>
    <div class="client-product-detail-section">
      <div class="client-product-detail-label">Emplacement en magasin</div>
      <div class="client-result-locations">${clientProductLocations(product)}</div>
    </div>
    <div class="client-product-detail-section">
      <div class="client-product-detail-label">Description</div>
      <div class="client-product-detail-copy">${esc(description)}</div>
    </div>
    ${product.usage_notes && product.usage_notes !== description ? `<div class="client-product-detail-section"><div class="client-product-detail-label">Aide client</div><div class="client-product-detail-copy">${esc(product.usage_notes)}</div></div>` : ''}
    ${searchTerms ? `<div class="client-product-detail-section"><div class="client-product-detail-label">Termes associés</div><div class="client-product-detail-copy">${esc(searchTerms)}</div></div>` : ''}
    <div class="client-product-detail-codes">
      ${product.barcode ? `<span>UPC ${esc(product.barcode)}</span>` : ''}
      ${product.product_code ? `<span>Code pharmacie ${esc(product.product_code)}</span>` : ''}
    </div>`;
  const input = document.getElementById('clientProductQuestion');
  if (input) input.value = '';
  modal.hidden = false;
  document.body.classList.add('modal-open');
  window.setTimeout(() => input?.focus(), 50);
}

function closeClientProductDetails() {
  const modal = document.getElementById('clientProductModal');
  if (modal) modal.hidden = true;
  _clientFocusedProductId = '';
  document.body.classList.remove('modal-open');
}

function onClientProductModalClick(event) {
  if (event.target?.id === 'clientProductModal') closeClientProductDetails();
}

async function askAboutClientProduct() {
  const input = document.getElementById('clientProductQuestion');
  const question = input?.value.trim() || '';
  if (!question) {
    input?.focus();
    return;
  }
  const focusProductId = _clientFocusedProductId;
  closeClientProductDetails();
  await runClientRequest(question, {followUp: true, focusProductId});
}

function clientProductCard(product) {
  const description = product.usage_notes || product.description || '';
  return `<article class="client-result-card${product.in_stock === 0 ? ' is-out-of-stock' : ''}" id="${clientProductDomId(product)}" data-client-id="${esc(product.client_id || '')}" role="button" tabindex="0" onclick="openClientProductDetails(this.dataset.clientId)" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();openClientProductDetails(this.dataset.clientId)}">
    ${clientProductImage(product)}
    <div class="client-result-body">
      <div class="client-result-badges">
        <span class="client-plan-badge ${product.is_plano ? 'is-plano' : 'is-hors-plano'}">${product.is_plano ? 'PLANO' : 'HORS-PLANO'}</span>
        ${product.in_stock === 0 ? '<span class="client-stock-badge">RUPTURE</span>' : ''}
        ${isHomeBrand(product.brand) ? '<span class="client-home-badge">MARQUE MAISON</span>' : ''}
      </div>
      <h3>${esc(product.name)}</h3>
      ${product.brand ? `<div class="client-result-brand">${esc(product.brand)}</div>` : ''}
      <div class="client-result-locations">${clientProductLocations(product)}</div>
      ${description ? `<div class="client-result-description">${esc(description)}</div>` : ''}
      <div class="client-result-codes">
        ${product.barcode ? `<span>UPC ${esc(product.barcode)}</span>` : ''}
        ${product.product_code ? `<span>Code ${esc(product.product_code)}</span>` : ''}
        <span class="client-detail-affordance">Voir détails</span>
      </div>
    </div>
  </article>`;
}

function renderClientMatches(matches) {
  const target = document.getElementById('clientMatches');
  if (!target) return;
  if (!matches.length) {
    target.innerHTML = '<div class="empty">Aucun produit correspondant dans le plan actuel du magasin.</div>';
    return;
  }
  target.innerHTML = `
    <section class="client-products-section">
      <div class="client-products-heading">
        <div>
          <div class="section-title">Produits proposés</div>
          <div class="section-note">${matches.length} produit(s) trouvé(s) dans le plan actuel du magasin.</div>
        </div>
        <span class="client-plan-source">PLAN MAGASIN</span>
      </div>
      <div class="client-results-list">${matches.map(clientProductCard).join('')}</div>
    </section>
  `;
}

function updateClientQuotePreview() {
  const preview = document.getElementById('clientQuotePreview');
  const text = document.getElementById('clientQuoteText');
  if (text) text.textContent = _clientSelectedQuote;
  if (preview) preview.hidden = !_clientSelectedQuote;
}

function clearClientSelectedQuote() {
  _clientSelectedQuote = '';
  updateClientQuotePreview();
}

function captureClientAnswerSelection() {
  window.setTimeout(() => {
    const selection = window.getSelection?.();
    const quote = String(selection?.toString() || '').trim().replace(/\s+/g, ' ').slice(0, 500);
    if (!quote || !selection?.rangeCount) return;
    const node = selection.getRangeAt(0).commonAncestorContainer;
    const element = node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
    const answer = element?.closest?.('.client-message-assistant .client-chat-answer');
    if (!answer || !document.getElementById('clientAdvice')?.contains(answer)) return;
    _clientSelectedQuote = quote;
    updateClientQuotePreview();
    document.getElementById('clientFollowupQuestion')?.focus();
  }, 0);
}

function useClientFollowup(question) {
  const input = document.getElementById('clientFollowupQuestion');
  if (!input) return;
  input.value = String(question || '');
  input.focus();
}

function onClientFollowupKeydown(event) {
  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    submitClientFollowup();
  }
}

function submitClientFollowup() {
  const input = document.getElementById('clientFollowupQuestion');
  const question = input?.value.trim() || '';
  if (!question) {
    input?.focus();
    return;
  }
  return runClientRequest(question, {followUp: true, selectedText: _clientSelectedQuote});
}

function resetClientSearchResults(showStatus=true) {
  if (_clientRagController) {
    _clientRagController.abort();
    _clientRagController = null;
  }
  if (_clientImagePollTimer) window.clearTimeout(_clientImagePollTimer);
  _latestClientResult = null;
  _clientSelectedQuote = '';
  _clientFocusedProductId = '';
  currentClientMatches = [];
  clientConversation = [];
  const advice = document.getElementById('clientAdvice');
  const matches = document.getElementById('clientMatches');
  if (advice) advice.innerHTML = '';
  if (matches) matches.innerHTML = '';
  const button = document.getElementById('clientFindButton');
  if (button) button.disabled = false;
  if (showStatus) {
    const status = document.getElementById('clientHelpStatus');
    if (status) status.textContent = 'Écrivez une demande, puis cliquez sur « Trouver produits ».';
  }
  persistClientDraft();
}

function onClientQuestionInput() {
  if (_clientRagController) {
    _clientRagController.abort();
    _clientRagController = null;
    const button = document.getElementById('clientFindButton');
    if (button) button.disabled = false;
  }
  const status = document.getElementById('clientHelpStatus');
  if (status) status.textContent = clientConversation.length
    ? 'Cliquez sur « Trouver produits » pour envoyer cette question de suivi.'
    : 'Cliquez sur « Trouver produits » pour lancer la recherche.';
}

async function sendAiFeedback(rating) {
  const row = document.getElementById('aiFeedbackRow');
  if (row) row.innerHTML = '<span class="client-feedback-thanks">Merci</span>';
  const lastQuestion = [...clientConversation].reverse().find(message => message.role === 'user')?.content || '';
  try {
    await apiFetch('/api/ai/feedback', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({question: lastQuestion, rating, store: (typeof getCurrentStoreName === 'function' ? getCurrentStoreName() : '')})
    });
  } catch (_) {}
}

function updateClientImages(images) {
  let updated = 0;
  for (const product of currentClientMatches) {
    const imageUrl = images?.[String(product.id)] || '';
    if (!imageUrl || product.image_url) continue;
    product.image_url = imageUrl;
    const media = document.querySelector(`[data-client-image-id="${product.id}"]`);
    if (!media || media.querySelector('img')) continue;
    const img = document.createElement('img');
    img.src = imageUrl;
    img.alt = product.name;
    img.loading = 'lazy';
    img.onload = () => { const fallback = media.querySelector('.client-product-image-fallback'); if (fallback) fallback.hidden = true; };
    img.onerror = () => img.remove();
    media.appendChild(img);
    updated += 1;
  }
  return updated;
}

async function pollClientProductImages(attempt=0) {
  const missingIds = currentClientMatches.filter(product => product.id && !product.image_url).map(product => product.id);
  if (!missingIds.length || attempt >= 6) return;
  const data = await apiGetProductImages(missingIds);
  updateClientImages(data.images || {});
  if (currentClientMatches.some(product => product.id && !product.image_url)) {
    _clientImagePollTimer = window.setTimeout(() => pollClientProductImages(attempt + 1), 5000);
  }
}

async function runClientRequest(question, options={}) {
  question = String(question || '').trim();
  const status = document.getElementById('clientHelpStatus');
  const button = document.getElementById('clientFindButton');
  const followupButton = document.getElementById('clientFollowupButton');
  if (!question) {
    if (status) status.textContent = 'Écrivez d’abord la demande du client.';
    document.getElementById('clientQuestion')?.focus();
    return;
  }

  if (_clientRagController) _clientRagController.abort();
  if (_clientImagePollTimer) window.clearTimeout(_clientImagePollTimer);
  const history = clientHistoryPayload();
  const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  _clientRagController = controller;
  if (button) button.disabled = true;
  if (followupButton) followupButton.disabled = true;
  if (status) status.textContent = options.followUp
    ? `${aiProviderLabel()} approfondit la réponse avec les produits déjà affichés…`
    : 'Recherche dans le plan actuel du magasin…';

  const result = await apiGenerateClientHelp({
    question,
    history,
    follow_up: Boolean(options.followUp),
    selected_text: options.selectedText || '',
    focus_product_id: options.focusProductId || '',
    context_product_ids: currentClientMatches.map(product => product.client_id).filter(Boolean).slice(0, 80),
  }, controller?.signal);
  if (controller && _clientRagController !== controller) return;
  _clientRagController = null;
  if (button) button.disabled = false;
  if (followupButton) followupButton.disabled = false;
  if (!result.success) {
    if (status) status.textContent = result.error || 'Recherche indisponible pour le moment.';
    return;
  }

  const products = Array.isArray(result.products) ? result.products.map(normalizeProduct) : [];
  result.products = products;
  if (result.response_mode === 'lookup') {
    result.answer = products.length
      ? `${products.length} produit(s) correspondant(s) sont dans le plan du magasin. Ouvrez une carte pour voir sa description, ses codes et tous ses emplacements.`
      : 'Aucun produit correspondant n’est présent dans le plan actuel du magasin.';
    result.advice = {...(result.advice || {}), summary: result.answer};
  }
  _latestClientResult = result;
  currentClientMatches = products;
  clientConversation.push({role: 'user', content: question});
  clientConversation.push({role: 'assistant', content: result.answer || result.advice?.summary || ''});
  clientConversation = clientConversation.slice(-12);
  const input = document.getElementById('clientQuestion');
  if (input) input.value = '';
  const followupInput = document.getElementById('clientFollowupQuestion');
  if (followupInput) followupInput.value = '';
  _clientSelectedQuote = '';
  persistClientDraft();
  renderClientConversation();
  renderClientMatches(products);
  pollClientProductImages();
  if (status) {
    const timing = result.elapsed_ms ? ` en ${(result.elapsed_ms / 1000).toFixed(1)} s` : '';
    status.textContent = result.response_mode === 'lookup'
      ? `Recherche rapide${timing} : ${products.length} produit(s) trouvé(s) dans le plan.`
      : `Réponse détaillée${timing}. Vous pouvez répondre directement sous la réponse.`;
  }
  document.getElementById('clientFollowupQuestion')?.focus();
}

function findClientProducts() {
  return runClientRequest(getClientQuestion(), {followUp: false});
}

// Compatibility hooks for older installed service-worker shells.
function scheduleClientSearch() {}
function runClientSearch() { return Promise.resolve([]); }
function generateClientHelp() { return findClientProducts(); }

window.AppAI = {
  findClientProducts, resetClientSearchResults, onClientQuestionInput,
  getClientConversationForStorage, restoreClientConversation,
};
