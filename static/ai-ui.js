// Client tab: AI interprets the conversation; deterministic retrieval returns
// every matching product from the real mapped store plan.
let _clientRagController = null;
let _clientRequestSequence = 0;
let _clientImagePollTimer = null;
let _latestClientResult = null;
let _clientSelectedQuote = '';
let _clientQuoteFocusProductId = '';
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

function clientProductForStorage(product) {
  return {
    id: product.id ?? null,
    client_id: String(product.client_id || ''),
    name: String(product.name || ''),
    brand: String(product.brand || ''),
    description: String(product.description || ''),
    image_url: String(product.image_url || ''),
    search_terms: String(product.search_terms || ''),
    usage_notes: String(product.usage_notes || ''),
    barcode: String(product.barcode || ''),
    product_code: String(product.product_code || ''),
    facings: Number(product.facings) || 1,
    aisle: String(product.aisle || ''),
    side: String(product.side || ''),
    section: String(product.section || '1'),
    shelf: String(product.shelf || ''),
    position: String(product.position || ''),
    locations: Array.isArray(product.locations) ? product.locations.slice(0, 20) : [],
    is_plano: Number(product.is_plano) ? 1 : 0,
    in_stock: product.in_stock === 0 ? 0 : 1,
  };
}

function getClientSearchStateForStorage() {
  const products = currentClientMatches.slice(0, 100).map(clientProductForStorage);
  const advice = _latestClientResult?.advice || {};
  const latestResult = _latestClientResult ? {
    response_mode: _latestClientResult.response_mode || 'detailed',
    answer: String(_latestClientResult.answer || advice.summary || ''),
    highlighted_product_ids: Array.isArray(_latestClientResult.highlighted_product_ids)
      ? _latestClientResult.highlighted_product_ids.slice(0, 12).map(String)
      : [],
    advice: {
      summary: String(advice.summary || ''),
      follow_up_questions: Array.isArray(advice.follow_up_questions)
        ? advice.follow_up_questions.slice(0, 4).map(String) : [],
      safety_flags: Array.isArray(advice.safety_flags)
        ? advice.safety_flags.slice(0, 5).map(String) : [],
      pharmacist_referral: Boolean(advice.pharmacist_referral),
      pharmacist_reason: String(advice.pharmacist_reason || ''),
    },
  } : null;
  return {products, latest_result: latestResult};
}

function restoreClientSearchState(state) {
  if (!state || typeof state !== 'object') return;
  const rawProducts = Array.isArray(state.products)
    ? state.products
    : (Array.isArray(state.latest_result?.products) ? state.latest_result.products : []);
  if (!state.latest_result && !rawProducts.length) {
    updateClientHistoryAction();
    return;
  }
  const products = rawProducts.slice(0, 100).map(normalizeProduct);
  currentClientMatches = products;
  _latestClientResult = state.latest_result && typeof state.latest_result === 'object'
    ? {...state.latest_result, products}
    : null;
  renderClientConversation();
  renderClientMatches(products);
  if (products.length) pollClientProductImages();
  updateClientHistoryAction();
}

function restoreClientConversation(messages) {
  if (!Array.isArray(messages)) return;
  clientConversation = messages.slice(-12).filter(message =>
    message && ['user', 'assistant'].includes(message.role) && String(message.content || '').trim()
  ).map(message => ({role: message.role, content: String(message.content).trim()}));
  renderClientConversation();
  updateClientHistoryAction();
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

function clientQuoteButton(quote, focusProductId='', label='Citer ce passage') {
  const cleanQuote = String(quote || '').trim().replace(/\s+/g, ' ').slice(0, 500);
  return `<button type="button" class="client-quote-action" data-quote="${encodeURIComponent(cleanQuote)}" data-focus-product="${esc(focusProductId)}" onclick="event.stopPropagation();quoteClientPassage(decodeURIComponent(this.dataset.quote),this.dataset.focusProduct)" title="${esc(label)}" aria-label="${esc(label)}">&#8220;</button>`;
}

function renderQuotableClientAnswer(answer, products=[]) {
  const text = cleanClientAnswer(answer);
  if (!text) return '';
  const passages = [];
  for (const line of text.split(/\n+/).map(item => item.trim()).filter(Boolean)) {
    if (line.length <= 280) {
      passages.push(line);
      continue;
    }
    const sentences = line.match(/[^.!?]+[.!?]+(?:\s+|$)|[^.!?]+$/g) || [line];
    let passage = '';
    for (const rawSentence of sentences) {
      const sentence = rawSentence.trim();
      if (!sentence) continue;
      if (passage && `${passage} ${sentence}`.length > 280) {
        passages.push(passage);
        passage = sentence;
      } else {
        passage = passage ? `${passage} ${sentence}` : sentence;
      }
    }
    if (passage) passages.push(passage);
  }
  return passages.map(passage => `
    <div class="client-answer-paragraph">
      <div class="client-answer-paragraph-text">${linkifyClientAnswer(passage, products)}</div>
      ${clientQuoteButton(passage)}
    </div>`).join('');
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
    <div class="client-chat-answer">${renderQuotableClientAnswer(result?.answer || advice.summary || '', products)}</div>
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
    updateClientHistoryAction();
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
              : `<div class="client-chat-answer">${renderQuotableClientAnswer(message.content)}</div>${clientDirectReplyMarkup()}`)
          : (message.role === 'assistant'
              ? `<div class="client-chat-answer">${renderQuotableClientAnswer(message.content)}</div>`
              : `<div class="client-chat-answer">${esc(cleanClientAnswer(message.content))}</div>`)}
      </div>`;
    }).join('')}
  </div>`;
  target.onmouseup = captureClientTextSelection;
  target.ontouchend = captureClientTextSelection;
  updateClientHistoryAction();
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
  modal.dataset.clientId = _clientFocusedProductId;
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
      <div class="client-product-detail-label-row">
        <div class="client-product-detail-label">Description</div>
        ${clientQuoteButton(`${product.name}: ${description}`, _clientFocusedProductId, 'Citer cette description')}
      </div>
      <div class="client-product-detail-copy">${esc(description)}</div>
    </div>
    ${product.usage_notes && product.usage_notes !== description ? `<div class="client-product-detail-section"><div class="client-product-detail-label-row"><div class="client-product-detail-label">Aide client</div>${clientQuoteButton(`${product.name}: ${product.usage_notes}`, _clientFocusedProductId, 'Citer cette information')}</div><div class="client-product-detail-copy">${esc(product.usage_notes)}</div></div>` : ''}
    ${searchTerms ? `<div class="client-product-detail-section"><div class="client-product-detail-label">Termes associés</div><div class="client-product-detail-copy">${esc(searchTerms)}</div></div>` : ''}
    <div class="client-product-detail-codes">
      ${product.barcode ? `<span>UPC ${esc(product.barcode)}</span>` : ''}
      ${product.product_code ? `<span>Code pharmacie ${esc(product.product_code)}</span>` : ''}
    </div>`;
  const input = document.getElementById('clientProductQuestion');
  if (input) input.value = '';
  modal.hidden = false;
  document.body.classList.add('modal-open');
  content.onmouseup = captureClientTextSelection;
  content.ontouchend = captureClientTextSelection;
  updateClientQuotePreview();
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
  const selectedText = _clientQuoteFocusProductId === focusProductId ? _clientSelectedQuote : '';
  closeClientProductDetails();
  await runClientRequest(question, {followUp: true, focusProductId, selectedText});
}

function clientProductCard(product) {
  const description = product.usage_notes || product.description || '';
  const productQuote = description ? `${product.name}: ${description}` : '';
  return `<article class="client-result-card${product.in_stock === 0 ? ' is-out-of-stock' : ''}" id="${clientProductDomId(product)}" data-client-id="${esc(product.client_id || '')}" onclick="if(!(window.getSelection?.()?.toString()||'').trim())openClientProductDetails(this.dataset.clientId)">
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
      ${description ? `<div class="client-result-description-row"><div class="client-result-description">${esc(description)}</div>${clientQuoteButton(productQuote, String(product.client_id || ''), 'Citer cette description')}</div>` : ''}
      <div class="client-result-codes">
        ${product.barcode ? `<span>UPC ${esc(product.barcode)}</span>` : ''}
        ${product.product_code ? `<span>Code ${esc(product.product_code)}</span>` : ''}
        <button type="button" class="client-detail-affordance" onclick="event.stopPropagation();openClientProductDetails(this.closest('.client-result-card').dataset.clientId)">Voir détails</button>
      </div>
    </div>
  </article>`;
}

function renderClientMatches(matches) {
  const target = document.getElementById('clientMatches');
  if (!target) return;
  if (!matches.length) {
    target.innerHTML = '<div class="empty">Aucun produit correspondant dans le plan actuel du magasin.</div>';
    updateClientHistoryAction();
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
  target.onmouseup = captureClientTextSelection;
  target.ontouchend = captureClientTextSelection;
  updateClientHistoryAction();
}

function updateClientQuotePreview() {
  const preview = document.getElementById('clientQuotePreview');
  const text = document.getElementById('clientQuoteText');
  if (text) text.textContent = _clientSelectedQuote;
  if (preview) preview.hidden = !_clientSelectedQuote;
  const productPreview = document.getElementById('clientProductQuotePreview');
  const productText = document.getElementById('clientProductQuoteText');
  const isFocusedProductQuote = Boolean(
    _clientSelectedQuote && _clientQuoteFocusProductId &&
    _clientQuoteFocusProductId === _clientFocusedProductId
  );
  if (productText) productText.textContent = isFocusedProductQuote ? _clientSelectedQuote : '';
  if (productPreview) productPreview.hidden = !isFocusedProductQuote;

  document.querySelectorAll('.client-quote-action').forEach(button => {
    const quote = decodeURIComponent(button.dataset.quote || '');
    const focus = button.dataset.focusProduct || '';
    button.classList.toggle(
      'is-active', quote === _clientSelectedQuote && focus === _clientQuoteFocusProductId
    );
  });
}

function clearClientSelectedQuote() {
  _clientSelectedQuote = '';
  _clientQuoteFocusProductId = '';
  updateClientQuotePreview();
}

function quoteClientPassage(quote, focusProductId='') {
  const cleaned = String(quote || '').trim().replace(/\s+/g, ' ').slice(0, 500);
  if (!cleaned) return;
  _clientSelectedQuote = cleaned;
  _clientQuoteFocusProductId = String(focusProductId || '');
  updateClientQuotePreview();

  const modal = document.getElementById('clientProductModal');
  const useProductReply = Boolean(
    modal && !modal.hidden && _clientQuoteFocusProductId === _clientFocusedProductId
  );
  const input = document.getElementById(
    useProductReply ? 'clientProductQuestion' : 'clientFollowupQuestion'
  );
  if (!useProductReply) {
    input?.scrollIntoView({behavior: 'smooth', block: 'center'});
  }
  window.setTimeout(() => input?.focus(), 80);
}

function captureClientTextSelection() {
  window.setTimeout(() => {
    const selection = window.getSelection?.();
    const quote = String(selection?.toString() || '').trim().replace(/\s+/g, ' ').slice(0, 500);
    if (!quote || !selection?.rangeCount) return;
    const node = selection.getRangeAt(0).commonAncestorContainer;
    const element = node.nodeType === Node.ELEMENT_NODE ? node : node.parentElement;
    const answer = element?.closest?.('.client-message-assistant .client-chat-answer');
    if (answer && document.getElementById('clientAdvice')?.contains(answer)) {
      quoteClientPassage(quote);
      return;
    }
    const productCopy = element?.closest?.('.client-result-description, .client-product-detail-copy');
    if (!productCopy) return;
    const card = element.closest?.('.client-result-card');
    const modal = element.closest?.('#clientProductModal');
    const candidateId = card?.dataset.clientId || modal?.dataset.clientId || '';
    const product = currentClientMatches.find(
      item => String(item.client_id || '') === String(candidateId)
    );
    quoteClientPassage(product ? `${product.name}: ${quote}` : quote, candidateId);
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
  return runClientRequest(question, {
    followUp: true,
    selectedText: _clientSelectedQuote,
    focusProductId: _clientQuoteFocusProductId,
  });
}

function updateClientHistoryAction() {
  const button = document.getElementById('clientClearHistoryButton');
  if (!button) return;
  button.hidden = !(
    clientConversation.length || currentClientMatches.length || _latestClientResult
  );
}

function clearClientHistory() {
  resetClientSearchResults(false);
  const status = document.getElementById('clientHelpStatus');
  if (status) status.textContent = 'Historique effacé. La demande en cours est conservée.';
  updateClientHistoryAction();
  document.getElementById('clientQuestion')?.focus();
}

function resetClientSearchResults(showStatus=true) {
  _clientRequestSequence += 1;
  if (_clientRagController) {
    _clientRagController.abort();
    _clientRagController = null;
  }
  if (_clientImagePollTimer) window.clearTimeout(_clientImagePollTimer);
  _latestClientResult = null;
  _clientSelectedQuote = '';
  _clientQuoteFocusProductId = '';
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
  updateClientHistoryAction();
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
  if (updated) persistClientDraft();
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

function localClientMatches(question, limit=60) {
  if (typeof searchProductsFromCache !== 'function' || !allProductsCache.length) return [];
  const rawMatches = searchProductsFromCache(question, Math.min(limit * 2, 100), 100);
  const grouped = [];
  const byKey = new Map();
  for (const raw of rawMatches) {
    const barcode = typeof normalizedDigits === 'function' ? normalizedDigits(raw.barcode) : String(raw.barcode || '');
    const nameKey = typeof normalizeSearchText === 'function'
      ? normalizeSearchText(`${raw.name || ''} ${raw.brand || ''}`)
      : `${raw.name || ''}|${raw.brand || ''}`.toLowerCase();
    const key = barcode ? `barcode:${barcode}` : `name:${nameKey}`;
    const location = {
      aisle: String(raw.aisle || ''), side: String(raw.side || 'Gauche'),
      section: String(raw.section || '1'), shelf: String(raw.shelf || ''),
      position: String(raw.position || ''),
    };
    if (byKey.has(key)) {
      const product = byKey.get(key);
      if (!product.locations.some(item => JSON.stringify(item) === JSON.stringify(location))) {
        product.locations.push(location);
      }
      if (!product.image_url && raw.image_url) product.image_url = raw.image_url;
      continue;
    }
    const product = normalizeProduct({
      ...raw,
      client_id: raw.client_id || (raw.id != null ? `product:${raw.id}` : key),
      locations: [location],
    });
    byKey.set(key, product);
    grouped.push(product);
    if (grouped.length >= limit) break;
  }
  return grouped;
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
  const requestId = ++_clientRequestSequence;
  const history = clientHistoryPayload();
  const contextProductIds = currentClientMatches
    .map(product => product.client_id).filter(Boolean).slice(0, 80);
  const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  _clientRagController = controller;
  if (button) button.disabled = true;
  if (followupButton) followupButton.disabled = true;
  if (!options.followUp) _latestClientResult = null;
  let visibleProducts = options.followUp ? currentClientMatches : localClientMatches(question, 60);
  if (!options.followUp) {
    currentClientMatches = visibleProducts;
    if (visibleProducts.length) {
      renderClientMatches(visibleProducts);
      persistClientDraft();
    } else {
      const matches = document.getElementById('clientMatches');
      if (matches) matches.innerHTML = '';
    }
  }
  if (status) {
    status.textContent = options.followUp
      ? `${aiProviderLabel()} approfondit la réponse avec les produits déjà affichés…`
      : visibleProducts.length
        ? `${visibleProducts.length} produit(s) affiché(s) depuis le plan. ${aiProviderLabel()} prépare la réponse…`
        : 'Recherche dans le plan actuel du magasin…';
  }

  let requestFinalized = false;
  const fastProductsPromise = options.followUp
    ? Promise.resolve([])
    : apiClientFind(question, 60, controller?.signal);
  void fastProductsPromise.then(products => {
    if (requestFinalized || requestId !== _clientRequestSequence) return;
    const normalized = Array.isArray(products) ? products.map(normalizeProduct) : [];
    if (!normalized.length && visibleProducts.length) return;
    visibleProducts = normalized;
    currentClientMatches = normalized;
    renderClientMatches(normalized);
    persistClientDraft();
    if (status) status.textContent = normalized.length
      ? `${normalized.length} produit(s) trouvé(s) dans le plan. ${aiProviderLabel()} prépare la réponse…`
      : `${aiProviderLabel()} vérifie la demande…`;
  });

  const result = await apiGenerateClientHelp({
    question,
    history,
    follow_up: Boolean(options.followUp),
    selected_text: options.selectedText || '',
    focus_product_id: options.focusProductId || '',
    context_product_ids: contextProductIds,
  }, controller?.signal);
  if (requestId !== _clientRequestSequence) return;
  requestFinalized = true;
  _clientRagController = null;
  if (button) button.disabled = false;
  if (followupButton) followupButton.disabled = false;
  if (!result.success) {
    if (status) {
      const suffix = currentClientMatches.length
        ? ` Les ${currentClientMatches.length} produit(s) trouvé(s) restent disponibles ci-dessous.`
        : '';
      status.textContent = `${result.error || 'Recherche indisponible pour le moment.'}${suffix}`;
    }
    persistClientDraft();
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
  _clientQuoteFocusProductId = '';
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
  getClientSearchStateForStorage, restoreClientSearchState, clearClientHistory,
};
