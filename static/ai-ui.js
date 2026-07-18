// Client tab: AI interprets the conversation; deterministic retrieval returns
// every matching product from the real mapped store plan.
let _clientRagController = null;
let _clientRequestSequence = 0;
let _clientImagePollTimer = null;
let _clientWorkingTimer = null;
let _clientWorkingStartedAt = 0;
let _latestClientResult = null;
let _clientSelectedQuote = '';
let _clientQuoteFocusProductId = '';
let _clientFocusedProductId = '';
let _clientVisibleExchangeId = '';
let _clientSearchMode = 'fast';
let clientConversation = [];
const CLIENT_MAX_MESSAGES = 12;
const CLIENT_FAST_PRODUCT_LIMIT = 100;
const CLIENT_CONTEXT_PRODUCT_LIMIT = 80;
const CLIENT_MAX_PRODUCTS_PER_EXCHANGE = 100;

function normalizeClientMode(mode) {
  return ['fast', 'ai', 'documented'].includes(mode) ? mode : 'fast';
}

function normalizeClientResponseMode(mode) {
  return ['lookup', 'detailed', 'documented'].includes(mode) ? mode : 'detailed';
}

function getClientQuestion() {
  return document.getElementById('clientQuestion')?.value.trim() || '';
}

function getClientConversationForStorage() {
  return clientConversation.slice(-CLIENT_MAX_MESSAGES).map(message => {
    const stored = {
      role: message.role,
      content: String(message.content || ''),
      exchange_id: String(message.exchange_id || ''),
      mode: normalizeClientMode(message.mode),
    };
    if (message.role === 'assistant' && message.result) {
      stored.result = clientResultForStorage(message.result);
    }
    return stored;
  });
}

function clientProductForStorage(product, compact=false) {
  return {
    id: product.id ?? null,
    client_id: String(product.client_id || ''),
    name: String(product.name || ''),
    brand: String(product.brand || ''),
    description: String(product.description || '').slice(0, compact ? 700 : 1800),
    image_url: String(product.image_url || '').slice(0, 1600),
    source_url: String(product.source_url || '').slice(0, 1600),
    search_terms: compact ? '' : String(product.search_terms || '').slice(0, 1200),
    usage_notes: String(product.usage_notes || '').slice(0, compact ? 700 : 1800),
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

function clientDocumentationForStorage(documentation) {
  const data = documentation && typeof documentation === 'object' ? documentation : {};
  const sourceIds = value => Array.isArray(value)
    ? value.slice(0, 4).map(item => String(item || '').slice(0, 120))
    : [];
  const textItems = value => (Array.isArray(value) ? value : []).slice(0, 6).map(item => ({
    text: String(item?.text || '').slice(0, 1200),
    source_ids: sourceIds(item?.source_ids),
  })).filter(item => item.text);
  return {
    key_points: (Array.isArray(data.key_points) ? data.key_points : []).slice(0, 6).map(item => ({
      heading: String(item?.heading || '').slice(0, 120),
      detail: String(item?.detail || '').slice(0, 1400),
      source_ids: sourceIds(item?.source_ids),
    })).filter(item => item.heading && item.detail),
    comparisons: (Array.isArray(data.comparisons) ? data.comparisons : []).slice(0, 8).map(item => ({
      candidate_id: String(item?.candidate_id || '').slice(0, 120),
      difference: String(item?.difference || '').slice(0, 1200),
      practical_note: String(item?.practical_note || '').slice(0, 1200),
      source_ids: sourceIds(item?.source_ids),
    })).filter(item => item.candidate_id && (item.difference || item.practical_note)),
    useful_guidance: textItems(data.useful_guidance),
    important_checks: textItems(data.important_checks),
    sources: (Array.isArray(data.sources) ? data.sources : []).slice(0, 15).map(source => ({
      source_id: String(source?.source_id || '').slice(0, 120),
      title: String(source?.title || '').slice(0, 280),
      publisher: String(source?.publisher || '').slice(0, 160),
      url: String(source?.url || '').slice(0, 1600),
      summary: String(source?.summary || '').slice(0, 1200),
      candidate_ids: Array.isArray(source?.candidate_ids)
        ? source.candidate_ids.slice(0, 16).map(String) : [],
    })).filter(source => source.source_id && source.title),
  };
}

function clientResultForStorage(result) {
  if (!result || typeof result !== 'object') return null;
  const advice = result.advice || {};
  return {
    success: true,
    response_mode: normalizeClientResponseMode(result.response_mode),
    answer: String(result.answer || advice.summary || '').slice(0, 6000),
    degraded: Boolean(result.degraded),
    warning: String(result.warning || '').slice(0, 500),
    elapsed_ms: Number(result.elapsed_ms) || 0,
    highlighted_product_ids: Array.isArray(result.highlighted_product_ids)
      ? result.highlighted_product_ids.slice(0, 16).map(String)
      : [],
    assortment_product_ids: Array.isArray(result.assortment_product_ids)
      ? result.assortment_product_ids.slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE).map(String)
      : [],
    products: (Array.isArray(result.products) ? result.products : [])
      .slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE)
      .map(product => clientProductForStorage(product, result.response_mode === 'lookup')),
    advice: {
      summary: String(advice.summary || result.answer || '').slice(0, 6000),
      follow_up_questions: Array.isArray(advice.follow_up_questions)
        ? advice.follow_up_questions.slice(0, 4).map(String) : [],
      safety_flags: Array.isArray(advice.safety_flags)
        ? advice.safety_flags.slice(0, 5).map(String) : [],
      pharmacist_referral: Boolean(advice.pharmacist_referral),
      pharmacist_reason: String(advice.pharmacist_reason || '').slice(0, 1200),
      documentation: clientDocumentationForStorage(advice.documentation),
    },
  };
}

function normalizeStoredClientResult(result) {
  const stored = clientResultForStorage(result);
  if (!stored) return null;
  stored.products = stored.products.map(product => (
    typeof normalizeProduct === 'function' ? normalizeProduct(product) : {...product}
  ));
  return stored;
}

function getClientSearchStateForStorage() {
  const products = currentClientMatches
    .slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE)
    .map(product => clientProductForStorage(product, true));
  const latestResult = clientResultForStorage(_latestClientResult);
  if (latestResult) delete latestResult.products;
  return {
    mode: _clientSearchMode,
    visible_exchange_id: _clientVisibleExchangeId,
    products,
    latest_result: latestResult,
  };
}

function restoreClientSearchState(state) {
  if (!state || typeof state !== 'object') return;
  setClientSearchMode(normalizeClientMode(state.mode), false);
  const rawProducts = Array.isArray(state.products)
    ? state.products
    : (Array.isArray(state.latest_result?.products) ? state.latest_result.products : []);
  if (!state.latest_result && !rawProducts.length) {
    updateClientHistoryAction();
    return;
  }
  const products = rawProducts.slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE).map(product => (
    typeof normalizeProduct === 'function' ? normalizeProduct(product) : {...product}
  ));
  const visibleMessage = findClientAssistantMessage(state.visible_exchange_id);
  currentClientMatches = visibleMessage?.result?.products?.length
    ? visibleMessage.result.products
    : products;
  _clientVisibleExchangeId = visibleMessage?.exchange_id || String(state.visible_exchange_id || '');
  const latestAssistant = [...clientConversation].reverse().find(message => message.role === 'assistant');
  if (latestAssistant?.result) {
    _latestClientResult = latestAssistant.result;
  } else if (state.latest_result && typeof state.latest_result === 'object') {
    _latestClientResult = normalizeStoredClientResult({...state.latest_result, products});
    if (latestAssistant && _latestClientResult) latestAssistant.result = _latestClientResult;
  }
  renderClientConversation();
  renderClientMatches(currentClientMatches);
  if (currentClientMatches.length) pollClientProductImages();
  updateClientHistoryAction();
}

function restoreClientConversation(messages) {
  if (!Array.isArray(messages)) return;
  let pendingExchangeId = '';
  let restoredIndex = 0;
  clientConversation = [];
  for (const raw of messages.slice(-CLIENT_MAX_MESSAGES)) {
    const role = String(raw?.role || '');
    const content = String(raw?.content || '').trim();
    if (!['user', 'assistant'].includes(role) || !content) continue;
    if (role === 'user') {
      pendingExchangeId = String(raw.exchange_id || `restored-${restoredIndex++}`);
    }
    const exchangeId = String(raw.exchange_id || pendingExchangeId || `restored-${restoredIndex++}`);
    const message = {
      role,
      content,
      exchange_id: exchangeId,
      mode: normalizeClientMode(raw.mode),
    };
    if (role === 'assistant' && raw.result) {
      message.result = normalizeStoredClientResult(raw.result);
    }
    clientConversation.push(message);
    if (role === 'assistant') pendingExchangeId = '';
  }
  const latestAssistant = [...clientConversation].reverse().find(message => message.role === 'assistant');
  if (latestAssistant?.result) {
    _latestClientResult = latestAssistant.result;
    currentClientMatches = latestAssistant.result.products || [];
    _clientVisibleExchangeId = latestAssistant.exchange_id;
    renderClientMatches(currentClientMatches);
  }
  renderClientConversation();
  updateClientHistoryAction();
}

function clientHistoryPayload() {
  return clientConversation.slice(-10).map(message => ({
    role: message.role,
    content: message.content,
  }));
}

function getClientSearchMode() {
  return _clientSearchMode;
}

function clientModeLabel(mode) {
  if (mode === 'documented') return 'Réponse documentée';
  if (mode === 'ai') return 'Avec IA';
  return 'Recherche rapide';
}

function setClientSearchMode(mode, shouldPersist=true) {
  _clientSearchMode = normalizeClientMode(mode);
  document.querySelectorAll('[data-client-mode]').forEach(button => {
    const active = button.dataset.clientMode === _clientSearchMode;
    button.classList.toggle('is-active', active);
    button.setAttribute('aria-checked', active ? 'true' : 'false');
  });
  if (!_clientRagController) {
    const status = document.getElementById('clientHelpStatus');
    if (status) {
      status.textContent = _clientSearchMode === 'documented'
        ? `${aiProviderLabel()} approfondira la réponse avec les fiches produit et les sources disponibles.`
        : (_clientSearchMode === 'ai'
          ? `${aiProviderLabel()} répondra et vérifiera les produits du plan.`
          : 'Recherche rapide : noms, images et emplacements du plan.');
    }
  }
  if (shouldPersist && typeof persistClientDraft === 'function') persistClientDraft();
}

function clientProductDomId(product) {
  const key = String(product.client_id || product.id || product.barcode || product.name || 'product');
  return `client-product-${key.replace(/[^a-zA-Z0-9_-]+/g, '-')}`;
}

function clientProductLink(product, label, exchangeId='') {
  const historyAction = exchangeId
    ? ` data-exchange-id="${esc(exchangeId)}" data-product-id="${esc(product.client_id || '')}" onclick="event.preventDefault();showClientHistoryProducts(this.dataset.exchangeId,this.dataset.productId)"`
    : '';
  return `<a class="client-product-link" href="#${clientProductDomId(product)}"${historyAction}>${esc(label || product.name)}</a>`;
}

function cleanClientAnswer(answer) {
  return String(answer || '')
    .replace(/\*\*/g, '')
    .replace(/__/g, '')
    .replace(/^#{1,6}\s*/gm, '')
    .trim();
}

function linkifyClientAnswer(answer, products, exchangeId='') {
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
    return product ? clientProductLink(product, part, exchangeId) : esc(part);
  }).join('');
}

function clientQuoteButton(quote, focusProductId='', label='Citer ce passage') {
  const cleanQuote = String(quote || '').trim().replace(/\s+/g, ' ').slice(0, 500);
  return `<button type="button" class="client-quote-action" data-quote="${encodeURIComponent(cleanQuote)}" data-focus-product="${esc(focusProductId)}" onclick="event.stopPropagation();quoteClientPassage(decodeURIComponent(this.dataset.quote),this.dataset.focusProduct)" title="${esc(label)}" aria-label="${esc(label)}">&#8220;</button>`;
}

function renderQuotableClientAnswer(answer, products=[], exchangeId='') {
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
      <div class="client-answer-paragraph-text">${linkifyClientAnswer(passage, products, exchangeId)}</div>
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

function clientProductsActionMarkup(result, exchangeId) {
  const count = Array.isArray(result?.products) ? result.products.length : 0;
  if (!count) return '';
  return `<button type="button" class="btn btn-outline btn-inline client-show-products" data-exchange-id="${esc(exchangeId)}" onclick="showClientHistoryProducts(this.dataset.exchangeId)">Voir ${count} produit${count > 1 ? 's' : ''}</button>`;
}

function clientDocumentationSourceDomId(exchangeId, index) {
  const key = String(exchangeId || 'current').replace(/[^a-zA-Z0-9_-]+/g, '-');
  return `client-doc-source-${key}-${index}`;
}

function safeClientSourceUrl(value) {
  const url = String(value || '').trim();
  return /^https?:\/\//i.test(url) ? url : '';
}

function showClientDocumentationSource(exchangeId, index) {
  const source = document.getElementById(clientDocumentationSourceDomId(exchangeId, index));
  if (!source) return false;
  const details = source.closest?.('details');
  if (details) details.open = true;
  window.setTimeout(() => source.scrollIntoView?.({behavior: 'smooth', block: 'center'}), 0);
  return false;
}

function clientDocumentCitations(sourceIds, sourceNumbers, exchangeId) {
  const numbers = [];
  for (const sourceId of Array.isArray(sourceIds) ? sourceIds : []) {
    const number = sourceNumbers.get(String(sourceId || ''));
    if (number && !numbers.includes(number)) numbers.push(number);
  }
  if (!numbers.length) return '';
  return `<span class="client-doc-citations">${numbers.map(number => `
    <button type="button" class="client-doc-cite" aria-label="Voir la source ${number}" data-exchange-id="${esc(exchangeId)}" data-source-number="${number}" onclick="showClientDocumentationSource(this.dataset.exchangeId,Number(this.dataset.sourceNumber))">[${number}]</button>
  `).join('')}</span>`;
}

function renderDocumentedClientDetails(result, exchangeId) {
  if (result?.response_mode !== 'documented') return '';
  const documentation = result?.advice?.documentation || {};
  const sources = Array.isArray(documentation.sources) ? documentation.sources : [];
  const sourceNumbers = new Map(sources.map((source, index) => [String(source.source_id || ''), index + 1]));
  const products = Array.isArray(result.products) ? result.products : [];
  const productsById = new Map(products.map(product => [String(product.client_id || ''), product]));
  const keyPoints = Array.isArray(documentation.key_points) ? documentation.key_points : [];
  const comparisons = Array.isArray(documentation.comparisons) ? documentation.comparisons : [];
  const usefulGuidance = Array.isArray(documentation.useful_guidance) ? documentation.useful_guidance : [];
  const importantChecks = Array.isArray(documentation.important_checks) ? documentation.important_checks : [];

  const textSection = (title, items) => items.length ? `
    <section class="client-doc-section">
      <div class="client-doc-heading">${esc(title)}</div>
      <div class="client-doc-list">${items.map(item => `
        <div class="client-doc-item">
          <div class="client-doc-item-copy">${esc(item.text || '')}${clientDocumentCitations(item.source_ids, sourceNumbers, exchangeId)}</div>
          ${clientQuoteButton(item.text || '', '', 'Citer ce point')}
        </div>
      `).join('')}</div>
    </section>` : '';

  return `<div class="client-documented">
    ${keyPoints.length ? `<section class="client-doc-section">
      <div class="client-doc-heading">Points essentiels</div>
      <div class="client-doc-list">${keyPoints.map(item => `
        <div class="client-doc-item">
          <div>
            <div class="client-doc-item-heading">${esc(item.heading || '')}</div>
            <div class="client-doc-item-copy">${esc(item.detail || '')}${clientDocumentCitations(item.source_ids, sourceNumbers, exchangeId)}</div>
          </div>
          ${clientQuoteButton(`${item.heading || ''}: ${item.detail || ''}`, '', 'Citer ce point')}
        </div>
      `).join('')}</div>
    </section>` : ''}
    ${comparisons.length ? `<section class="client-doc-section">
      <div class="client-doc-heading">Différences entre les produits</div>
      ${comparisons.map(item => {
        const product = productsById.get(String(item.candidate_id || ''));
        const productName = product?.name || 'Produit du plan';
        const copy = [item.difference, item.practical_note].filter(Boolean).join(' ');
        return `<div class="client-doc-comparison">
          <div class="client-doc-comparison-product">${product ? clientProductLink(product, '', exchangeId) : esc(productName)}</div>
          <div class="client-doc-comparison-copy">
            ${item.difference ? `<div><strong>Différence :</strong> ${esc(item.difference)}</div>` : ''}
            ${item.practical_note ? `<div><strong>En pratique :</strong> ${esc(item.practical_note)}</div>` : ''}
            ${clientDocumentCitations(item.source_ids, sourceNumbers, exchangeId)}
            ${clientQuoteButton(`${productName}: ${copy}`, item.candidate_id || '', 'Citer cette comparaison')}
          </div>
        </div>`;
      }).join('')}
    </section>` : ''}
    ${textSection('Conseils pratiques', usefulGuidance)}
    ${textSection('À vérifier', importantChecks)}
    ${sources.length ? `<section class="client-doc-section">
      <details class="client-doc-sources">
        <summary>Sources consultées (${sources.length})</summary>
        <div class="client-doc-source-list">${sources.map((source, index) => {
          const number = index + 1;
          const url = safeClientSourceUrl(source.url);
          return `<div class="client-doc-source" id="${clientDocumentationSourceDomId(exchangeId, number)}">
            <span class="client-doc-source-number">[${number}]</span>
            <div class="client-doc-source-title">${url
              ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(source.title || 'Source')}</a>`
              : esc(source.title || 'Source')}</div>
            ${source.publisher ? `<div>${esc(source.publisher)}</div>` : ''}
            ${source.summary ? `<div class="client-doc-source-summary">${esc(source.summary)}</div>` : ''}
          </div>`;
        }).join('')}</div>
      </details>
    </section>` : ''}
  </div>`;
}

function renderClientResponseMode(result) {
  const mode = normalizeClientResponseMode(result?.response_mode);
  const className = mode === 'lookup' ? 'is-fast' : (mode === 'documented' ? 'is-documented' : 'is-detailed');
  const label = mode === 'lookup' ? 'Recherche rapide' : (mode === 'documented' ? 'Réponse documentée' : 'Réponse avec IA');
  return `<div class="client-response-mode ${className}">
    ${label}
  </div>`;
}

function renderLatestAssistantDetails(result, exchangeId, includeActions=true) {
  const advice = result?.advice || {};
  const products = Array.isArray(result?.products) ? result.products : [];
  const highlighted = highlightedClientProducts(result);
  const links = highlighted.map(product => clientProductLink(product, '', exchangeId)).join('<span class="client-link-sep"> · </span>');
  return `
    ${renderClientResponseMode(result)}
    ${result?.degraded ? `<div class="msg info client-ai-fallback">${esc(result.warning || 'Réponse de secours fondée sur les données disponibles.')}</div>` : ''}
    ${result?.response_mode === 'documented' ? '<div class="client-doc-heading">À dire au client</div>' : ''}
    <div class="client-chat-answer">${renderQuotableClientAnswer(result?.answer || advice.summary || '', products, exchangeId)}</div>
    ${links ? `<div class="client-answer-products"><span>Produits cités :</span> ${links}</div>` : ''}
    ${clientProductsActionMarkup(result, exchangeId)}
    ${renderDocumentedClientDetails(result, exchangeId)}
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
    ${includeActions && result?.response_mode !== 'lookup' ? `<div id="aiFeedbackRow" class="client-feedback-row">
      <span>Cette réponse aide ?</span>
      <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('up')">Oui</button>
      <button class="btn btn-outline btn-inline" onclick="sendAiFeedback('down')">Non</button>
    </div>` : ''}
    ${includeActions ? clientDirectReplyMarkup() : ''}
  `;
}

function findClientAssistantMessage(exchangeId) {
  const wanted = String(exchangeId || '');
  if (!wanted) return null;
  return clientConversation.find(message => (
    message.role === 'assistant' && String(message.exchange_id || '') === wanted
  )) || null;
}

function showClientHistoryProducts(exchangeId, focusProductId='') {
  const message = findClientAssistantMessage(exchangeId);
  const products = Array.isArray(message?.result?.products) ? message.result.products : [];
  if (!products.length) return false;
  currentClientMatches = products;
  _clientVisibleExchangeId = String(exchangeId || '');
  renderClientMatches(products);
  persistClientDraft();
  pollClientProductImages();
  window.setTimeout(() => {
    const focusProduct = focusProductId
      ? currentClientMatches.find(product => String(product.client_id || '') === String(focusProductId))
      : null;
    const target = focusProduct
      ? document.getElementById(clientProductDomId(focusProduct))
      : document.getElementById('clientMatches');
    target?.scrollIntoView?.({behavior: 'smooth', block: 'start'});
  }, 0);
  return false;
}

function clientConversationExchanges() {
  const exchanges = [];
  let pending = null;
  for (const message of clientConversation) {
    if (message.role === 'user') {
      pending = {
        id: String(message.exchange_id || `exchange-${exchanges.length}`),
        mode: normalizeClientMode(message.mode),
        user: message,
        assistant: null,
      };
      exchanges.push(pending);
      continue;
    }
    if (pending && !pending.assistant) {
      pending.assistant = message;
      pending.mode = normalizeClientMode(message.mode || pending.mode);
      pending = null;
    } else {
      exchanges.push({
        id: String(message.exchange_id || `exchange-${exchanges.length}`),
        mode: normalizeClientMode(message.mode),
        user: null,
        assistant: message,
      });
    }
  }
  return exchanges;
}

function renderHistoricalClientExchange(exchange) {
  const assistant = exchange.assistant;
  const result = assistant?.result || null;
  const products = Array.isArray(result?.products) ? result.products : [];
  return `<details class="client-history-item" data-exchange-id="${esc(exchange.id)}">
    <summary class="client-history-summary">
      <div class="client-history-summary-main">
        <div class="client-history-label">Demande précédente</div>
        <div class="client-history-question">${esc(cleanClientAnswer(exchange.user?.content || 'Réponse précédente'))}</div>
      </div>
      <div class="client-history-meta"><span>${exchange.mode === 'documented' ? 'Documenté' : (exchange.mode === 'ai' ? 'IA' : 'Rapide')}</span>${products.length ? `<span>· ${products.length} produit${products.length > 1 ? 's' : ''}</span>` : ''}</div>
    </summary>
    <div class="client-history-body">
      <div class="client-message client-message-assistant">
        <div class="client-message-label">Réponse</div>
        ${result
          ? renderLatestAssistantDetails(result, exchange.id, false)
          : `<div class="client-chat-answer">${renderQuotableClientAnswer(assistant?.content || '')}</div>`}
      </div>
    </div>
  </details>`;
}

function renderLatestClientExchange(exchange) {
  const assistant = exchange.assistant;
  return `<div class="client-exchange" data-exchange-id="${esc(exchange.id)}">
    ${exchange.user ? `<div class="client-message client-message-user">
      <div class="client-message-label">Demande</div>
      <div class="client-chat-answer">${esc(cleanClientAnswer(exchange.user.content))}</div>
    </div>` : ''}
    ${assistant ? `<div class="client-message client-message-assistant">
      <div class="client-message-label">Réponse</div>
      ${assistant.result
        ? renderLatestAssistantDetails(assistant.result, exchange.id)
        : `<div class="client-chat-answer">${renderQuotableClientAnswer(assistant.content)}</div>${clientDirectReplyMarkup()}`}
    </div>` : ''}
  </div>`;
}

function renderClientConversation() {
  const target = document.getElementById('clientAdvice');
  if (!target) return;
  if (!clientConversation.length) {
    target.innerHTML = '';
    updateClientHistoryAction();
    return;
  }
  const exchanges = clientConversationExchanges();
  const latest = exchanges[exchanges.length - 1];
  const previous = exchanges.slice(0, -1).reverse();
  target.innerHTML = `<div class="client-conversation" aria-live="polite">
    ${latest ? renderLatestClientExchange(latest) : ''}
    ${previous.map(renderHistoricalClientExchange).join('')}
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

function setClientWorking(active, mode=_clientSearchMode) {
  if (_clientWorkingTimer && typeof window.clearInterval === 'function') {
    window.clearInterval(_clientWorkingTimer);
  }
  _clientWorkingTimer = null;
  const region = document.getElementById('clientWorking');
  const label = document.getElementById('clientWorkingText');
  const button = document.getElementById('clientFindButton');
  const followupButton = document.getElementById('clientFollowupButton');
  if (region) region.hidden = !active;
  if (button) button.disabled = active;
  if (followupButton) followupButton.disabled = active;
  document.querySelectorAll('[data-client-mode]').forEach(option => {
    option.disabled = active;
  });
  if (!active) return;

  _clientWorkingStartedAt = Date.now();
  const update = () => {
    const elapsed = Math.max(0, Math.floor((Date.now() - _clientWorkingStartedAt) / 1000));
    if (label) label.textContent = mode === 'documented'
      ? `${aiProviderLabel()} consulte les fiches et prépare la réponse documentée · ${elapsed} s`
      : (mode === 'ai'
        ? `${aiProviderLabel()} analyse et vérifie les produits du plan · ${elapsed} s`
        : 'Recherche dans le plan actuel du magasin…');
  };
  update();
  if (mode !== 'fast' && typeof window.setInterval === 'function') {
    _clientWorkingTimer = window.setInterval(update, 1000);
  }
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
  setClientWorking(false);
  _latestClientResult = null;
  _clientSelectedQuote = '';
  _clientQuoteFocusProductId = '';
  _clientFocusedProductId = '';
  _clientVisibleExchangeId = '';
  currentClientMatches = [];
  clientConversation = [];
  const advice = document.getElementById('clientAdvice');
  const matches = document.getElementById('clientMatches');
  if (advice) advice.innerHTML = '';
  if (matches) matches.innerHTML = '';
  if (showStatus) {
    const status = document.getElementById('clientHelpStatus');
    if (status) status.textContent = 'Écrivez une demande, puis cliquez sur « Trouver produits ».';
  }
  persistClientDraft();
  updateClientHistoryAction();
}

function onClientQuestionInput() {
  _clientRequestSequence += 1;
  if (_clientRagController) {
    _clientRagController.abort();
    _clientRagController = null;
    setClientWorking(false);
  }
  const status = document.getElementById('clientHelpStatus');
  const selectedMode = clientModeLabel(_clientSearchMode);
  if (status) status.textContent = clientConversation.length
    ? `${selectedMode} : cliquez sur « Trouver produits » pour envoyer cette question de suivi.`
    : `${selectedMode} : cliquez sur « Trouver produits » pour lancer la recherche.`;
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

function clientRequiredConceptGroups(question) {
  const normalized = typeof normalizeSearchText === 'function'
    ? normalizeSearchText(question)
    : String(question || '').toLowerCase();
  const tokens = new Set(normalized.split(/\s+/).filter(Boolean));
  const groups = [];
  const cottonBalls = ['watte', 'ouate'].some(token => tokens.has(token)) || (
    ['coton', 'cotton'].some(token => tokens.has(token)) &&
    ['boule', 'boules', 'ball', 'balls'].some(token => tokens.has(token))
  );
  if (cottonBalls) {
    groups.push(['coton', 'cotons', 'cotton', 'ouate', 'watte']);
    groups.push(['boule', 'boules', 'ball', 'balls', 'ouate']);
  }
  const transparentDressing = [
    'membrane transparent', 'pansement transparent', 'film transparent',
  ].some(marker => normalized.includes(marker)) ||
    ['opsite', 'upsite', 'upside'].some(token => tokens.has(token));
  if (transparentDressing) {
    groups.push(['transparent', 'transparente', 'transp', 'opsite', 'tegaderm']);
    groups.push(['pansement', 'pans', 'diach', 'bandage', 'band aid', 'opsite', 'tegaderm']);
  }
  const electric = [...tokens].some(token => token.startsWith('elect') || token === 'elec');
  const compound = tokens.has('toothbrush') || tokens.has('toothbrushes');
  const brush = compound || [...tokens].some(token => token.startsWith('bross') || token === 'brush');
  const tooth = compound || [...tokens].some(token => token.startsWith('dent') || token.startsWith('tooth'));
  if (electric && brush && tooth) {
    groups.push([
      'brosse dent', 'brosse dents', 'br dent', 'br dents', 'toothbrush',
      'rech bros', 'recharge bros', 'soni rech', 'tete br dent',
    ]);
    groups.push([
      'electrique', 'electric', 'elec', 'pile', 'sonicare', 'philips one',
      'tete br dent',
    ]);
  }
  return groups.map(compileClientConceptGroup);
}

function clientExcludedConceptTerms(question) {
  const normalized = typeof normalizeSearchText === 'function'
    ? normalizeSearchText(question) : String(question || '').toLowerCase();
  const tokens = new Set(normalized.split(/\s+/).filter(Boolean));
  const electric = [...tokens].some(token => token.startsWith('elect') || token === 'elec');
  const compound = tokens.has('toothbrush') || tokens.has('toothbrushes');
  const brush = compound || [...tokens].some(token => token.startsWith('bross') || token === 'brush');
  const tooth = compound || [...tokens].some(token => token.startsWith('dent') || token.startsWith('tooth'));
  const terms = electric && brush && tooth
    ? ['irr', 'irrigateur', 'hydropulseur', 'airfloss', 'water flosser', 's fil'] : [];
  return terms.length ? [compileClientConceptGroup(terms)] : [];
}

function compileClientConceptGroup(terms) {
  const alternatives = terms.map(term => {
    const normalized = typeof normalizeSearchText === 'function'
      ? normalizeSearchText(term) : String(term || '').toLowerCase();
    return normalized.split(/\s+/).filter(Boolean).map(token => (
      token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + (token.length >= 4 ? '[a-z0-9]*' : '')
    )).join(' ');
  }).filter(Boolean);
  return new RegExp(`(?:^| )(?:${alternatives.join('|')})(?= |$)`);
}

function clientConceptTermMatches(hayTokens, term) {
  const conceptTokens = Array.isArray(term) ? term : (
    typeof normalizeSearchText === 'function'
      ? normalizeSearchText(term) : String(term || '').toLowerCase()
  ).split(/\s+/).filter(Boolean);
  if (!conceptTokens.length || conceptTokens.length > hayTokens.length) return false;
  for (let start = 0; start <= hayTokens.length - conceptTokens.length; start += 1) {
    const matches = conceptTokens.every((expected, offset) => {
      const actual = hayTokens[start + offset];
      return actual === expected || (expected.length >= 4 && actual.startsWith(expected));
    });
    if (matches) return true;
  }
  return false;
}

function productMatchesClientConcepts(product, groups, excludedNameTerms=[]) {
  const rawHay = typeof productSearchText === 'function'
    ? productSearchText(product)
    : [product.name, product.brand, product.description, product.search_terms, product.usage_notes].join(' ');
  const hasNormalizedSearchCache = Boolean(
    product && product._sf && product._sf.haystack === rawHay
  );
  const normalizedHay = typeof normalizeSearchText === 'function' && !hasNormalizedSearchCache
    ? normalizeSearchText(rawHay)
    : String(rawHay || '').toLowerCase();
  const requiredMatch = groups.every(
    group => (group && typeof group.test === 'function')
      ? group.test(normalizedHay)
      : group.some(term => clientConceptTermMatches(normalizedHay.split(/\s+/), term))
  );
  if (!requiredMatch) return false;
  const normalizedName = typeof normalizeSearchText === 'function'
    ? normalizeSearchText(product.name || '') : String(product.name || '').toLowerCase();
  return !excludedNameTerms.some(group => (
    group && typeof group.test === 'function'
      ? group.test(normalizedName)
      : group.some(term => clientConceptTermMatches(normalizedName.split(/\s+/), term))
  ));
}

function localClientMatches(question, limit=60) {
  if (typeof searchProductsFromCache !== 'function' || !allProductsCache.length) return [];
  const requiredConcepts = clientRequiredConceptGroups(question);
  const excludedConcepts = clientExcludedConceptTerms(question);
  const predicate = requiredConcepts.length || excludedConcepts.length
    ? product => productMatchesClientConcepts(product, requiredConcepts, excludedConcepts)
    : null;
  const rawMatches = searchProductsFromCache(
    question, Math.min(limit * 2, 100), 100, predicate
  );
  const grouped = [];
  const byKey = new Map();
  for (const raw of rawMatches) {
    if (!productMatchesClientConcepts(raw, requiredConcepts, excludedConcepts)) continue;
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

function buildFastClientResult(products, elapsedMs=0) {
  const matches = (Array.isArray(products) ? products : []).slice(0, CLIENT_FAST_PRODUCT_LIMIT);
  const names = matches.slice(0, 4).map(product => String(product.name || '').trim()).filter(Boolean);
  let answer = 'Aucun produit proche de cette demande n’a été trouvé dans le plan actuel du magasin.';
  if (names.length === 1 && matches.length === 1) {
    answer = `Produit trouvé dans le plan : ${names[0]}.`;
  } else if (names.length) {
    const remaining = Math.max(0, matches.length - names.length);
    answer = `Produits les plus proches dans le plan : ${names.join(', ')}${remaining ? ` et ${remaining} autre${remaining > 1 ? 's' : ''}` : ''}.`;
  }
  const ids = matches.map(product => String(product.client_id || '')).filter(Boolean);
  return {
    success: true,
    response_mode: 'lookup',
    answer,
    products: matches,
    highlighted_product_ids: ids,
    elapsed_ms: Math.max(0, Number(elapsedMs) || 0),
    advice: {
      summary: answer,
      follow_up_questions: [],
      safety_flags: [],
      pharmacist_referral: false,
      pharmacist_reason: '',
    },
  };
}

function prepareClientResult(result) {
  const prepared = {...(result || {})};
  const advice = prepared.advice && typeof prepared.advice === 'object' ? prepared.advice : {};
  const rawProducts = Array.isArray(prepared.products) ? prepared.products : [];
  const normalizedProducts = rawProducts.map(product => (
    typeof normalizeProduct === 'function' ? normalizeProduct(product) : {...product}
  ));
  let products = normalizedProducts.slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE);
  let highlightedIds = Array.isArray(prepared.highlighted_product_ids)
    ? prepared.highlighted_product_ids.slice(0, 16).map(String)
    : [];
  let assortmentIds = Array.isArray(prepared.assortment_product_ids)
    ? prepared.assortment_product_ids.slice(0, CLIENT_MAX_PRODUCTS_PER_EXCHANGE).map(String)
    : [];
  if (prepared.response_mode !== 'lookup') {
    const byId = new Map(products.map(product => [String(product.client_id || ''), product]));
    const displayIds = [...new Set([...highlightedIds, ...assortmentIds])];
    products = displayIds.map(id => byId.get(id)).filter(Boolean);
    const availableIds = new Set(products.map(product => String(product.client_id || '')));
    highlightedIds = highlightedIds.filter(id => availableIds.has(id));
    assortmentIds = assortmentIds.filter(id => availableIds.has(id));
  } else {
    products = products.slice(0, CLIENT_FAST_PRODUCT_LIMIT);
    highlightedIds = products.map(product => String(product.client_id || '')).filter(Boolean);
  }
  const answer = cleanClientAnswer(prepared.answer || advice.summary || '');
  return {
    ...prepared,
    success: true,
    response_mode: normalizeClientResponseMode(prepared.response_mode),
    answer,
    products,
    highlighted_product_ids: highlightedIds,
    assortment_product_ids: assortmentIds,
    advice: {
      ...advice,
      summary: cleanClientAnswer(advice.summary || answer),
      documentation: clientDocumentationForStorage(advice.documentation),
    },
  };
}

function appendClientExchange(question, rawResult, mode, requestId) {
  const result = prepareClientResult(rawResult);
  const exchangeId = `client-${Date.now().toString(36)}-${requestId}`;
  clientConversation.push({
    role: 'user', content: question, exchange_id: exchangeId, mode,
  });
  clientConversation.push({
    role: 'assistant', content: result.answer, exchange_id: exchangeId, mode, result,
  });
  clientConversation = clientConversation.slice(-CLIENT_MAX_MESSAGES);
  _latestClientResult = result;
  _clientVisibleExchangeId = exchangeId;
  currentClientMatches = result.products;
  const questionInput = document.getElementById('clientQuestion');
  if (questionInput) questionInput.value = '';
  const followupInput = document.getElementById('clientFollowupQuestion');
  if (followupInput) followupInput.value = '';
  _clientSelectedQuote = '';
  _clientQuoteFocusProductId = '';
  renderClientConversation();
  renderClientMatches(currentClientMatches);
  persistClientDraft();
  if (currentClientMatches.length) pollClientProductImages();
  window.setTimeout(() => {
    document.querySelector?.('.client-exchange')?.scrollIntoView?.({behavior: 'smooth', block: 'start'});
  }, 0);
  return exchangeId;
}

function updateClientExchangeResult(exchangeId, rawResult) {
  const assistant = findClientAssistantMessage(exchangeId);
  if (!assistant) return;
  const replyDraft = document.getElementById('clientFollowupQuestion')?.value || '';
  const result = prepareClientResult(rawResult);
  assistant.content = result.answer;
  assistant.result = result;
  if (clientConversation[clientConversation.length - 1] === assistant) {
    _latestClientResult = result;
  }
  if (_clientVisibleExchangeId === exchangeId) {
    currentClientMatches = result.products;
    renderClientMatches(currentClientMatches);
    if (currentClientMatches.length) pollClientProductImages();
  }
  renderClientConversation();
  const replyInput = document.getElementById('clientFollowupQuestion');
  if (replyInput && replyDraft) replyInput.value = replyDraft;
  persistClientDraft();
}

async function runClientRequest(question, options={}) {
  question = String(question || '').trim();
  const status = document.getElementById('clientHelpStatus');
  if (!question) {
    if (status) status.textContent = 'Écrivez d’abord la demande du client.';
    document.getElementById('clientQuestion')?.focus();
    return;
  }

  if (_clientRagController) _clientRagController.abort();
  if (_clientImagePollTimer) window.clearTimeout(_clientImagePollTimer);
  const requestId = ++_clientRequestSequence;
  const history = clientHistoryPayload();
  const mode = options.mode ? normalizeClientMode(options.mode) : _clientSearchMode;
  const contextProductIds = currentClientMatches
    .map(product => product.client_id).filter(Boolean).slice(0, CLIENT_CONTEXT_PRODUCT_LIMIT);
  const previousQuestion = [...clientConversation].reverse()
    .find(message => message.role === 'user')?.content || '';
  const retrievalQuestion = options.followUp && mode === 'fast' && previousQuestion
    ? `${previousQuestion} ${question}`
    : question;
  const controller = (typeof AbortController !== 'undefined') ? new AbortController() : null;
  _clientRagController = controller;
  if (!options.followUp) {
    currentClientMatches = [];
    _clientVisibleExchangeId = '';
    const matches = document.getElementById('clientMatches');
    if (matches) matches.innerHTML = '';
  }
  setClientWorking(true, mode);

  if (mode === 'fast') {
    const startedAt = Date.now();
    const localProducts = localClientMatches(retrievalQuestion, CLIENT_FAST_PRODUCT_LIMIT);
    const serverPromise = apiClientFind(
      retrievalQuestion, CLIENT_FAST_PRODUCT_LIMIT, controller?.signal
    );
    if (localProducts.length) {
      const exchangeId = appendClientExchange(
        question, buildFastClientResult(localProducts, Date.now() - startedAt), mode, requestId
      );
      setClientWorking(false);
      _clientRagController = null;
      if (status) status.textContent = `Recherche rapide : ${localProducts.length} produit${localProducts.length > 1 ? 's' : ''} affiché${localProducts.length > 1 ? 's' : ''}.`;
      void serverPromise.then(serverProducts => {
        if (requestId !== _clientRequestSequence || !serverProducts.length) return;
        updateClientExchangeResult(
          exchangeId, buildFastClientResult(serverProducts, Date.now() - startedAt)
        );
        if (status) status.textContent = `Recherche rapide : ${serverProducts.length} produit${serverProducts.length > 1 ? 's' : ''} trouvé${serverProducts.length > 1 ? 's' : ''} dans le plan.`;
      });
      return;
    }
    const serverProducts = await serverPromise;
    if (requestId !== _clientRequestSequence) return;
    _clientRagController = null;
    setClientWorking(false);
    appendClientExchange(
      question, buildFastClientResult(serverProducts, Date.now() - startedAt), mode, requestId
    );
    if (status) status.textContent = serverProducts.length
      ? `Recherche rapide : ${serverProducts.length} produit${serverProducts.length > 1 ? 's' : ''} trouvé${serverProducts.length > 1 ? 's' : ''} dans le plan.`
      : 'Aucun produit proche trouvé dans le plan. Essayez « Avec IA » ou « Documenté » pour interpréter la demande.';
    return;
  }

  if (status) status.textContent = mode === 'documented'
    ? `${aiProviderLabel()} consulte les fiches produit et les sources disponibles.`
    : `${aiProviderLabel()} analyse la demande et vérifiera chaque produit proposé.`;
  const result = await apiGenerateClientHelp({
    question,
    history,
    mode,
    follow_up: Boolean(options.followUp),
    selected_text: options.selectedText || '',
    focus_product_id: options.focusProductId || '',
    context_product_ids: contextProductIds,
  }, controller?.signal);
  if (requestId !== _clientRequestSequence) return;
  _clientRagController = null;
  setClientWorking(false);
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
  const prepared = prepareClientResult(result);
  appendClientExchange(question, prepared, mode, requestId);
  if (status) {
    const timing = result.elapsed_ms ? ` en ${(result.elapsed_ms / 1000).toFixed(1)} s` : '';
    const responseLabel = mode === 'documented' ? 'Réponse documentée' : 'Réponse avec IA';
    status.textContent = result.degraded
      ? `Réponse de secours${timing} : ${prepared.products.length} produit${prepared.products.length > 1 ? 's' : ''} du plan conservé${prepared.products.length > 1 ? 's' : ''}.`
      : `${responseLabel}${timing} : ${prepared.products.length} produit${prepared.products.length > 1 ? 's' : ''} vérifié${prepared.products.length > 1 ? 's' : ''}.`;
  }
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
  getClientSearchMode, setClientSearchMode, showClientHistoryProducts,
};
