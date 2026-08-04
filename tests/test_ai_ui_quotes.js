const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const elements = {
  clientQuotePreview: {hidden: true},
  clientQuoteText: {textContent: ''},
  clientProductQuotePreview: {hidden: true},
  clientProductQuoteText: {textContent: ''},
  clientProductModal: {hidden: true},
  clientAdvice: {innerHTML: ''},
  clientMatches: {innerHTML: ''},
  clientFindButton: {disabled: false},
  clientWorking: {hidden: true},
  clientWorkingText: {textContent: ''},
  clientHelpStatus: {textContent: ''},
  clientClearHistoryButton: {hidden: true},
  clientQuestion: {
    value: 'Nouvelle demande non envoyée',
    focused: false,
    focus() { this.focused = true; },
  },
  clientFollowupQuestion: {
    focused: false,
    scrolled: false,
    focus() { this.focused = true; },
    scrollIntoView() { this.scrolled = true; },
  },
};

function modeButton(mode) {
  const classes = new Set(mode === 'fast' ? ['is-active'] : []);
  return {
    dataset: {clientMode: mode}, disabled: false, ariaChecked: mode === 'fast' ? 'true' : 'false',
    classList: {toggle(name, active) { active ? classes.add(name) : classes.delete(name); }},
    setAttribute(name, value) { if (name === 'aria-checked') this.ariaChecked = value; },
    hasClass(name) { return classes.has(name); },
  };
}
const modeButtons = [modeButton('fast'), modeButton('ai'), modeButton('documented')];

const context = {
  console,
  currentClientMatches: [],
  document: {
    getElementById(id) { return elements[id] || null; },
    querySelectorAll(selector) { return selector === '[data-client-mode]' ? modeButtons : []; },
  },
  esc(value) { return String(value ?? ''); },
  sideStaffLabel(value) { return String(value || ''); },
  isHomeBrand() { return false; },
  aiProviderLabel() { return 'DeepSeek'; },
  normalizeSearchText(value) {
    return String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
      .toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  },
  productSearchText(product) {
    return [product.name, product.brand, product.description].join(' ');
  },
  persistClientDraft() {},
  window: {
    AppAI: {},
    setTimeout(callback) { callback(); },
  },
};
vm.createContext(context);

const source = fs.readFileSync('static/ai-ui.js', 'utf8');
assert.ok(!source.includes('const previewProducts = localClientMatches('),
  'AI modes must not expose local preview cards before model verification');
assert.ok(source.includes('apiRejectProductImage(product.id)'),
  'product details must allow a bad exact-package image to be rejected');
vm.runInContext(`${source}\n;globalThis.__quoteTest = {
  renderQuotableClientAnswer,
  clientRegulatoryIdentifiers,
  clientQuoteButton,
  quoteClientPassage,
  clearClientSelectedQuote,
  clearClientHistory,
  getClientConversationForStorage,
  restoreClientConversation,
  renderClientConversation,
  setClientSearchMode,
  getClientSearchMode,
  showHistoryProducts: exchangeId => {
    const originalRender = renderClientMatches;
    const originalPoll = pollClientProductImages;
    renderClientMatches = () => {};
    pollClientProductImages = () => {};
    try { return showClientHistoryProducts(exchangeId); }
    finally { renderClientMatches = originalRender; pollClientProductImages = originalPoll; }
  },
  clientRequiredConceptGroups,
  clientExcludedConceptTerms,
  productMatchesClientConcepts,
  buildFastClientResult,
  prepareClientResult,
  mergeDocumentedClientUpgrade,
  clientResultForStorage,
  renderDocumentedClientDetails,
  getClientSearchStateForStorage,
  seedSearchState: (result, products, conversation) => {
    _latestClientResult = result;
    currentClientMatches = products;
    clientConversation = conversation;
  },
  state: () => ({quote: _clientSelectedQuote, focus: _clientQuoteFocusProductId}),
  historyState: () => ({
    messages: clientConversation.length,
    products: currentClientMatches.length,
    hasResult: Boolean(_latestClientResult),
  }),
  currentProductNames: () => currentClientMatches.map(product => product.name),
};`, context);

const longAnswer = [
  'Première phrase qui explique clairement une différence au client.',
  'Deuxième phrase qui ajoute une autre information pratique et facile à citer.',
  'Troisième phrase qui termine ce long passage avec une recommandation prudente.',
  'Quatrième phrase ajoutée pour dépasser la taille maximale d’un seul passage sur téléphone.',
].join(' ');
const missingDinMarkup = context.__quoteTest.clientRegulatoryIdentifiers({regulatory_identifiers: []});
assert.ok(missingDinMarkup.includes('DIN / DIN-HM : non disponible'));
const confirmedDinMarkup = context.__quoteTest.clientRegulatoryIdentifiers({
  regulatory_identifiers: [{type: 'DIN', value: '12345678', status: 'confirmed'}],
});
assert.ok(confirmedDinMarkup.includes('DIN 12345678'));
assert.ok(!confirmedDinMarkup.includes('non disponible'));
const rendered = context.__quoteTest.renderQuotableClientAnswer(longAnswer, []);
assert.ok((rendered.match(/client-quote-action/g) || []).length >= 2);

const productButton = context.__quoteTest.clientQuoteButton(
  'Produit Exemple: Description utile', 'product:42', 'Citer cette description'
);
assert.ok(productButton.includes('data-focus-product="product:42"'));
assert.ok(productButton.includes('Citer cette description'));

context.__quoteTest.quoteClientPassage(' Produit Exemple: Description utile ', 'product:42');
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(context.__quoteTest.state())),
  {quote: 'Produit Exemple: Description utile', focus: 'product:42'}
);
assert.strictEqual(elements.clientQuotePreview.hidden, false);
assert.strictEqual(elements.clientQuoteText.textContent, 'Produit Exemple: Description utile');
assert.strictEqual(elements.clientFollowupQuestion.focused, true);
assert.strictEqual(elements.clientFollowupQuestion.scrolled, true);

context.__quoteTest.clearClientSelectedQuote();
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(context.__quoteTest.state())),
  {quote: '', focus: ''}
);

const savedProduct = {
  id: 42, client_id: 'product:42', name: 'Produit Exemple', brand: 'Marque',
  description: 'Description persistée', barcode: '123', aisle: '2', side: 'Gauche',
  section: '1', shelf: '3', position: '4', is_plano: 1, in_stock: 1,
};
context.__quoteTest.seedSearchState(
  {
    response_mode: 'detailed', answer: 'Réponse sauvegardée',
    highlighted_product_ids: ['product:42'],
    advice: {summary: 'Réponse sauvegardée', follow_up_questions: ['Pourquoi?']},
  },
  [savedProduct],
  [{role: 'user', content: 'Question'}, {role: 'assistant', content: 'Réponse'}]
);
const savedState = context.__quoteTest.getClientSearchStateForStorage();
assert.strictEqual(savedState.products.length, 1);
assert.strictEqual(savedState.products[0].description, 'Description persistée');
assert.strictEqual(savedState.latest_result.answer, 'Réponse sauvegardée');

const firstResult = {
  response_mode: 'detailed', answer: 'Première réponse',
  highlighted_product_ids: ['product:1'],
  products: [{...savedProduct, id: 1, client_id: 'product:1', name: 'Premier produit', image_url: '/one.jpg'}],
  advice: {summary: 'Première réponse'},
};
const secondResult = {
  response_mode: 'lookup', answer: 'Deuxième réponse',
  highlighted_product_ids: ['product:2'],
  products: [{...savedProduct, id: 2, client_id: 'product:2', name: 'Deuxième produit', image_url: '/two.jpg'}],
  advice: {summary: 'Deuxième réponse'},
};
context.__quoteTest.restoreClientConversation([
  {role: 'user', content: 'Première question', exchange_id: 'first', mode: 'ai'},
  {role: 'assistant', content: 'Première réponse', exchange_id: 'first', mode: 'ai', result: firstResult},
  {role: 'user', content: 'Deuxième question', exchange_id: 'second', mode: 'fast'},
  {role: 'assistant', content: 'Deuxième réponse', exchange_id: 'second', mode: 'fast', result: secondResult},
]);
const conversationHtml = elements.clientAdvice.innerHTML;
assert.ok(conversationHtml.indexOf('Deuxième question') < conversationHtml.indexOf('Première question'));
assert.ok(conversationHtml.includes('client-history-item'));
assert.ok(conversationHtml.includes('Voir 1 produit'));
const storedConversation = context.__quoteTest.getClientConversationForStorage();
assert.strictEqual(storedConversation[1].result.products[0].name, 'Premier produit');
context.__quoteTest.showHistoryProducts('first');
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(context.__quoteTest.currentProductNames())),
  ['Premier produit']
);

context.__quoteTest.setClientSearchMode('ai');
assert.strictEqual(context.__quoteTest.getClientSearchMode(), 'ai');
assert.strictEqual(modeButtons[0].ariaChecked, 'false');
assert.strictEqual(modeButtons[1].ariaChecked, 'true');
assert.strictEqual(context.__quoteTest.getClientSearchStateForStorage().mode, 'ai');

context.__quoteTest.setClientSearchMode('documented');
assert.strictEqual(context.__quoteTest.getClientSearchMode(), 'documented');
assert.strictEqual(modeButtons[1].ariaChecked, 'false');
assert.strictEqual(modeButtons[2].ariaChecked, 'true');
assert.strictEqual(context.__quoteTest.getClientSearchStateForStorage().mode, 'documented');

const documentedResult = context.__quoteTest.prepareClientResult({
  response_mode: 'documented',
  answer: 'Voici le point principal.',
  highlighted_product_ids: ['product:42'],
  products: [savedProduct],
  advice: {
    summary: 'Voici le point principal.',
    documentation: {
      key_points: [{
        heading: 'Ingrédient', detail: 'Information vérifiée.',
        source_ids: ['health-canada:12'],
      }],
      comparisons: [{
        candidate_id: 'product:42', difference: 'Format comprimé.',
        practical_note: 'Vérifier l’étiquette.', source_ids: ['health-canada:12'],
      }],
      useful_guidance: [{text: 'Confirmer le besoin.', source_ids: []}],
      important_checks: [{text: 'Vérifier les interactions.', source_ids: ['health-canada:12']}],
      sources: [{
        source_id: 'health-canada:12', title: 'Santé Canada - Produit Exemple',
        publisher: 'Santé Canada', url: 'https://example.test/source',
        summary: 'Fiche réglementaire.', candidate_ids: ['product:42'],
      }],
    },
  },
});
const documentedHtml = context.__quoteTest.renderDocumentedClientDetails(documentedResult, 'documented-1');
assert.strictEqual(documentedResult.response_mode, 'documented');
assert.ok(documentedHtml.includes('Points essentiels'));
assert.ok(documentedHtml.includes('Différences entre les produits'));
assert.ok(documentedHtml.includes('Sources consultées (1)'));
assert.ok(documentedHtml.includes('Santé Canada - Produit Exemple'));
const storedDocumented = context.__quoteTest.clientResultForStorage(documentedResult);
assert.strictEqual(storedDocumented.response_mode, 'documented');
assert.strictEqual(storedDocumented.advice.documentation.sources.length, 1);
const storedFallback = context.__quoteTest.clientResultForStorage({
  ...documentedResult,
  degraded: true,
  warning: "La réponse détaillée n'a pas été disponible à temps.",
});
assert.strictEqual(storedFallback.degraded, true);
assert.ok(storedFallback.warning.includes("réponse détaillée"));
const storedLegacyFallback = context.__quoteTest.clientResultForStorage({
  ...documentedResult,
  degraded: true,
  warning: "DeepSeek n'a pas répondu à temps; les produits et sources du magasin restent disponibles.",
});
assert.ok(!storedLegacyFallback.warning.includes("DeepSeek"));
assert.ok(storedLegacyFallback.warning.includes("réponse détaillée"));

const upgradedDocumented = context.__quoteTest.mergeDocumentedClientUpgrade(
  {
    ...documentedResult,
    degraded: true,
    ai_pending: true,
    documented_job_id: 'pending-job',
  },
  {
    answer: 'Réponse approfondie finale.',
    selected_product_ids: ['product:42'],
    follow_up_questions: ['Quel format préférez-vous?'],
    safety_flags: ['Vérifier l’étiquette.'],
    pharmacist_referral: false,
    pharmacist_reason: '',
    key_points: [{
      heading: 'Point final', detail: 'Détail final.',
      source_ids: ['health-canada:12'],
    }],
    comparisons: [],
    useful_guidance: [],
    important_checks: [],
    source_ids: ['health-canada:12'],
  },
  4200,
);
assert.strictEqual(upgradedDocumented.answer, 'Réponse approfondie finale.');
assert.strictEqual(upgradedDocumented.degraded, false);
assert.strictEqual(upgradedDocumented.ai_pending, false);
assert.strictEqual(
  upgradedDocumented.advice.documentation.key_points[0].heading,
  'Point final',
);

const headacheGroups = context.__quoteTest.clientRequiredConceptGroups(
  'Jai male a la tete que prendre'
);
const headacheExclusions = context.__quoteTest.clientExcludedConceptTerms(
  'Jai male a la tete que prendre'
);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'ADVIL 200MG CO100', brand: 'Advil', description: ''},
  headacheGroups, headacheExclusions,
), true);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'ORAL-B IO TETE BR/DENTS BLC 4', brand: 'Oral-B', description: ''},
  headacheGroups, headacheExclusions,
), false);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'TYLENOL RH/SIN JR NT CA20', brand: 'Tylenol', description: ''},
  headacheGroups, headacheExclusions,
), false);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'TYLENOL RH/TX/GR CA24', brand: 'Tylenol', description: ''},
  headacheGroups, headacheExclusions,
), false);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'MOTRIN NOURRISSON S/COLOR 30ML', brand: 'Motrin', description: ''},
  headacheGroups, headacheExclusions,
), false);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'TYLENOL X/F A/MUCUS 170ML', brand: 'Tylenol', description: ''},
  headacheGroups, headacheExclusions,
), false);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'LAKOTA EF ANALGESIQUE 57ML', brand: 'Lakota', description: ''},
  headacheGroups, headacheExclusions,
), false);
const topicalHeadacheGroups = context.__quoteTest.clientRequiredConceptGroups(
  'Jai mal a la tete et je cherche un analgesique topique'
);
const topicalHeadacheExclusions = context.__quoteTest.clientExcludedConceptTerms(
  'Jai mal a la tete et je cherche un analgesique topique'
);
assert.strictEqual(context.__quoteTest.productMatchesClientConcepts(
  {name: 'LAKOTA EF ANALGESIQUE 57ML', brand: 'Lakota', description: ''},
  topicalHeadacheGroups, topicalHeadacheExclusions,
), true);

const cottonGroups = context.__quoteTest.clientRequiredConceptGroups(
  'je cherche de la watte des petites boules de coton'
);
assert.strictEqual(
  context.__quoteTest.productMatchesClientConcepts(
    {name: 'PERSONNEL OUATE BOULES 100', brand: '', description: ''}, cottonGroups
  ),
  true
);
assert.strictEqual(
  context.__quoteTest.productMatchesClientConcepts(
    {name: 'CARTER PETITES PILULES LAX', brand: '', description: ''}, cottonGroups
  ),
  false
);
assert.strictEqual(
  context.__quoteTest.productMatchesClientConcepts(
    {name: 'Q-TIPS COTONS-TIGES 400', brand: '', description: ''}, cottonGroups
  ),
  false
);

const electricBrushGroups = context.__quoteTest.clientRequiredConceptGroups(
  'Brosse a dent electric'
);
const electricBrushExclusions = context.__quoteTest.clientExcludedConceptTerms(
  'Brosse a dent electric'
);
for (const name of [
  'ORAL-B P100 BR/DENTS ELEC NR 1',
  'ORAL-B D/C BR/DENTS A PILE 1',
  'SONICARE BR/DENT BH1022/04 TE2',
  'SONICARE RECH BROS HX9023/64 3',
  'ORAL-B IO TETE BR/DENTS BLC 4',
  'GUM BR/DENT CRAYOLA MARQ/ELEC1',
]) {
  assert.strictEqual(
    context.__quoteTest.productMatchesClientConcepts(
      {name, brand: '', description: ''}, electricBrushGroups
    ),
    true,
    `${name} should match the electric-toothbrush intent`
  );
}
assert.strictEqual(
  context.__quoteTest.productMatchesClientConcepts({
    name: 'PHILIPS SONI RECH HX6012/77 2', brand: 'Philips',
    description: 'Compatible avec tous les manches Sonicare.',
  }, electricBrushGroups, electricBrushExclusions),
  true,
  'an abbreviated Philips Sonicare replacement head should remain included'
);
for (const name of ['CURAPROX BR/DENT SMART 1', 'DENTA RINSE PRO .2% MENT 500ML']) {
  assert.strictEqual(
    context.__quoteTest.productMatchesClientConcepts(
      {name, brand: '', description: ''}, electricBrushGroups
    ),
    false,
    `${name} should not match the electric-toothbrush intent`
  );
}
assert.strictEqual(
  context.__quoteTest.productMatchesClientConcepts({
    name: 'SONICARE IRR S/FIL HX3826/23 1', brand: 'Sonicare',
    description: 'Utilisé en complément d’une brosse à dents manuelle.',
  }, electricBrushGroups, electricBrushExclusions),
  false,
  'a Sonicare irrigator must not become a toothbrush because its description compares both products'
);

const manyFastProducts = Array.from({length: 75}, (_, index) => ({
  id: index + 100,
  client_id: `product:${index + 100}`,
  name: `Produit rapide ${index + 1}`,
  description: 'Description rapide',
  search_terms: 'termes détaillés',
  image_url: `/product-${index + 1}.jpg`,
}));
const completeFastResult = context.__quoteTest.buildFastClientResult(manyFastProducts, 10);
assert.strictEqual(completeFastResult.products.length, 75);
assert.strictEqual(context.__quoteTest.prepareClientResult(completeFastResult).products.length, 75);
const storedFastResult = context.__quoteTest.clientResultForStorage(completeFastResult);
assert.strictEqual(storedFastResult.products.length, 75);
assert.strictEqual(storedFastResult.products[0].search_terms, '');

context.__quoteTest.clearClientHistory();
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(context.__quoteTest.historyState())),
  {messages: 0, products: 0, hasResult: false}
);
assert.strictEqual(elements.clientQuestion.value, 'Nouvelle demande non envoyée');
assert.strictEqual(elements.clientClearHistoryButton.hidden, true);

console.log('AI client UI tests passed.');
