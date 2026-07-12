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

const context = {
  console,
  currentClientMatches: [],
  document: {
    getElementById(id) { return elements[id] || null; },
    querySelectorAll() { return []; },
  },
  esc(value) { return String(value ?? ''); },
  persistClientDraft() {},
  window: {
    AppAI: {},
    setTimeout(callback) { callback(); },
  },
};
vm.createContext(context);

const source = fs.readFileSync('static/ai-ui.js', 'utf8');
vm.runInContext(`${source}\n;globalThis.__quoteTest = {
  renderQuotableClientAnswer,
  clientQuoteButton,
  quoteClientPassage,
  clearClientSelectedQuote,
  clearClientHistory,
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
};`, context);

const longAnswer = [
  'Première phrase qui explique clairement une différence au client.',
  'Deuxième phrase qui ajoute une autre information pratique et facile à citer.',
  'Troisième phrase qui termine ce long passage avec une recommandation prudente.',
  'Quatrième phrase ajoutée pour dépasser la taille maximale d’un seul passage sur téléphone.',
].join(' ');
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

context.__quoteTest.clearClientHistory();
assert.deepStrictEqual(
  JSON.parse(JSON.stringify(context.__quoteTest.historyState())),
  {messages: 0, products: 0, hasResult: false}
);
assert.strictEqual(elements.clientQuestion.value, 'Nouvelle demande non envoyée');
assert.strictEqual(elements.clientClearHistoryButton.hidden, true);

console.log('AI client UI tests passed.');
