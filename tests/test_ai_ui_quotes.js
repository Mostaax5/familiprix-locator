const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const elements = {
  clientQuotePreview: {hidden: true},
  clientQuoteText: {textContent: ''},
  clientProductQuotePreview: {hidden: true},
  clientProductQuoteText: {textContent: ''},
  clientProductModal: {hidden: true},
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
  state: () => ({quote: _clientSelectedQuote, focus: _clientQuoteFocusProductId}),
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

console.log('AI quote UI tests passed.');
