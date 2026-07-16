const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const product = {
  id: 42,
  name: 'Rasoir test',
  brand: '',
  description: '',
  search_terms: '',
  usage_notes: '',
  alternative_suggestions: '',
  barcode: '063848966068',
  image_url: '',
};
let replacement = null;
const placeholder = {replaceWith(node) { replacement = node; }};

const context = {
  console,
  SEARCH_STOPWORDS: new Set(),
  allProductsCache: [product],
  apiGetProductImages: async ids => {
    assert.deepStrictEqual(Array.from(ids), [42]);
    return {images: {'42': 'https://img.test/razor.jpg'}};
  },
  document: {
    querySelectorAll(selector) {
      assert.strictEqual(selector, '[data-product-image-id="42"]');
      return [placeholder];
    },
    createElement(tag) {
      assert.strictEqual(tag, 'img');
      return {};
    },
  },
  window: {
    AppSearch: {},
    clearTimeout() {},
    setTimeout() { return 1; },
  },
};

vm.createContext(context);
vm.runInContext(fs.readFileSync('static/search.js', 'utf8'), context);
context.window.AppSearch.startSearchImagePolling([product]);

setImmediate(() => {
  assert.strictEqual(product.image_url, 'https://img.test/razor.jpg');
  assert(replacement, 'the visible image placeholder should be replaced');
  assert.strictEqual(replacement.src, 'https://img.test/razor.jpg');
  assert.strictEqual(replacement.className, 'product-thumb');
  console.log('search image polling tests passed');
});
