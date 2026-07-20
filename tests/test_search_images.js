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
const referenceProduct = {
  catalog_only: true,
  name: 'Blistex test',
  barcode: '041388316000',
  image_url: '',
};
let replacement = null;
let referenceReplacement = null;
const placeholder = {replaceWith(node) { replacement = node; }};
const referencePlaceholder = {replaceWith(node) { referenceReplacement = node; }};

const context = {
  console,
  SEARCH_STOPWORDS: new Set(),
  allProductsCache: [product],
  apiGetProductImages: async ids => {
    assert.deepStrictEqual(Array.from(ids), [42]);
    return {images: {'42': 'https://img.test/razor.jpg'}};
  },
  apiGetReferenceProductImages: async barcodes => {
    assert.deepStrictEqual(Array.from(barcodes), ['041388316000']);
    return {images: {'041388316000': 'https://img.test/blistex.jpg'}};
  },
  document: {
    querySelectorAll(selector) {
      if (selector === '[data-product-image-id="42"]') return [placeholder];
      if (selector === '[data-reference-image-barcode="041388316000"]') return [referencePlaceholder];
      throw new Error(`Unexpected selector: ${selector}`);
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
context.window.AppSearch.startReferenceImagePolling([referenceProduct]);

setImmediate(() => {
  assert.strictEqual(product.image_url, 'https://img.test/razor.jpg');
  assert(replacement, 'the visible image placeholder should be replaced');
  assert.strictEqual(replacement.src, 'https://img.test/razor.jpg');
  assert.strictEqual(replacement.className, 'product-thumb');
  assert.strictEqual(referenceProduct.image_url, 'https://img.test/blistex.jpg');
  assert(referenceReplacement, 'the reference image placeholder should be replaced');
  assert.strictEqual(referenceReplacement.src, 'https://img.test/blistex.jpg');
  assert.strictEqual(referenceReplacement.className, 'product-thumb');
  console.log('search image polling tests passed');
});
