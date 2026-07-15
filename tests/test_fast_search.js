const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const products = Array.from({length: 12000}, (_, index) => ({
  id: index + 1,
  name: `Produit ${index + 1}`,
  brand: '',
  description: '',
  search_terms: '',
  usage_notes: '',
  alternative_suggestions: '',
  barcode: `063840${String(index).padStart(6, '0')}`,
  in_stock: 1,
}));
products[450].barcode = '063848904961';
products[900].barcode = '123456784961';

const context = {
  console,
  SEARCH_STOPWORDS: new Set(),
  allProductsCache: products,
  window: {AppSearch: {}, clearTimeout() {}, setTimeout() {}},
};

vm.createContext(context);
vm.runInContext(fs.readFileSync('static/search.js', 'utf8'), context);

const started = performance.now();
const suffixMatches = context.window.AppSearch.searchProductsFromCache('4961', 40);
const elapsed = performance.now() - started;

assert.deepStrictEqual(
  Array.from(suffixMatches, product => product.id),
  [451, 901, 4962],
  'last-four UPC search should return every matching plan location/product'
);
assert(elapsed < 300, `12k-product last-four search took ${elapsed.toFixed(1)} ms`);
assert(
  !Object.prototype.hasOwnProperty.call(products[0], '_sf'),
  'numeric lookup should use the barcode index instead of scoring every product'
);

const exactMatches = context.window.AppSearch.productsByBarcodeFromCache('0063848904961');
assert.deepStrictEqual(
  Array.from(exactMatches, product => product.id),
  [451],
  'equivalent leading-zero UPCs should resolve to the same product'
);

products.push({
  id: 12001, name: 'Nouveau', brand: '', description: '', search_terms: '',
  usage_notes: '', alternative_suggestions: '', barcode: '999999994961', in_stock: 1,
});
context.window.AppSearch.invalidateProductSearchIndexes();
assert.strictEqual(
  context.window.AppSearch.productsByBarcodeFromCache('4961').length,
  4,
  'product edits should invalidate the barcode index'
);

console.log(`fast search tests passed (${elapsed.toFixed(1)} ms for 12k products)`);
