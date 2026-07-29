const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const results = {innerHTML: ''};
const input = {value: 'Advil'};
const field = {value: ''};
let serverCalls = 0;
const serverProduct = {
  id: 22,
  name: 'ADVIL 200MG CO100',
  brand: 'Advil',
  description: '',
  search_terms: '',
  usage_notes: '',
  alternative_suggestions: '',
  barcode: '063848900022',
  in_stock: 1,
};

const context = {
  console,
  SEARCH_STOPWORDS: new Set(['a', 'de', 'la']),
  allProductsCache: [{
    id: 1,
    name: 'PRODUIT SANS RAPPORT',
    brand: '',
    description: '',
    search_terms: '',
    usage_notes: '',
    alternative_suggestions: '',
    barcode: '111111111111',
    in_stock: 1,
  }],
  apiSearchProducts: async () => {
    serverCalls += 1;
    return [serverProduct];
  },
  apiSearchReference: async () => [],
  looksLikeCompleteRetailBarcode: () => false,
  groupAndRenderSearchResults: products => products.map(product => product.name).join('|'),
  esc: value => String(value),
  document: {
    getElementById(id) {
      if (id === 'searchResults') return results;
      if (id === 'searchInput') return input;
      if (id === 'searchField') return field;
      return null;
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
context.groupAndRenderSearchResults = products => (
  products.map(product => product.name).join('|')
);

(async () => {
  await context.window.AppSearch.doSearchValue('Advil');
  assert.strictEqual(
    serverCalls,
    1,
    'a populated but partial phone cache must still query the current server index'
  );
  assert(
    results.innerHTML.includes(serverProduct.name),
    `the reconciled server result should replace the temporary empty cache result: ${results.innerHTML}`
  );
  console.log('search revalidation tests passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
