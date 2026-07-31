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
products[100].identifiers = [
  {type: 'NPN', value: '80123456', status: 'probable'},
  {type: 'MANUFACTURER_PART_NUMBER', value: 'MFG-ABC-900'},
];
products[101].identifiers = [{type: 'DIN', value: '01234567'}];
products[101].product_code = 'FAM-7711';

const configSource = fs.readFileSync('static/config.js', 'utf8');
const stopwordMatch = configSource.match(/const SEARCH_STOPWORDS = new Set\(\[([\s\S]*?)\]\);/);
assert.ok(stopwordMatch, 'config.js must expose the shared search stopwords');
const configContext = {};
vm.runInNewContext(`result = new Set([${stopwordMatch[1]}]);`, configContext);

const context = {
  console,
  SEARCH_STOPWORDS: configContext.result,
  allProductsCache: products,
  window: {AppSearch: {}, clearTimeout() {}, setTimeout() {}},
};

vm.createContext(context);
vm.runInContext(fs.readFileSync('static/search.js', 'utf8'), context);
assert.deepStrictEqual(
  Array.from(vm.runInContext(`tokenizeSearchQuery(
    "Peux tu me dire tout les type de melatonine les saveurs qu'on a en magasin et les difference de context dans lequel les utiliser"
  )`, context)),
  ['melatonine'],
  'browser retrieval must ignore answer-shaping words in a long client question'
);

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

assert.deepStrictEqual(
  Array.from(context.window.AppSearch.searchProductsByFieldFromCache('80123456', 'npn'), product => product.id),
  [101],
  'NPN mode should search only NPN identifiers, including probable candidates'
);
assert.deepStrictEqual(
  Array.from(context.window.AppSearch.searchProductsByFieldFromCache('MFG-ABC-900', 'manufacturer_part_number'), product => product.id),
  [101],
  'manufacturer mode should search the manufacturer identifier'
);
assert.deepStrictEqual(
  Array.from(context.window.AppSearch.searchProductsByFieldFromCache('01234567', 'npn'), product => product.id),
  [],
  'an explicit identifier mode must not leak matches from another identifier type'
);
assert.deepStrictEqual(
  Array.from(context.window.AppSearch.searchProductsByFieldFromCache('FAM-7711', 'code'), product => product.id),
  [102],
  'Familiprix-code mode should remain strict and support alphanumeric codes'
);
const mergedIdentifierResults = context.window.AppSearch.mergeIndexedSearchResults(
  [{
    ...products[101],
    regulatory_identifiers: [{
      type: 'DIN', value: '01234567', status: 'probable', label: 'À confirmer',
    }],
  }],
  [{...products[101], regulatory_identifiers: []}],
  40
);
assert.strictEqual(
  mergedIdentifierResults[0].regulatory_identifiers[0].label,
  'À confirmer',
  'the indexed probable identifier must replace stale cached metadata'
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

for (let index = 0; index < 150; index += 1) {
  products.push({
    id: 13000 + index,
    name: `AAA BROSSE DENT MANUELLE ${index}`,
    brand: '', description: '', search_terms: '', usage_notes: '',
    alternative_suggestions: '', barcode: `777777${String(index).padStart(6, '0')}`,
    in_stock: 1,
  });
}
const batteryBrush = {
  id: 14000, name: 'ZZZ ORAL-B BR/DENTS A PILE 1', brand: 'Oral-B',
  description: '', search_terms: '', usage_notes: '', alternative_suggestions: '',
  barcode: '888888888888', in_stock: 1,
};
const replacementHeads = {
  id: 14001, name: 'SONICARE RECH BROS HX9023 3', brand: 'Sonicare',
  description: '', search_terms: '', usage_notes: '', alternative_suggestions: '',
  barcode: '888888888889', in_stock: 1,
};
products.push(batteryBrush, replacementHeads);
context.window.AppSearch.invalidateProductSearchIndexes();
const electricCandidates = context.window.AppSearch.searchProductsFromCache(
  'brosse a dent electric', 100, 100,
  product => product.id === batteryBrush.id || product.id === replacementHeads.id
);
assert.deepStrictEqual(
  new Set(Array.from(electricCandidates, product => product.id)),
  new Set([batteryBrush.id, replacementHeads.id]),
  'the Client predicate must run before the 100-result limit and retain battery brushes and heads'
);
assert(
  electricCandidates.indexOf(batteryBrush) < electricCandidates.indexOf(replacementHeads),
  'a complete electric toothbrush should rank before replacement heads'
);

const cottonBalls = {
  id: 15000, name: 'PERSONNEL OUATE BOULES 100', brand: 'Personnelle',
  description: '', search_terms: '', usage_notes: '', alternative_suggestions: '',
  barcode: '999999999991', in_stock: 1,
};
const chocolateBalls = {
  id: 15001, name: 'REGAL BOULE DOR CHOCOLAT 144G', brand: 'Regal',
  description: '', search_terms: '', usage_notes: '', alternative_suggestions: '',
  barcode: '999999999992', in_stock: 1,
};
const cottonSwabs = {
  id: 15002, name: 'Q-TIPS COTONS-TIGES 400', brand: 'Q-Tips',
  description: '', search_terms: '', usage_notes: '', alternative_suggestions: '',
  barcode: '999999999993', in_stock: 1,
};
products.push(cottonBalls, chocolateBalls, cottonSwabs);
assert.deepStrictEqual(
  Array.from(
    context.window.AppSearch.searchProductsFromCache(
      'je cherche de la watte des petites boules de coton', 40
    ),
    product => product.id
  ),
  [cottonBalls.id],
  'spoken cotton-ball searches must reject chocolates and cotton swabs'
);

const biomedicCharcoal = {
  id: 16000, name: 'BIOMEDIC CHARB ACT 225MG CA75', brand: 'Biomedic',
  description: 'Capsules de charbon active', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '063848908532', in_stock: 1,
};
const pillCrusher = {
  id: 16001, name: 'BIOMEDIC ECRASE COUPE PILULE 1', brand: 'Biomedic',
  description: 'Broyeur de comprimes', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '063848960677', in_stock: 1,
};
const charcoalToothpaste = {
  id: 16002, name: 'CREST 3DW CHARBON FLUOR 135ML', brand: 'Crest',
  description: 'Dentifrice au charbon', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '030772053836', in_stock: 1,
};
const contaminatedCharcoalCosmetic = {
  id: 16003, name: 'LOREAL MEN NETT CHARBON 100ML', brand: 'Loreal',
  description: 'Capsules de charbon active', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '030772053837', in_stock: 1,
};
products.push(
  biomedicCharcoal, pillCrusher, charcoalToothpaste,
  contaminatedCharcoalCosmetic
);
assert.deepStrictEqual(
  Array.from(
    context.window.AppSearch.searchProductsFromCache('pilule de charbon', 40),
    product => product.id
  ),
  [biomedicCharcoal.id],
  'charcoal-pill searches must keep the exact capsule and reject pill tools and toothpaste'
);

const cranberryCapsules = {
  id: 17000, name: 'WEBBER CANNEB 10000MG CA90', brand: 'Webber',
  description: 'Capsules de canneberge', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111111', in_stock: 1,
};
const coffeeCapsules = {
  id: 17001, name: 'CAFE CAPSULE INTENSE 10', brand: 'Test',
  description: 'Capsules de cafe', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111112', in_stock: 1,
};
const mosquitoSpray = {
  id: 17002, name: 'OFF CHASSE MOUST VAPO 142G', brand: 'Off',
  description: 'Vaporisateur chasse-moustiques', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111113', in_stock: 1,
};
const hairSpray = {
  id: 17003, name: 'SPRAY COIFFANT 200ML', brand: 'Test',
  description: 'Fixatif en vaporisateur', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111114', in_stock: 1,
};
const bathFoam = {
  id: 17004, name: 'ATTITUDE B/L B/MOUS 473ML', brand: 'Attitude',
  description: 'Bain moussant', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111115', in_stock: 1,
};
const antiperspirant = {
  id: 17005, name: 'DOVE MEN ANTI VAPO 107G', brand: 'Dove',
  description: 'Antisudorifique en vaporisateur', search_terms: '', usage_notes: '',
  alternative_suggestions: '', barcode: '111111111116', in_stock: 1,
};
products.push(bathFoam, antiperspirant);
assert.deepStrictEqual(
  Array.from(
    context.window.AppSearch.searchProductsFromCache('spray anti moustique', 40),
    product => product.id
  ),
  [],
  'an absent requested object must not fall back to bath foam or antiperspirant products'
);
products.push(cranberryCapsules, coffeeCapsules, mosquitoSpray, hairSpray);
assert.deepStrictEqual(
  Array.from(
    context.window.AppSearch.searchProductsFromCache('capsules de canneberge', 40),
    product => product.id
  ),
  [cranberryCapsules.id],
  'a specific ingredient must outrank and remove unrelated products sharing only the dosage form'
);
assert.deepStrictEqual(
  Array.from(
    context.window.AppSearch.searchProductsFromCache('spray anti moustique', 40),
    product => product.id
  ),
  [mosquitoSpray.id],
  'a requested use must remove unrelated products sharing only the spray format'
);

console.log(`fast search tests passed (${elapsed.toFixed(1)} ms for 12k products)`);
