const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const scheduled = [];
const saveCalls = [];
const removalCalls = [];
const bulkMoveCalls = [];
const bulkDeleteCalls = [];
const planMessage = {className: '', textContent: ''};
const deleteButton = {
  dataset: {},
  disabled: false,
  textContent: '✕ Suppr.',
  attributes: {},
  setAttribute(name, value) { this.attributes[name] = value; },
  removeAttribute(name) { delete this.attributes[name]; },
};
const context = {
  console,
  esc(value) { return String(value ?? ''); },
  mapLayouts: [{
    aisle: '1', max_section: '1', max_shelf: '1', max_position: '4',
    config: {
      sides: {
        Gauche: {sections: [{shelves: [4], labels: ['']}]},
        Droite: {sections: []},
      },
      facade_a: {shelves: [], labels: []},
      facade_b: {shelves: [], labels: []},
      presentoirs: [],
    },
  }],
  dirtyLayoutAisles: new Set(),
  allProductsCache: [],
  lastProductsRefreshAt: 0,
  STORAGE_KEYS: {planSnapshot: ''},
  document: {getElementById(id) { return id === 'addMsg' ? planMessage : null; }},
  localStorage: {setItem() {}, removeItem() {}},
  confirm() { return true; },
  loadEditorSession() { return {username: 'tester'}; },
  nowIsoWithoutMs() { return '2026-07-13T12:00:00'; },
  async apiUpdateLayoutAisle(aisle, payload) {
    saveCalls.push({aisle, payload});
    return {success: true, removed_products: 0};
  },
  async apiRemoveLayoutPart(aisle, endpoint, payload) {
    removalCalls.push({aisle, endpoint, payload});
    const config = JSON.parse(JSON.stringify(payload.config));
    if (endpoint === 'remove-section') {
      config.sides[payload.side].sections.splice(Number(payload.section) - 1, 1);
    } else {
      const section = config.sides[payload.side].sections[Number(payload.section) - 1];
      section.shelves.splice(Number(payload.shelf) - 1, 1);
      section.labels.splice(Number(payload.shelf) - 1, 1);
    }
    return {success: true, config, removed_products: 0};
  },
  window: {
    AppLayout: {},
    setTimeout(callback, delay) {
      scheduled.push({callback, delay, cancelled: false});
      return scheduled.length - 1;
    },
    clearTimeout(id) {
      if (scheduled[id]) scheduled[id].cancelled = true;
    },
  },
};

vm.createContext(context);
vm.runInContext(fs.readFileSync('static/layout-ui.js', 'utf8'), context);

async function run() {
  vm.runInContext("markLayoutDirty('1')", context);
  assert(context.dirtyLayoutAisles.has('1'), 'editing should mark the aisle dirty');
  assert.strictEqual(scheduled.at(-1).delay, 700, 'autosave should be debounced');

  await scheduled.at(-1).callback();

  assert.strictEqual(saveCalls.length, 1, 'autosave should persist one layout');
  assert.strictEqual(saveCalls[0].aisle, '1');
  assert.deepStrictEqual(saveCalls[0].payload.config.sides.Gauche.sections[0].shelves, [4]);
  assert(!context.dirtyLayoutAisles.has('1'), 'a successful autosave should clear dirty state');

  vm.runInContext(`
    rerenderSection = () => {};
    rerenderShelfCard = () => {};
    addShelf('1', 'Gauche', 0);
    setShelfPositionCount('1', 'Gauche', 0, 0, 6);
    setSectionShelfCount('1', 'Gauche', 0, 3);
  `, context);
  assert.deepStrictEqual(
    context.mapLayouts[0].config.sides.Gauche.sections[0].shelves,
    [6, 4, 4],
    'the tablet Plan buttons should update shelves and positions'
  );

  vm.runInContext(`
    refreshPlanUi = () => {};
    savePlanSnapshot = () => {};
  `, context);
  await vm.runInContext("removeShelf('1', 'Gauche', 0, 0)", context);
  assert.deepStrictEqual(
    context.mapLayouts[0].config.sides.Gauche.sections[0].shelves,
    [4, 4],
    'the Suppr. button should remove its shelf immediately'
  );
  vm.runInContext(`
    mapLayouts[0].config.sides.Gauche.sections.push({shelves: [2], labels: ['']});
  `, context);
  await vm.runInContext("removeSection('1', 'Gauche', 0)", context);
  assert.strictEqual(
    JSON.stringify(context.mapLayouts[0].config.sides.Gauche.sections),
    JSON.stringify([{shelves: [2], labels: ['']}]),
    'the Supprimer section button should remove its section immediately'
  );
  assert.deepStrictEqual(
    removalCalls.map(call => call.endpoint),
    ['remove-shelf', 'remove-section'],
    'both delete controls should use the atomic removal API'
  );
  context.apiRemoveLayoutPart = async () => ({success: false, error: 'Server refused deletion'});
  context.deleteButton = deleteButton;
  await vm.runInContext("removeShelf('1', 'Gauche', 0, 0, deleteButton)", context);
  assert.deepStrictEqual(
    context.mapLayouts[0].config.sides.Gauche.sections[0].shelves,
    [2],
    'a failed deletion must leave the plan unchanged'
  );
  assert.strictEqual(deleteButton.disabled, false, 'the delete button should become usable again');
  assert.strictEqual(deleteButton.textContent, '✕ Suppr.');
  assert.strictEqual(planMessage.textContent, 'Server refused deletion');

  const flow = vm.runInContext(`
    planoData = {products: Array.from({length: 22}, (_, index) => ({
      tablette: index + 1, position: 1, en_stock: true
    }))};
    computePlanoFlow({
      sides: {
        Gauche: {sections: [{shelves: [4]}]},
        Droite: {sections: [
          {shelves: [8,8,8,8,8,8,8]},
          {shelves: [8,8,8,8,8,8,8]},
          {shelves: [8,8,8,8,8,8,8]}
        ]}
      }
    }, 'Droite', 1, 1, 1, 99, false)
  `, context);
  assert.strictEqual(flow.startSectionShelves, 7, 'the starting section should show its physical shelf count');
  assert.strictEqual(flow.availableShelves, 21, 'the import path should total shelves across sections');
  assert.strictEqual(flow.availableSections, 3, 'the total should say how many sections it spans');
  assert.deepStrictEqual([flow.byIdx[6].section, flow.byIdx[6].shelf], [1, 7]);
  assert.deepStrictEqual([flow.byIdx[7].section, flow.byIdx[7].shelf], [2, 1]);
  assert.deepStrictEqual([flow.byIdx[13].section, flow.byIdx[13].shelf], [2, 7]);
  assert.deepStrictEqual([flow.byIdx[14].section, flow.byIdx[14].shelf], [3, 1]);
  assert.deepStrictEqual([flow.byIdx[20].section, flow.byIdx[20].shelf], [3, 7]);
  assert(flow.overflow.has(21), 'the 22nd PDF shelf should overflow after all 21 physical shelves');

  const filteredFlow = vm.runInContext(`
    planoData = {products: [
      {tablette: 1, position: 1, en_stock: true},
      {tablette: 1, position: 2, en_stock: false},
      {tablette: 1, position: 3, en_stock: false}
    ]};
    computePlanoFlow({
      sides: {
        Gauche: {sections: [{shelves: [3]}]},
        Droite: {sections: []}
      }
    }, 'Gauche', 1, 1, 1, 1, true)
  `, context);
  assert.strictEqual(filteredFlow.placed, 1, 'only in-stock products should be placed when filtering');
  assert.strictEqual(filteredFlow.filteredNonStock, 2, 'the preview must report every filtered product');

  context.skipNonStockControl = {checked: true};
  context.replaceControl = {checked: false};
  vm.runInContext(`
    document.getElementById = id => id === 'planoSkipNonStock'
      ? skipNonStockControl
      : (id === 'planoReplace' ? replaceControl : null);
    updatePlanoPreview = () => {};
    reimportCalled = false;
    importPlanogram = () => { reimportCalled = true; };
    reimportIncludingNonStock();
  `, context);
  assert.strictEqual(context.skipNonStockControl.checked, false, 'one-click recovery should include out-of-stock products');
  assert.strictEqual(context.replaceControl.checked, true, 'one-click recovery should safely replace the partial tablet');
  assert.strictEqual(context.reimportCalled, true, 'one-click recovery should immediately rerun this plano import');

  const coteAFlow = vm.runInContext(`
    planoData = {products: [
      {tablette: 1, position: 1, en_stock: true},
      {tablette: 1, position: 2, en_stock: true},
      {tablette: 2, position: 1, en_stock: true},
      {tablette: 2, position: 2, en_stock: true}
    ]};
    computePlanoFlow({
      sides: {
        Gauche: {sections: Array.from({length: 9}, () => ({shelves: [3]}))},
        Droite: {sections: Array.from({length: 9}, () => ({shelves: [3]}))}
      }
    }, 'Gauche', 9, 1, 1, 99, false)
  `, context);
  assert.deepStrictEqual(
    [
      coteAFlow.byIdx[0].section, coteAFlow.byIdx[0].position,
      coteAFlow.byIdx[1].section, coteAFlow.byIdx[1].position,
      coteAFlow.byIdx[2].section, coteAFlow.byIdx[2].position,
      coteAFlow.byIdx[3].section, coteAFlow.byIdx[3].position,
    ],
    [9, 1, 9, 2, 8, 1, 8, 2],
    'Cote A should descend S9 to S8 without reversing positions inside a shelf'
  );

  context.allProductsCache = [
    {id: 101, name: 'Produit A', aisle: '1', side: 'Gauche', section: '1', shelf: '1', position: '1', modified_at: 'v1'},
    {id: 102, name: 'Produit B', aisle: '1', side: 'Gauche', section: '1', shelf: '2', position: '1', modified_at: 'v2'},
    {id: 103, name: 'Produit C', aisle: '2', side: 'Droite', section: '3', shelf: '1', position: '1', modified_at: 'v3'},
  ];
  context.lastProductsRefreshAt = 100;
  const sectionIds = JSON.parse(vm.runInContext(
    "JSON.stringify(planScopeProductIds('section',{aisle:'1',side:'Gauche',section:'1'}))",
    context,
  ));
  const aisleIds = JSON.parse(vm.runInContext(
    "JSON.stringify(planScopeProductIds('aisle',{aisle:'1'}))",
    context,
  ));
  assert.deepStrictEqual(sectionIds, [101, 102], 'a section selector should include only that section');
  assert.deepStrictEqual(aisleIds, [101, 102], 'an aisle selector should include all products in that aisle');
  assert(
    vm.runInContext("renderPlanSelectionCheckbox('shelf','1','Gauche','1','1').includes('data-select-kind=\"shelf\"')", context),
    'tablet selectors should carry an exact scope',
  );

  context.normalizeProduct = product => ({...product});
  context.requireEditorSession = () => true;
  context.invalidateProductSearchIndexes = () => {};
  context.refreshPlanUi = () => {};
  context.savePlanSnapshot = () => {};
  context.apiBulkMoveLayoutProducts = async payload => {
    bulkMoveCalls.push(payload);
    return {
      success: true,
      moved_products: 2,
      product_updates: context.allProductsCache.slice(0, 2).map((product, index) => ({
        id: product.id, section: '1', shelf: '1', position: String(index + 1), modified_at: 'moved',
      })),
    };
  };
  context.dirtyLayoutAisles.clear();
  vm.runInContext('planSelectedProductIds.add(101); planSelectedProductIds.add(102)', context);
  await vm.runInContext(
    "movePlanSelection({aisle:'1',side:'Gauche',section:'1',shelf:'1',mode:'shelf'})",
    context,
  );
  assert.strictEqual(bulkMoveCalls.length, 1, 'one atomic request should move the whole selection');
  assert.deepStrictEqual(Array.from(bulkMoveCalls[0].product_ids), [101, 102]);
  assert.deepStrictEqual(
    JSON.parse(JSON.stringify(context.allProductsCache.slice(0, 2).map(product => [product.shelf, product.position]))),
    [['1', '1'], ['1', '2']],
    'the local plan should immediately mirror the committed positions',
  );
  assert.strictEqual(vm.runInContext('planSelectedProductIds.size', context), 0);

  context.apiBulkDeleteLayoutProducts = async payload => {
    bulkDeleteCalls.push(payload);
    return {success: true, removed_products: 1, deleted_product_ids: [101]};
  };
  await vm.runInContext("deletePlanProducts([101], 'cette tablette')", context);
  assert.strictEqual(bulkDeleteCalls.length, 1, 'scoped deletion should be one atomic request');
  assert.deepStrictEqual(
    Array.from(context.allProductsCache, product => product.id),
    [102, 103],
    'scoped deletion must keep products outside the exact selection',
  );
  assert.deepStrictEqual(
    context.mapLayouts[0].config.sides.Gauche.sections[0].shelves,
    [2],
    'product-only deletion must preserve the tablet structure',
  );
  const renderedTablet = vm.runInContext("renderShelfCard('1','Gauche',0,0,2,'')", context);
  assert(renderedTablet.includes('data-select-kind="shelf"'), 'a tablet should have a scope selector');
  assert(renderedTablet.includes('data-drop-mode="shelf"'), 'a tablet should be a drop destination');
  assert(renderedTablet.includes('Vider produits (1)'), 'a tablet should expose product-only clearing');
  assert(renderedTablet.includes('plan-drag-handle'), 'products should expose a desktop drag handle');
  assert(vm.runInContext('renderPlanBulkToolbar()', context).includes('planSelectionMove'));
  console.log('layout autosave tests passed');
}

run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
