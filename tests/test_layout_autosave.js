const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const scheduled = [];
const saveCalls = [];
const removalCalls = [];
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
  console.log('layout autosave tests passed');
}

run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
