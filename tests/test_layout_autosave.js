const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const scheduled = [];
const saveCalls = [];
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
  STORAGE_KEYS: {planSnapshot: ''},
  document: {getElementById() { return null; }},
  localStorage: {setItem() {}, removeItem() {}},
  loadEditorSession() { return {username: 'tester'}; },
  nowIsoWithoutMs() { return '2026-07-13T12:00:00'; },
  async apiUpdateLayoutAisle(aisle, payload) {
    saveCalls.push({aisle, payload});
    return {success: true, removed_products: 0};
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
  console.log('layout autosave tests passed');
}

run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
