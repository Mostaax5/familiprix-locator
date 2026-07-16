const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const source = fs.readFileSync('static/scanner.js', 'utf8');

function scannerContext({mediaError=null, firstMediaError=null} = {}) {
  const elements = new Map();
  const element = id => ({
    id,
    style: {},
    textContent: '',
    innerHTML: '',
    readyState: 2,
    videoWidth: 1920,
    videoHeight: 1080,
    srcObject: null,
    async play() {},
    querySelector() { return null; },
  });
  for (const id of [
    'scannerStatus', 'cameraButton', 'cameraPreview', 'html5Reader',
    'searchScannerStatus', 'searchCameraButton', 'searchCameraPreview',
    'searchHtml5Reader', 'scanResult',
  ]) elements.set(id, element(id));

  const track = {
    stopped: false,
    stop() { this.stopped = true; },
    getCapabilities() { return {}; },
    async applyConstraints() {},
  };
  const stream = {
    getVideoTracks() { return [track]; },
    getTracks() { return [track]; },
  };
  const mediaRequests = [];
  let frameCallback = null;

  class NativeDetector {
    constructor(options) { this.options = options; }
    static async getSupportedFormats() {
      return ['ean_13', 'ean_8', 'upc_a', 'upc_e'];
    }
    async detect() { return [{rawValue: '063848966068'}]; }
  }

  const window = {
    BarcodeDetector: NativeDetector,
    isSecureContext: true,
    setTimeout() { return 1; },
    clearTimeout() {},
    setInterval() { return 1; },
    clearInterval() {},
  };
  const context = {
    console: {log() {}, warn() {}, error() {}},
    window,
    BarcodeDetector: NativeDetector,
    navigator: {
      mediaDevices: {
        async getUserMedia(constraints) {
          mediaRequests.push(constraints);
          if (firstMediaError && mediaRequests.length === 1) throw firstMediaError;
          if (mediaError) throw mediaError;
          return stream;
        },
      },
    },
    document: {
      body: {appendChild() {}},
      getElementById(id) { return elements.get(id) || null; },
      createElement(tag) { return element(tag); },
      querySelectorAll() { return []; },
    },
    location: {hostname: 'familiprix-locator.onrender.com'},
    localStorage: {getItem() { return null; }, setItem() {}, removeItem() {}},
    requestAnimationFrame(callback) { frameCallback = callback; return 1; },
    cancelAnimationFrame() { frameCallback = null; },
    setTimeout: window.setTimeout,
    clearTimeout: window.clearTimeout,
    setInterval: window.setInterval,
    clearInterval: window.clearInterval,
    mediaRequests,
    getFrameCallback: () => frameCallback,
    quaggaLoads: 0,
    ocrLoads: 0,
    zxingLoads: 0,
    quaggaStarts: 0,
    decoded: null,
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  vm.runInContext(`
    resetCameraCandidate = () => {};
    showCameraExtras = () => {};
    bumpCameraActivity = () => {};
    updateDeviceSupport = () => {};
    ensureQuaggaLoaded = async () => { quaggaLoads += 1; return false; };
    ensureOcrLoaded = async () => { ocrLoads += 1; return false; };
    ensureZXingLoaded = async () => { zxingLoads += 1; return false; };
    startQuaggaScanner = async () => { quaggaStarts += 1; };
    onDecodedCode = async (code, instant) => { decoded = {code, instant}; };
  `, context);
  return context;
}

(async () => {
  const android = scannerContext();
  await vm.runInContext('startCamera()', android);

  assert.strictEqual(android.mediaRequests.length, 1, 'native camera should open immediately');
  assert.strictEqual(android.quaggaLoads, 0, 'Android native path must not wait for Quagga');
  assert.strictEqual(android.ocrLoads, 0, 'Android native path must not download OCR');
  assert.strictEqual(android.zxingLoads, 0, 'Android native path must not wait for ZXing');
  assert.strictEqual(android.mediaRequests[0].video.facingMode.ideal, 'environment');

  await android.getFrameCallback()();
  assert.strictEqual(android.decoded?.code, '063848966068');
  assert.strictEqual(
    android.decoded?.instant, true,
    'native UPC reads should be accepted on the first valid frame',
  );

  const pickyAndroid = scannerContext({firstMediaError: {name: 'OverconstrainedError'}});
  await vm.runInContext('startCamera()', pickyAndroid);
  assert.strictEqual(pickyAndroid.mediaRequests.length, 2, 'camera constraints should retry once');
  assert.strictEqual(
    pickyAndroid.mediaRequests[1].video.facingMode.ideal, 'environment',
    'constraint retry should still request the rear camera',
  );

  const blockedNative = scannerContext({mediaError: {name: 'SecurityError'}});
  vm.runInContext(`
    ensureQuaggaLoaded = async () => {
      quaggaLoads += 1;
      window.Quagga = {};
      return true;
    };
  `, blockedNative);
  await vm.runInContext('startCamera()', blockedNative);
  assert.strictEqual(blockedNative.quaggaLoads, 1, 'web decoder should remain available');
  assert.strictEqual(blockedNative.quaggaStarts, 1, 'Quagga should start after native failure');
  assert.strictEqual(blockedNative.ocrLoads, 0, 'OCR should still stay lazy');

  assert(
    !/\.decodeFromCanvas\s*\(/.test(source),
    'scanner must not call the missing ZXing decodeFromCanvas API',
  );

  console.log('Android scanner tests passed');
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
