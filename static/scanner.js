// ── Scanner state ─────────────────────────────────────────────────────────────
let scannerStream = null;
let scanPaused = false;
let html5Scanner = null;
let hardwareScanBuffer = '';
let hardwareScanTimer = null;
let cameraCandidateBarcode = '';
let cameraCandidateCount = 0;
let cameraCandidateTimer = null;
let lastAcceptedCameraBarcode = '';
let lastAcceptedCameraAt = 0;
let cameraUsageMode = 'scan';
let scanFrameTimer = null;
let cameraTrack = null;
let quaggaActive = false;
let quaggaDetectedHandler = null;
let quaggaProcessedHandler = null;
let quaggaOcrTimer = null;
let quaggaStartedAt = 0;
let ocrBusy = false;
let quaggaLibraryPromise = null;
let ocrLibraryPromise = null;
let nativeScanActive = false;
let nativeScanFrame = null;
let torchEnabled = false;
let zxingActive = false;
let zxingFrame = null;
let zxingLibraryPromise = null;
let zoomDebounceTimer = null;
let lastOcrCandidate = '';
let ocrHistory = [];        // recent valid OCR candidates (for agreement check)
let ocrLoadState = 'idle';  // 'idle' | 'loading' | 'ready' | 'failed' — shown on screen

// ── Live diagnostic panel (temporary, on-screen, no devtools needed) ──────────
const _dbg = {q:'?', z:'?', t:'?', qRead:'-', qErr:'-', zRead:'-', ocr:'-'};
function _renderDbg() {
  let el = document.getElementById('scanDbgPanel');
  if (!el) {
    el = document.createElement('div');
    el.id = 'scanDbgPanel';
    el.style.cssText = 'position:fixed;left:4px;right:4px;bottom:4px;z-index:9999;'
      + 'font:11px/1.4 monospace;background:rgba(15,23,42,.94);color:#a7f3d0;'
      + 'padding:7px 9px;border-radius:8px;white-space:pre-wrap;pointer-events:none';
    document.body.appendChild(el);
  }
  el.textContent =
    `moteurs  Quagga:${_dbg.q}  ZXing:${_dbg.z}  OCR:${_dbg.t}\n` +
    `Quagga lit : ${_dbg.qRead}   (erreur ${_dbg.qErr})\n` +
    `ZXing  lit : ${_dbg.zRead}\n` +
    `OCR    lit : ${_dbg.ocr}`;
}
function _removeDbg() {
  const el = document.getElementById('scanDbgPanel');
  if (el) el.remove();
}

// ── Camera DOM helpers ────────────────────────────────────────────────────────
function getCameraDom() {
  if (cameraUsageMode === 'search') {
    return {
      status: document.getElementById('searchScannerStatus'),
      button: document.getElementById('searchCameraButton'),
      video: document.getElementById('searchCameraPreview'),
      reader: document.getElementById('searchHtml5Reader')
    };
  }
  return {
    status: document.getElementById('scannerStatus'),
    button: document.getElementById('cameraButton'),
    video: document.getElementById('cameraPreview'),
    reader: document.getElementById('html5Reader')
  };
}

// ── Camera controls ───────────────────────────────────────────────────────────
function toggleScanPause() {
  const btn = document.getElementById('pauseScanButton');
  const status = document.getElementById('scannerStatus');
  if (scanPaused) {
    scanPaused = false;
    resetCameraCandidate();
    if (btn) { btn.textContent = '⏸ Pause'; btn.style.background = ''; btn.style.color = ''; btn.style.borderColor = ''; }
    if (quaggaActive) status.textContent = 'Cadrez les barres et les chiffres';
  } else {
    scanPaused = true;
    if (btn) { btn.textContent = '▶ Reprendre'; btn.style.background = '#16a34a'; btn.style.color = 'white'; btn.style.borderColor = '#16a34a'; }
    status.textContent = '⏸ En pause';
  }
}

async function startSearchScan() {
  if (scannerStream || html5Scanner || quaggaActive) {
    await stopCamera();
  }
  cameraUsageMode = 'search';
  document.getElementById('searchResults').innerHTML = '<div class="msg info">Ouverture de la camera pour rechercher un produit...</div>';
  await startCamera();
}

async function toggleCamera() {
  if (scannerStream || html5Scanner || quaggaActive) {
    await stopCamera();
    return;
  }
  if (!requireEditorSession('ouvrir la camera de scan')) return;
  cameraUsageMode = 'scan';
  await startCamera();
}

async function startCamera() {
  const {status, button, video, reader} = getCameraDom();

  if (!('mediaDevices' in navigator) || !navigator.mediaDevices.getUserMedia) {
    status.textContent = 'Camera non disponible';
    showCameraHint('Ce navigateur ne donne pas acces a la camera. Utilisez un navigateur mobile recent ou entrez le code manuellement.');
    return;
  }

  resetCameraCandidate();

  // Native BarcodeDetector: hardware-accelerated, works on iOS 17.4+ AND Android Chrome
  if ('BarcodeDetector' in window) {
    try {
      await startNativeScan(video, status, button, reader);
      return;
    } catch (err) {
      console.warn('[Scanner] BarcodeDetector failed, falling to Quagga:', err);
    }
  }

  // Pre-load Tesseract NOW so OCR is ready the moment Quagga needs a fallback.
  // Without this, OCR fires at ~1.5s but Tesseract still takes 3-5s to download,
  // so the first several OCR cycles silently return null.
  ensureOcrLoaded().catch(() => {});

  // iPhone / Firefox: Quagga2 — the only engine proven on iPhone WebKit
  try {
    await ensureQuaggaLoaded();
    if (!('Quagga' in window)) {
      status.textContent = 'Scanner non supporte';
      showCameraHint('Quagga non charge — verifiez la connexion.');
      return;
    }
    video.style.display = 'none';
    reader.style.display = 'block';
    reader.innerHTML = '';
    await startQuaggaScanner(reader, status, button);
  } catch (error) {
    status.textContent = 'Camera bloquee';
    showCameraHint(error.message || 'Impossible d ouvrir la camera. Sur telephone, utilisez une adresse HTTPS.');
  }
}

async function startNativeScan(video, status, button, reader) {
  const supported = await BarcodeDetector.getSupportedFormats();
  const want = ['ean_13', 'ean_8', 'upc_a', 'upc_e', 'code_128', 'code_39', 'itf'];
  const formats = want.filter(f => supported.includes(f));
  const detector = new BarcodeDetector({formats: formats.length ? formats : ['ean_13', 'upc_a']});

  const stream = await navigator.mediaDevices.getUserMedia({
    video: {
      facingMode: 'environment',
      width: {ideal: 1920, min: 640},
      height: {ideal: 1080, min: 480},
      advanced: [{focusMode: 'continuous'}]
    }
  });
  scannerStream = stream;
  cameraTrack = stream.getVideoTracks()[0];
  video.srcObject = stream;
  video.style.display = 'block';
  if (reader) { reader.style.display = 'none'; reader.innerHTML = ''; }
  await video.play().catch(() => {});

  showCameraExtras();

  button.textContent = '■ Arreter camera';
  button.style.background = '#c8102e';
  button.style.color = 'white';
  button.style.borderColor = '#c8102e';
  if (cameraUsageMode === 'scan') {
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.style.display = ''; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  }
  status.textContent = 'Cadrez le code-barres';

  nativeScanActive = true;
  updateDeviceSupport();
  const loop = async () => {
    if (!nativeScanActive) return;
    if (!scanPaused && video.readyState >= 2) {
      try {
        const barcodes = await detector.detect(video);
        for (const bc of barcodes) {
          if (validateRetailBarcode(bc.rawValue)) {
            // Native hardware decode — trustworthy, accept on first read (instant)
            await onDecodedCode(bc.rawValue, true);
          }
        }
      } catch (_) {}
    }
    nativeScanFrame = requestAnimationFrame(loop);
  };
  nativeScanFrame = requestAnimationFrame(loop);
}

function showCameraExtras() {
  torchEnabled = false;
  const ids = cameraUsageMode === 'scan'
    ? ['torchButton', 'zoomSlider']
    : ['searchTorchButton', 'searchZoomSlider'];
  const caps = cameraTrack ? cameraTrack.getCapabilities() : {};

  const torchEl = document.getElementById(ids[0]);
  if (torchEl) {
    torchEl.style.display = caps.torch ? '' : 'none';
    torchEl.textContent = '💡';
    torchEl.style.background = '';
    torchEl.style.color = '';
  }
  const zoomEl = document.getElementById(ids[1]);
  const zoomLabelId = cameraUsageMode === 'scan' ? 'zoomValue' : 'searchZoomValue';
  const zoomLabel = document.getElementById(zoomLabelId);
  if (zoomEl && caps.zoom) {
    zoomEl.min = caps.zoom.min ?? 1;
    zoomEl.max = caps.zoom.max ?? 5;
    zoomEl.step = caps.zoom.step ?? 0.5;
    const savedZoom = parseFloat(localStorage.getItem('familiprixZoom') || '0');
    const clampedZoom = (savedZoom >= (caps.zoom.min ?? 1) && savedZoom <= (caps.zoom.max ?? 5))
      ? savedZoom : (caps.zoom.min ?? 1);
    zoomEl.value = clampedZoom;
    zoomEl.style.display = '';
    if (zoomLabel) { zoomLabel.textContent = _zoomLabel(clampedZoom); zoomLabel.style.display = ''; }
    if (clampedZoom > (caps.zoom.min ?? 1)) applyZoom(clampedZoom);
  } else if (zoomEl) {
    zoomEl.style.display = 'none';
    if (zoomLabel) zoomLabel.style.display = 'none';
  }
}

function _zoomLabel(val) {
  const n = parseFloat(val);
  return (n % 1 === 0 ? n : n.toFixed(1)) + '×';
}

function hideCameraExtras() {
  ['torchButton', 'zoomSlider', 'zoomValue', 'searchTorchButton', 'searchZoomSlider', 'searchZoomValue'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  torchEnabled = false;
}

async function toggleTorch() {
  if (!cameraTrack) return;
  torchEnabled = !torchEnabled;
  try {
    await cameraTrack.applyConstraints({advanced: [{torch: torchEnabled}]});
  } catch (_) { torchEnabled = !torchEnabled; }
  const ids = cameraUsageMode === 'scan' ? ['torchButton'] : ['searchTorchButton'];
  ids.forEach(id => {
    const el = document.getElementById(id);
    if (el) {
      el.style.background = torchEnabled ? '#f59e0b' : '';
      el.style.color = torchEnabled ? '#fff' : '';
    }
  });
}

function applyZoom(val) {
  const parsed = parseFloat(val);
  const labelId = cameraUsageMode === 'scan' ? 'zoomValue' : 'searchZoomValue';
  const sliderId = cameraUsageMode === 'scan' ? 'zoomSlider' : 'searchZoomSlider';
  const label = document.getElementById(labelId);
  const slider = document.getElementById(sliderId);
  if (label) { label.textContent = _zoomLabel(parsed); label.style.display = ''; }
  if (slider) slider.value = parsed;
  window.clearTimeout(zoomDebounceTimer);
  zoomDebounceTimer = window.setTimeout(async () => {
    if (!cameraTrack) return;
    try {
      await cameraTrack.applyConstraints({advanced: [{zoom: parsed}]});
      localStorage.setItem('familiprixZoom', parsed);
    } catch (_) {}
  }, 180);
}

// ── Library loaders ───────────────────────────────────────────────────────────
async function ensureQuaggaLoaded() {
  if ('Quagga' in window) return true;
  if (quaggaLibraryPromise) return quaggaLibraryPromise;
  const sources = [
    'https://cdn.jsdelivr.net/npm/@ericblade/quagga2/dist/quagga.min.js',
    'https://unpkg.com/@ericblade/quagga2/dist/quagga.min.js'
  ];
  quaggaLibraryPromise = (async () => {
    for (const src of sources) {
      const loaded = await injectScript(src);
      if (loaded && 'Quagga' in window) return true;
    }
    return false;
  })();
  return quaggaLibraryPromise;
}

async function ensureOcrLoaded() {
  if ('Tesseract' in window) return true;
  if (ocrLibraryPromise) return ocrLibraryPromise;
  const sources = [
    'https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js',
    'https://unpkg.com/tesseract.js@5/dist/tesseract.min.js'
  ];
  ocrLibraryPromise = (async () => {
    for (const src of sources) {
      const loaded = await injectScript(src);
      if (loaded && 'Tesseract' in window) return true;
    }
    return false;
  })();
  return ocrLibraryPromise;
}

async function ensureZXingLoaded() {
  if (window.ZXing) return true;
  if (zxingLibraryPromise) return zxingLibraryPromise;
  const sources = [
    'https://cdn.jsdelivr.net/npm/@zxing/library@0.21.3/umd/index.min.js',
    'https://unpkg.com/@zxing/library@0.21.3/umd/index.min.js'
  ];
  zxingLibraryPromise = (async () => {
    for (const src of sources) {
      const loaded = await injectScript(src);
      if (loaded && window.ZXing) return true;
    }
    return false;
  })();
  return zxingLibraryPromise;
}

// ── Smart two-stage scanner: localise barcode → read region + digit OCR ───────
// Stage 1: findBarcodeRegion() locates the high-gradient bar area in the frame.
// Stage 2a: ZXing decodes the bar pattern from that cropped region only.
// Stage 2b: OCR reads the human-readable digits printed directly below the bars.
// Both paths go through validateRetailBarcode (EAN checksum guard).
let _smartBarcodeRegion = null;  // last located region, shared with OCR loop

async function startSmartScan(video, status, button) {
  const stream = await navigator.mediaDevices.getUserMedia({
    video: {
      facingMode: 'environment',
      width:  {min: 640, ideal: 1280},
      height: {min: 480, ideal: 720},
      advanced: [{focusMode: 'continuous'}]
    }
  });
  scannerStream = stream;
  cameraTrack = stream.getVideoTracks()[0];
  video.srcObject = stream;
  await video.play().catch(() => {});
  showCameraExtras();

  button.textContent = '■ Arreter camera';
  button.style.background = '#c8102e'; button.style.color = 'white'; button.style.borderColor = '#c8102e';
  if (cameraUsageMode === 'scan') {
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.style.display = ''; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  }
  status.textContent = 'Cadrez le code-barres';
  zxingActive = true;
  quaggaStartedAt = Date.now();
  updateDeviceSupport();

  // Load ZXing for bar-pattern decoding
  const zxingLoaded = await ensureZXingLoaded().catch(() => false);
  let zxingReader = null;
  if (zxingLoaded && window.ZXing) {
    const hints = new Map();
    hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
      ZXing.BarcodeFormat.EAN_13, ZXing.BarcodeFormat.EAN_8,
      ZXing.BarcodeFormat.UPC_A,  ZXing.BarcodeFormat.UPC_E,
    ]);
    hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
    zxingReader = new ZXing.BrowserMultiFormatReader(hints);
  }

  const workCanvas = document.createElement('canvas');
  const workCtx   = workCanvas.getContext('2d', {willReadFrequently: true});
  const INTERVAL = 120;  // ~8 fps for localise+decode; OCR runs separately
  let lastScan = 0;

  const loop = (ts) => {
    if (!zxingActive) return;
    zxingFrame = requestAnimationFrame(loop);
    if (scanPaused || video.readyState < 2 || !video.videoWidth) return;
    if (ts - lastScan < INTERVAL) return;
    lastScan = ts;

    // Reduce frame to max 640px wide for fast gradient analysis
    const vw = video.videoWidth, vh = video.videoHeight;
    const sc = Math.min(1, 640 / vw);
    const sw = Math.max(1, Math.floor(vw * sc)), sh = Math.max(1, Math.floor(vh * sc));
    workCanvas.width = sw; workCanvas.height = sh;
    workCtx.filter = 'none';
    workCtx.drawImage(video, 0, 0, sw, sh);

    // Stage 1: find the barcode region (gradient-based)
    const region = findBarcodeRegion(workCanvas);
    _smartBarcodeRegion = region;  // share with OCR loop

    if (!region) return;

    // Stage 2a: crop region + high-contrast, try ZXing bar decode
    if (zxingReader) {
      const bc = cropCanvas(workCanvas, region.x, region.y, region.w, region.h, 'grayscale(1) contrast(2.5)');
      try {
        const r = zxingReader.decodeFromCanvas(bc);
        if (r && validateRetailBarcode(r.getText())) { onDecodedCode(r.getText()); return; }
      } catch (_) {}
    }

    // Stage 2b (synchronous quick check): also try native BarcodeDetector on the crop
    // when available (works on iOS 17.4+ as well as Android)
    if ('BarcodeDetector' in window && !ocrBusy) {
      const bc2 = cropCanvas(workCanvas, region.x, region.y, region.w, region.h, null);
      const det = new BarcodeDetector({formats: ['ean_13','ean_8','upc_a','upc_e']});
      det.detect(bc2).then(bcs => {
        for (const bc of bcs) {
          if (validateRetailBarcode(bc.rawValue)) { onDecodedCode(bc.rawValue); break; }
        }
      }).catch(() => {});
    }
  };
  zxingFrame = requestAnimationFrame(loop);

  // OCR loop: reads the printed digits below the barcode every 1000ms.
  // Even when the bar pattern can't be decoded (glare, angle), the large
  // printed numbers below are almost always readable by Tesseract.
  quaggaOcrTimer = window.setInterval(() => _smartOcrDigits(video, status), 1000);
}

async function _smartOcrDigits(video, status) {
  if (ocrBusy || scanPaused || !zxingActive || cameraUsageMode === 'search') return;
  if (Date.now() - quaggaStartedAt < 800) return;
  if (!video || !video.videoWidth) return;
  ocrBusy = true;
  try {
    if (!(await ensureOcrLoaded()) || !window.Tesseract) return;

    // Find where to crop for digits.
    // If gradient locator found a barcode region, the digits are right below it.
    // Otherwise fall back to the lower-center of the frame.
    let x, y, w, h;
    const reg = _smartBarcodeRegion;
    if (reg) {
      // Digits are immediately below the barcode bars, roughly 20% the bar height
      x = Math.max(0, reg.x - 0.02);
      y = Math.min(0.95, reg.y + reg.h);
      w = Math.min(1 - x, reg.w + 0.04);
      h = Math.min(1 - y, Math.max(0.06, reg.h * 0.25));
    } else {
      x = 0.05; y = 0.60; w = 0.90; h = 0.22;
    }

    const canvas = _ocrCrop(video, x, y, w, h, 3.5);
    if (!canvas) return;

    const result = await window.Tesseract.recognize(canvas, 'eng', {
      logger: () => {},
      tessedit_char_whitelist: '0123456789'
    });
    const raw = result?.data?.text || '';
    const candidates = extractBarcodeTextCandidates(raw);
    if (raw.replace(/\s/g,'').length > 3)
      console.log('[OCR-digits]', raw.trim().replace(/\n/g,' '), '→', candidates[0] || 'none');

    if (!candidates.length) { lastOcrCandidate = ''; return; }
    if (candidates[0] === lastOcrCandidate) {
      lastOcrCandidate = '';
      onDecodedCode(candidates[0]);
    } else {
      lastOcrCandidate = candidates[0];
    }
  } catch (_) { lastOcrCandidate = ''; }
  finally {
    ocrBusy = false;
    if (!scanPaused && zxingActive) status.textContent = 'Cadrez les barres et les chiffres';
  }
}

// ── ZXing live stream — primary scanner for iPhone/Firefox ───────────────────
// No halfSample, no artifacts. ZXing scans horizontal line-by-line across the
// frame — same algorithm used in Android's native barcode scanning.
// Also reads the human-readable digits printed under the barcode via OCR.
async function startZXingLiveScan(video, status, button) {
  const hints = new Map();
  hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
    ZXing.BarcodeFormat.EAN_13, ZXing.BarcodeFormat.EAN_8,
    ZXing.BarcodeFormat.UPC_A,  ZXing.BarcodeFormat.UPC_E,
  ]);
  hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
  const zxingReader = new ZXing.BrowserMultiFormatReader(hints);

  const stream = await navigator.mediaDevices.getUserMedia({
    video: {
      facingMode: 'environment',
      width:  {min: 640, ideal: 1280},
      height: {min: 480, ideal: 720},
      advanced: [{focusMode: 'continuous'}]
    }
  });
  scannerStream = stream;
  cameraTrack = stream.getVideoTracks()[0];
  video.srcObject = stream;
  await video.play().catch(() => {});
  showCameraExtras();

  button.textContent = '■ Arreter camera';
  button.style.background = '#c8102e'; button.style.color = 'white'; button.style.borderColor = '#c8102e';
  if (cameraUsageMode === 'scan') {
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.style.display = ''; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  }
  status.textContent = 'Cadrez les barres et les chiffres';
  zxingActive = true;
  quaggaStartedAt = Date.now();   // OCR fallback waits 2s from this
  updateDeviceSupport();

  const scanCanvas = document.createElement('canvas');
  const ctx = scanCanvas.getContext('2d', {willReadFrequently: true});
  const INTERVAL = 80;   // ~12 fps — fast enough for near-instant detection
  const MAX_W   = 800;   // cap width before decode; larger = more accurate, slower
  let lastScan  = 0;

  const loop = (ts) => {
    if (!zxingActive) return;
    zxingFrame = requestAnimationFrame(loop);
    if (scanPaused || video.readyState < 2 || !video.videoWidth) return;
    if (ts - lastScan < INTERVAL) return;
    lastScan = ts;

    // Crop to the scan area (same proportions as Quagga area: 15% margin)
    const vw = video.videoWidth, vh = video.videoHeight;
    const cx = Math.floor(vw * 0.05), cy = Math.floor(vh * 0.15);
    const cw = Math.floor(vw * 0.90), ch = Math.floor(vh * 0.70);
    const scale = Math.min(1, MAX_W / cw);
    const ow = Math.max(1, Math.floor(cw * scale));
    const oh = Math.max(1, Math.floor(ch * scale));
    scanCanvas.width = ow; scanCanvas.height = oh;
    ctx.filter = 'grayscale(1) contrast(1.6)';
    ctx.drawImage(video, cx, cy, cw, ch, 0, 0, ow, oh);
    try {
      const result = zxingReader.decodeFromCanvas(scanCanvas);
      if (result && validateRetailBarcode(result.getText())) {
        onDecodedCode(result.getText());
      }
    } catch (_) {}
  };
  zxingFrame = requestAnimationFrame(loop);

  // Parallel OCR path: reads the human-readable digits printed UNDER the barcode.
  // When ZXing can't decode the bar pattern (glare, angle, partial cover),
  // the printed digits are often still readable by Tesseract.
  quaggaOcrTimer = window.setInterval(() => {
    maybeRunOcrFallbackOnVideo(video, status);
  }, 1200);
}

// OCR directly on a video element (for ZXing live scan path — no Quagga reader element)
async function maybeRunOcrFallbackOnVideo(video, status) {
  if (ocrBusy || scanPaused || !zxingActive) return;
  if (Date.now() - quaggaStartedAt < 2000) return;
  if (cameraUsageMode === 'search') return;
  if (!video || !video.videoWidth) return;
  ocrBusy = true;
  try {
    if (!(await ensureOcrLoaded()) || !window.Tesseract) return;
    // Crop the digit strip: immediately below center of frame where digits print
    const canvas = _ocrCrop(video, 0.10, 0.64, 0.80, 0.18, 3.0)
               || _ocrCrop(video, 0.05, 0.58, 0.90, 0.28, 2.2);
    if (!canvas) return;
    const result = await window.Tesseract.recognize(canvas, 'eng', {
      logger: () => {},
      tessedit_char_whitelist: '0123456789'
    });
    const rawText = result?.data?.text || '';
    const candidates = extractBarcodeTextCandidates(rawText);
    if (rawText.replace(/\s/g, '').length > 3) {
      console.log('[OCR-digits]', rawText.trim().replace(/\n/g, ' '), '→', candidates[0] || 'none');
    }
    if (!candidates.length) { lastOcrCandidate = ''; return; }
    if (candidates[0] === lastOcrCandidate) {
      lastOcrCandidate = '';
      await onDecodedCode(candidates[0]);
    } else {
      lastOcrCandidate = candidates[0];
    }
  } catch (_) {
    lastOcrCandidate = '';
  } finally {
    ocrBusy = false;
    if (!scanPaused) status.textContent = 'Cadrez les barres et les chiffres';
  }
}

// ── ZXing streaming (kept for photo use only — not called from startCamera) ───
async function startZXingScan(video, status, button, reader) {
  const hints = new Map();
  hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
    ZXing.BarcodeFormat.EAN_13, ZXing.BarcodeFormat.EAN_8,
    ZXing.BarcodeFormat.UPC_A, ZXing.BarcodeFormat.UPC_E,
    ZXing.BarcodeFormat.CODE_128, ZXing.BarcodeFormat.CODE_39,
    ZXing.BarcodeFormat.ITF,
  ]);
  hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
  const zxingReader = new ZXing.BrowserMultiFormatReader(hints);

  const stream = await navigator.mediaDevices.getUserMedia({
    video: {
      facingMode: 'environment',
      width: {ideal: 1920, min: 640},
      height: {ideal: 1080, min: 480},
      advanced: [{focusMode: 'continuous'}]
    }
  });
  scannerStream = stream;
  cameraTrack = stream.getVideoTracks()[0];
  video.srcObject = stream;
  video.style.display = 'block';
  if (reader) { reader.style.display = 'none'; reader.innerHTML = ''; }
  await video.play().catch(() => {});
  showCameraExtras();

  button.textContent = '■ Arreter camera';
  button.style.background = '#c8102e';
  button.style.color = 'white';
  button.style.borderColor = '#c8102e';
  if (cameraUsageMode === 'scan') {
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.style.display = ''; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  }
  status.textContent = 'Cadrez le code-barres';

  const scanCanvas = document.createElement('canvas');
  const ctx = scanCanvas.getContext('2d', {willReadFrequently: true});
  const ZXING_INTERVAL = 120;
  const MAX_SCAN_DIM = 640;
  let lastZxingScan = 0;
  zxingActive = true;
  updateDeviceSupport();

  const loop = (ts) => {
    if (!zxingActive) return;
    zxingFrame = requestAnimationFrame(loop);
    if (scanPaused || video.readyState < 2 || !video.videoWidth) return;
    if (ts - lastZxingScan < ZXING_INTERVAL) return;
    lastZxingScan = ts;

    const vw = video.videoWidth;
    const vh = video.videoHeight;
    const cropX = Math.floor(vw * 0.05);
    const cropY = Math.floor(vh * 0.20);
    const cropW = Math.floor(vw * 0.90);
    const cropH = Math.floor(vh * 0.60);
    const scale = Math.min(1, MAX_SCAN_DIM / cropW);
    const outW = Math.max(1, Math.floor(cropW * scale));
    const outH = Math.max(1, Math.floor(cropH * scale));
    scanCanvas.width = outW;
    scanCanvas.height = outH;
    ctx.filter = 'grayscale(1) contrast(1.8)';
    ctx.drawImage(video, cropX, cropY, cropW, cropH, 0, 0, outW, outH);
    try {
      const result = zxingReader.decodeFromCanvas(scanCanvas);
      if (result && validateRetailBarcode(result.getText())) {
        onDecodedCode(result.getText());
      }
    } catch (_) {}
  };
  zxingFrame = requestAnimationFrame(loop);
}

// ── Barcode region localization (gradient-based) ──────────────────────────────
function findBarcodeRegion(canvas) {
  const MAX_W = 400;
  const scale = Math.min(1, MAX_W / canvas.width);
  const sw = Math.max(2, Math.floor(canvas.width  * scale));
  const sh = Math.max(2, Math.floor(canvas.height * scale));

  const tmp = document.createElement('canvas');
  tmp.width = sw; tmp.height = sh;
  const ctx = tmp.getContext('2d', {willReadFrequently: true});
  ctx.drawImage(canvas, 0, 0, sw, sh);
  const data = ctx.getImageData(0, 0, sw, sh).data;

  const gray = new Uint8Array(sw * sh);
  for (let i = 0; i < sw * sh; i++) {
    gray[i] = (data[i*4]*77 + data[i*4+1]*150 + data[i*4+2]*29) >> 8;
  }

  const colScore = new Float32Array(sw);
  for (let x = 1; x < sw - 1; x++) {
    let s = 0;
    for (let y = 0; y < sh; y++) {
      s += Math.abs(gray[y*sw + x+1] - gray[y*sw + x-1]);
    }
    colScore[x] = s / sh;
  }

  const blurW = Math.max(1, Math.floor(sw * 0.02));
  const smooth = new Float32Array(sw);
  for (let x = 0; x < sw; x++) {
    let s = 0, n = 0;
    for (let dx = -blurW; dx <= blurW; dx++) {
      const nx = x + dx;
      if (nx >= 0 && nx < sw) { s += colScore[nx]; n++; }
    }
    smooth[x] = s / n;
  }

  let mean = 0;
  for (let x = 0; x < sw; x++) mean += smooth[x];
  mean /= sw;
  let variance = 0;
  for (let x = 0; x < sw; x++) variance += (smooth[x] - mean) ** 2;
  const threshold = mean + 0.5 * Math.sqrt(variance / sw);

  let bestStart = -1, bestLen = 0, curStart = -1, curLen = 0;
  for (let x = 0; x < sw; x++) {
    if (smooth[x] > threshold) {
      if (curStart < 0) curStart = x;
      curLen++;
    } else {
      if (curLen > bestLen) { bestLen = curLen; bestStart = curStart; }
      curStart = -1; curLen = 0;
    }
  }
  if (curLen > bestLen) { bestLen = curLen; bestStart = curStart; }

  if (bestStart < 0 || bestLen < sw * 0.04) return null;

  let minY = sh, maxY = 0;
  for (let y = 0; y < sh; y++) {
    let rowGrad = 0;
    for (let x = bestStart; x < Math.min(bestStart + bestLen, sw - 1); x++) {
      rowGrad += Math.abs(gray[y*sw + x+1] - gray[y*sw + x-1]);
    }
    if (rowGrad / bestLen > threshold * 0.4) {
      minY = Math.min(minY, y);
      maxY = Math.max(maxY, y);
    }
  }
  if (maxY <= minY) return null;

  const padX = 0.06, padY = 0.05, extraBottom = 0.12;
  return {
    x: Math.max(0, bestStart / sw - padX),
    y: Math.max(0, minY / sh - padY),
    w: Math.min(1 - Math.max(0, bestStart / sw - padX), bestLen / sw + padX * 2),
    h: Math.min(1, (maxY - minY) / sh + padY + extraBottom),
  };
}

// ── Photo scan pipeline ───────────────────────────────────────────────────────
async function photoToCanvas(file) {
  const img = new Image();
  const url = URL.createObjectURL(file);
  await new Promise((resolve, reject) => { img.onload = resolve; img.onerror = reject; img.src = url; });
  URL.revokeObjectURL(url);
  const maxDim = 1920;
  const scale = Math.min(1, maxDim / Math.max(img.width || 1, img.height || 1));
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.floor(img.width * scale));
  canvas.height = Math.max(1, Math.floor(img.height * scale));
  canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
  return canvas;
}

function cropCanvas(src, x, y, w, h, filter) {
  const out = document.createElement('canvas');
  const sx = Math.floor(src.width * x);
  const sy = Math.floor(src.height * y);
  out.width  = Math.max(1, Math.floor(src.width * w));
  out.height = Math.max(1, Math.floor(src.height * h));
  const ctx = out.getContext('2d', {willReadFrequently: true});
  if (filter) ctx.filter = filter;
  ctx.drawImage(src, sx, sy, Math.floor(src.width * w), Math.floor(src.height * h), 0, 0, out.width, out.height);
  return out;
}

async function ocrBarcodeFromCanvas(canvas) {
  if (!(await ensureOcrLoaded()) || !window.Tesseract) return null;
  const crops = [
    cropCanvas(canvas, 0,    0.60, 1, 0.40, 'grayscale(1) contrast(3) brightness(1.15)'),
    cropCanvas(canvas, 0.05, 0.50, 0.90, 0.50, 'grayscale(1) contrast(2.5) brightness(1.1)'),
    cropCanvas(canvas, 0,    0,    1, 1,    'grayscale(1) contrast(2)'),
  ];
  for (const crop of crops) {
    try {
      const { data } = await window.Tesseract.recognize(crop, 'eng', {
        logger: () => {},
        tessedit_char_whitelist: '0123456789 ',
        tessedit_pageseg_mode: '6',
      });
      const candidates = extractBarcodeTextCandidates(data?.text || '');
      if (candidates.length) return candidates[0];
    } catch (_) {}
  }
  return null;
}

async function scanFromPhoto(input, mode) {
  const file = input.files[0];
  input.value = '';
  if (!file) return;

  const isSearch = mode === 'search';
  const statusEl = document.getElementById(isSearch ? 'searchScannerStatus' : 'scannerStatus');
  const setStatus = t => { if (statusEl) statusEl.textContent = t; };

  setStatus('Lecture de la photo...');

  let canvas;
  try { canvas = await photoToCanvas(file); }
  catch (_) { setStatus('Erreur lecture photo'); return; }

  let code = null;

  if (!code && 'BarcodeDetector' in window) {
    setStatus('Detection native...');
    try {
      const det = new BarcodeDetector({formats: ['ean_13','ean_8','upc_a','upc_e','code_128','code_39','itf']});
      for (const bc of await det.detect(canvas)) {
        if (validateRetailBarcode(bc.rawValue)) { code = bc.rawValue; break; }
      }
    } catch (_) {}
  }

  setStatus('Localisation du code-barres...');
  const region = findBarcodeRegion(canvas);
  const regionCanvas = region ? cropCanvas(canvas, region.x, region.y, region.w, region.h, null) : null;

  if (!code && regionCanvas && 'BarcodeDetector' in window) {
    try {
      const det = new BarcodeDetector({formats: ['ean_13','ean_8','upc_a','upc_e','code_128','code_39','itf']});
      for (const bc of await det.detect(regionCanvas)) {
        if (validateRetailBarcode(bc.rawValue)) { code = bc.rawValue; break; }
      }
    } catch (_) {}
  }

  if (!code) {
    setStatus('Lecture code-barres...');
    const loaded = await ensureZXingLoaded();
    if (loaded) {
      const hints = new Map();
      hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
        ZXing.BarcodeFormat.EAN_13, ZXing.BarcodeFormat.EAN_8,
        ZXing.BarcodeFormat.UPC_A,  ZXing.BarcodeFormat.UPC_E,
        ZXing.BarcodeFormat.CODE_128, ZXing.BarcodeFormat.CODE_39, ZXing.BarcodeFormat.ITF,
      ]);
      hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
      const reader = new ZXing.BrowserMultiFormatReader(hints);
      const attempts = [
        canvas,
        regionCanvas,
        cropCanvas(canvas, 0,   0,   1,   0.5, null),
        cropCanvas(canvas, 0,   0.5, 1,   0.5, null),
        cropCanvas(canvas, 0.1, 0.2, 0.8, 0.6, null),
      ].filter(Boolean);
      for (const attempt of attempts) {
        if (code) break;
        try {
          const r = reader.decodeFromCanvas(attempt);
          if (r && validateRetailBarcode(r.getText())) code = r.getText();
        } catch (_) {}
      }
    }
  }

  if (!code) {
    setStatus('Lecture des chiffres...');
    code = regionCanvas
      ? await ocrBarcodeFromCanvas(regionCanvas)
      : await ocrBarcodeFromCanvas(canvas);
  }

  if (!code) {
    setStatus('Code non reconnu — reessayez plus pres avec bonne lumiere');
    return;
  }

  playBeep();
  vibratePhone();
  if (isSearch) {
    document.getElementById('searchInput').value = code;
    setStatus(`✓ ${code}`);
    doSearch();
  } else {
    scanPaused = false;
    document.getElementById('scanInput').value = code;
    persistScanDraft();
    setStatus(`✓ ${code}`);
    lookupScanFromInput(true, code);
  }
}

// ── Script loader ─────────────────────────────────────────────────────────────
function injectScript(src) {
  return new Promise(resolve => {
    const existing = document.querySelector(`script[data-src="${src}"]`);
    if (existing) {
      if (existing.dataset.loaded === '1') { resolve(true); return; }
      existing.addEventListener('load', () => resolve(true), {once: true});
      existing.addEventListener('error', () => resolve(false), {once: true});
      return;
    }
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.dataset.src = src;
    script.onload = () => { script.dataset.loaded = '1'; resolve(true); };
    script.onerror = () => resolve(false);
    document.head.appendChild(script);
  });
}

// ── Canvas helpers ────────────────────────────────────────────────────────────
function makeScanCanvas(width, height) {
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(32, Math.floor(width));
  canvas.height = Math.max(32, Math.floor(height));
  return canvas;
}

function drawProcessedCrop(video, crop) {
  const sourceWidth = video.videoWidth;
  const sourceHeight = video.videoHeight;
  if (!sourceWidth || !sourceHeight) return null;
  const sx = Math.max(0, Math.floor(sourceWidth * crop.x));
  const sy = Math.max(0, Math.floor(sourceHeight * crop.y));
  const sw = Math.max(64, Math.floor(sourceWidth * crop.w));
  const sh = Math.max(32, Math.floor(sourceHeight * crop.h));
  const canvas = makeScanCanvas(sw * crop.scale, sh * crop.scale);
  const ctx = canvas.getContext('2d', {willReadFrequently: true});
  if (!ctx) return null;
  ctx.filter = 'grayscale(1) contrast(2.2) brightness(1.18) saturate(0) sharpen(1)';
  ctx.drawImage(video, sx, sy, sw, sh, 0, 0, canvas.width, canvas.height);
  return canvas;
}

function getQuaggaVideoElement(reader) {
  return reader.querySelector('video');
}

// ── Barcode validation ────────────────────────────────────────────────────────
function validateRetailBarcode(code) {
  const digits = String(code || '').replace(/\D/g, '');
  if (/^\d{8}$/.test(digits))  return checkEanChecksum(digits, 8);
  if (/^\d{12}$/.test(digits)) return checkEanChecksum(digits, 12);
  if (/^\d{13}$/.test(digits)) return checkEanChecksum(digits, 13);
  if (/^\d{14}$/.test(digits)) return checkEanChecksum(digits, 14);
  return false;
}

function checkEanChecksum(code, length) {
  if (code.length !== length) return false;
  const digits = code.split('').map(Number);
  const checkDigit = digits.pop();
  const reversed = digits.reverse();
  const sum = reversed.reduce((total, digit, index) => {
    const weight = index % 2 === 0 ? 3 : 1;
    return total + digit * weight;
  }, 0);
  return (10 - (sum % 10)) % 10 === checkDigit;
}

function extractBarcodeTextCandidates(text) {
  const rawDigits = String(text || '').replace(/\D/g, ' ');
  const chunks = rawDigits.split(/\s+/).filter(Boolean);
  const candidates = new Set();
  for (const chunk of chunks) {
    for (const size of [14, 13, 12, 8]) {
      if (chunk.length === size) candidates.add(chunk);
      if (chunk.length > size) {
        for (let start = 0; start <= chunk.length - size; start += 1) {
          candidates.add(chunk.slice(start, start + size));
        }
      }
    }
  }
  const allDigits = chunks.join('');
  if (allDigits.length >= 8) {
    for (const size of [13, 12, 8]) {
      if (allDigits.length === size) {
        candidates.add(allDigits);
      } else if (allDigits.length > size) {
        for (let start = allDigits.length - size; start >= 0; start -= 1) {
          candidates.add(allDigits.slice(start, start + size));
        }
      }
    }
  }
  return Array.from(candidates).filter(validateRetailBarcode);
}

// ── OCR from video stream ─────────────────────────────────────────────────────

// Crop + binarize a region of the video for OCR.
// Binarization (black/white threshold) greatly stabilises digit recognition
// frame-to-frame, reducing the noise that caused the "never matches twice" problem.
function _ocrCrop(video, x, y, w, h, scale) {
  const vw = video.videoWidth;
  const vh = video.videoHeight;
  if (!vw || !vh) return null;
  const sx = Math.floor(vw * x);
  const sy = Math.floor(vh * y);
  const sw = Math.max(1, Math.floor(vw * w));
  const sh = Math.max(1, Math.floor(vh * h));
  const out = document.createElement('canvas');
  out.width  = Math.max(1, Math.floor(sw * scale));
  out.height = Math.max(1, Math.floor(sh * scale));
  const ctx = out.getContext('2d', {willReadFrequently: true});
  // High-contrast grayscale
  ctx.filter = 'grayscale(1) contrast(2.4) brightness(1.1)';
  ctx.drawImage(video, sx, sy, sw, sh, 0, 0, out.width, out.height);
  // Binarization: threshold at 128 for clean black/white output
  const img = ctx.getImageData(0, 0, out.width, out.height);
  const d = img.data;
  for (let i = 0; i < d.length; i += 4) {
    const v = d[i] > 128 ? 255 : 0;
    d[i] = d[i+1] = d[i+2] = v;
  }
  ctx.putImageData(img, 0, 0);
  return out;
}

// Strict OCR candidate extractor — NO substring guessing.
// Only accepts a complete digit run (or the fully-joined digits) of an exact
// valid barcode length that passes checksum. This is what stops the random reads:
// the printed barcode number is a clean 12/13-digit run; prices/lot numbers are
// the wrong length and get rejected, never sliced into fake barcodes.
function ocrBarcodeCandidate(text) {
  const raw = String(text || '');
  const tryStr = s => {
    const d = String(s).replace(/[^0-9]/g, '');
    return ([8, 12, 13, 14].includes(d.length) && validateRetailBarcode(d)) ? d : null;
  };
  // KEY: check each LINE separately. The printed barcode digits are their own
  // line (e.g. "3 6 1 6 3 0 3 2 4 2 2 8 2"); the bar pattern above produces a
  // separate garbage line. Per-line parsing isolates the real digit row.
  for (const line of raw.split(/[\r\n]+/)) {
    const hit = tryStr(line);            // digits in this line, joined
    if (hit) return hit;
  }
  // Then each whitespace-separated run on any line
  for (const run of raw.replace(/[^0-9\s]/g, ' ').split(/\s+/)) {
    if (tryStr(run)) return run.replace(/[^0-9]/g, '');
  }
  // Last: whole text joined (handles digits split oddly across lines)
  return tryStr(raw);
}

async function recognizeBarcodeDigitsFromVideo(reader) {
  const video = getQuaggaVideoElement(reader);
  if (!video || !video.videoWidth || !video.videoHeight) return null;
  if (!(await ensureOcrLoaded()) || !window.Tesseract) return null;

  // TIGHT center crop only — where the user deliberately aims the barcode.
  // Center 70% width × center 40% height. Keeps prices/shelf text out of frame.
  const canvas = _ocrCrop(video, 0.15, 0.32, 0.70, 0.40, 3.0);
  if (!canvas) return null;

  const result = await window.Tesseract.recognize(canvas, 'eng', {
    logger: () => {},
    tessedit_char_whitelist: '0123456789'
  });
  const rawText = result?.data?.text || '';
  const candidate = ocrBarcodeCandidate(rawText);
  if (rawText.replace(/\s/g, '').length > 3) {
    console.log('[OCR]', rawText.trim().replace(/\n/g, ' '), '→', candidate || 'none');
  }
  return candidate;
}

async function maybeRunOcrFallback(reader, status) {
  if (ocrBusy || scanPaused || !quaggaActive) return;
  if (cameraUsageMode === 'search') return;

  // Make the OCR engine state visible (diagnostic panel).
  if (!('Tesseract' in window)) {
    if (ocrLoadState !== 'loading' && ocrLoadState !== 'failed') {
      ocrLoadState = 'loading';
      _dbg.t = '⏳'; _renderDbg();
      ensureOcrLoaded().then(ok => { ocrLoadState = ok ? 'ready' : 'failed'; _dbg.t = ok ? '✓' : '✗ pas chargé'; _renderDbg(); })
                       .catch(() => { ocrLoadState = 'failed'; _dbg.t = '✗ pas chargé'; _renderDbg(); });
    }
    return;
  }
  _dbg.t = '✓';

  ocrBusy = true;
  try {
    const video = getQuaggaVideoElement(reader);
    if (!video || !video.videoWidth) return;
    // Tall+wide crop covering the whole scan area. Per-line parsing isolates the
    // 13-digit barcode row; prices/words/short numbers fail validation and are
    // ignored. y 0.10-0.85 guarantees the digit row is captured wherever framed.
    const canvas = _ocrCrop(video, 0.06, 0.10, 0.88, 0.75, 3.0);
    if (!canvas) return;
    const result = await window.Tesseract.recognize(canvas, 'eng', {
      logger: () => {},
      tessedit_char_whitelist: '0123456789',
      preserve_interword_spaces: '1'
    });
    const rawText = result?.data?.text || '';
    const candidate = ocrBarcodeCandidate(rawText);

    // Show what OCR is reading, live (diagnostic panel).
    const seen = rawText.replace(/[^0-9]/g, '');
    _dbg.ocr = (seen ? seen.slice(0, 16) : '(rien)') + (candidate ? ' ✓' : '');
    _renderDbg();

    if (candidate) {
      ocrHistory.push(candidate);
      if (ocrHistory.length > 5) ocrHistory.shift();
      // Accept when the SAME valid number appears at least twice in the recent
      // window — tolerates one-digit OCR flicker between frames while still
      // requiring agreement (a random misread won't repeat the same value).
      const count = ocrHistory.filter(c => c === candidate).length;
      if (count >= 2) {
        ocrHistory = [];
        await onDecodedCode(candidate, true);
      }
    }
  } catch (error) {
    /* keep scanning */
  } finally {
    ocrBusy = false;
  }
}

// Average per-bar decode error from a Quagga result (0 = perfect, 1 = garbage).
// This is Quagga's own confidence metric — the professional way to reject false
// reads instantly without needing OCR cross-checks or multiple frames.
function quaggaDecodeError(result) {
  const codes = result && result.codeResult && result.codeResult.decodedCodes;
  if (!Array.isArray(codes)) return 1;
  const errs = codes.filter(c => c && typeof c.error === 'number').map(c => c.error);
  if (!errs.length) return 1;
  return errs.reduce((a, b) => a + b, 0) / errs.length;
}

// ── Quagga scanner ────────────────────────────────────────────────────────────
async function startQuaggaScanner(reader, status, button) {
  reader.style.height = '100%';
  reader.style.minHeight = '0';
  const config = {
    inputStream: {
      type: 'LiveStream',
      target: reader,
      constraints: {
        facingMode: 'environment',
        // High capture resolution so small/dense etiquette barcodes keep enough
        // detail even after halfSample (1920 → 960px processing = sharp thin bars).
        width:  {min: 640, ideal: 1920},
        height: {min: 480, ideal: 1080},
        advanced: [{focusMode: 'continuous'}]
      },
      area: {top: '15%', right: '5%', left: '5%', bottom: '15%'}
    },
    // patchSize 'small' locates small/dense barcodes (etiquettes) far better than
    // 'medium'. The decode-error gate now rejects any false reads this enables.
    locator: {patchSize: 'small', halfSample: true},
    numOfWorkers: 2,
    frequency: 20,
    locate: true,
    decoder: {
      // Only EAN/UPC — all have checksums; Code39/ITF removed (no checksum → false fires)
      // Longest formats first — Quagga tries each reader in order and stops on first match.
      // EAN-13 before EAN-8 means a 13-digit barcode is never mis-decoded as 8 digits.
      readers: ['ean_reader', 'upc_reader', 'ean_8_reader', 'upc_e_reader'],
      multiple: false
    }
  };

  await new Promise((resolve, reject) => {
    window.Quagga.init(config, err => err ? reject(err) : resolve());
  });

  const quaggaVideo = reader.querySelector('video');
  if (quaggaVideo && quaggaVideo.srcObject) {
    const tracks = quaggaVideo.srcObject.getVideoTracks();
    if (tracks.length) {
      cameraTrack = tracks[0];
      showCameraExtras();
    }
  }

  quaggaDetectedHandler = result => {
    const code = String(result?.codeResult?.code || '').trim();
    const err = quaggaDecodeError(result);
    // Diagnostic: show EVERY raw read and its error, even ones we reject.
    _dbg.qRead = code || '(rien)';
    _dbg.qErr = err.toFixed(3);
    _renderDbg();
    if (!code || !validateRetailBarcode(code)) return;
    if (err > 0.30) return;            // only reject very noisy reads
    onDecodedCode(code, err < 0.10);   // clean → instant; marginal → 2-frame confirm
  };
  quaggaProcessedHandler = () => {
    // Don't overwrite the live OCR readout (🔢 / ⏳ / ⚠ / 🔍)
    if (scanPaused) return;
    const t = status.textContent || '';
    if (/^[🔢⏳⚠🔍✓]/.test(t)) return;
    status.textContent = 'Cadrez les barres et les chiffres';
  };

  window.Quagga.onDetected(quaggaDetectedHandler);
  window.Quagga.onProcessed(quaggaProcessedHandler);
  window.Quagga.start();
  quaggaActive = true;
  quaggaStartedAt = Date.now();
  _dbg.q = '✓'; _renderDbg();
  updateDeviceSupport();
  button.textContent = '■ Arreter camera';
  button.style.background = '#c8102e';
  button.style.color = 'white';
  button.style.borderColor = '#c8102e';
  if (cameraUsageMode === 'scan') {
    const pb = document.getElementById('pauseScanButton');
    if (pb) { pb.style.display = ''; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  }
  status.textContent = 'Cadrez les barres et les chiffres';
  // OCR fallback for small/glossy etiquette barcodes Quagga can't decode.
  // Reads the printed digits in a TIGHT center crop, accepts only a complete
  // exact-length checksummed number seen on 2 consecutive passes. Runs in
  // parallel with Quagga — whichever gets a valid read first wins.
  quaggaOcrTimer = window.setInterval(() => {
    maybeRunOcrFallback(reader, status);
  }, 600);

  // ZXing parallel decoder — reads SMALL/DENSE etiquette barcodes that Quagga
  // misses. ZXing scans at FULL resolution (no halfSample) so thin bars survive.
  // Gives the exact barcode (not an OCR guess). Runs every 180ms on a high-res
  // center crop of Quagga's own video element.
  startZXingParallel(reader);
}

async function startZXingParallel(reader) {
  _dbg.z = '⏳'; _renderDbg();
  const ok = await ensureZXingLoaded().catch(() => false);
  if (!ok || !window.ZXing) { _dbg.z = '✗ pas chargé'; _renderDbg(); return; }

  // Correct low-level API: MultiFormatReader.decode(BinaryBitmap).
  // (decodeFromCanvas does not exist in this build → was throwing TypeError.)
  let mfReader, hints;
  try {
    hints = new Map();
    hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
      ZXing.BarcodeFormat.EAN_13, ZXing.BarcodeFormat.EAN_8,
      ZXing.BarcodeFormat.UPC_A,  ZXing.BarcodeFormat.UPC_E,
    ]);
    hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
    mfReader = new ZXing.MultiFormatReader();
    mfReader.setHints(hints);
    _dbg.z = '✓'; _renderDbg();
  } catch (e) {
    _dbg.z = '✗ init:' + (e && e.name); _renderDbg();
    return;
  }

  const cv = document.createElement('canvas');
  const cx = cv.getContext('2d', {willReadFrequently: true});

  scanFrameTimer = window.setInterval(() => {
    if (scanPaused || !quaggaActive) return;
    const video = reader.querySelector('video');
    if (!video || !video.videoWidth) return;
    const vw = video.videoWidth, vh = video.videoHeight;
    const sx = Math.floor(vw * 0.02), sy = Math.floor(vh * 0.05);
    const sw = Math.floor(vw * 0.96), sh = Math.floor(vh * 0.90);
    cv.width = sw; cv.height = sh;
    cx.drawImage(video, sx, sy, sw, sh, 0, 0, sw, sh);
    try {
      const source = new ZXing.HTMLCanvasElementLuminanceSource(cv);
      const bitmap = new ZXing.BinaryBitmap(new ZXing.HybridBinarizer(source));
      const r = mfReader.decode(bitmap);
      if (r) {
        const txt = r.getText();
        _dbg.zRead = txt + (validateRetailBarcode(txt) ? ' ✓' : ' (cksum?)');
        _renderDbg();
        if (validateRetailBarcode(txt)) onDecodedCode(txt, true);
      }
    } catch (e) {
      const name = (e && e.name) ? e.name : 'err';
      // NotFoundException is normal (no barcode this frame); show any other error.
      if (name.indexOf('NotFound') < 0) { _dbg.zRead = '(' + name + ')'; _renderDbg(); }
    } finally {
      try { mfReader.reset(); } catch (_) {}   // required between decodes
    }
  }, 200);
}

// ── Stop camera ───────────────────────────────────────────────────────────────
async function stopCamera() {
  const {status, button, video, reader} = getCameraDom();
  scanPaused = false;
  lastOcrCandidate = '';
  ocrHistory = [];
  _removeDbg();
  window.clearInterval(scanFrameTimer);   // ZXing parallel decoder interval
  scanFrameTimer = null;
  window.clearInterval(quaggaOcrTimer);
  quaggaOcrTimer = null;
  resetCameraCandidate();
  if (nativeScanActive) {
    nativeScanActive = false;
    if (nativeScanFrame !== null) { cancelAnimationFrame(nativeScanFrame); nativeScanFrame = null; }
  }
  if (zxingActive) {
    zxingActive = false;
    if (zxingFrame !== null) { cancelAnimationFrame(zxingFrame); zxingFrame = null; }
  }
  hideCameraExtras();
  if (quaggaActive && 'Quagga' in window) {
    try {
      if (quaggaDetectedHandler) window.Quagga.offDetected(quaggaDetectedHandler);
      if (quaggaProcessedHandler) window.Quagga.offProcessed(quaggaProcessedHandler);
      window.Quagga.stop();
    } catch (error) {}
  }
  quaggaActive = false;
  quaggaDetectedHandler = null;
  quaggaProcessedHandler = null;
  if (scannerStream) {
    scannerStream.getTracks().forEach(track => track.stop());
  }
  if (html5Scanner) {
    try { await html5Scanner.stop(); await html5Scanner.clear(); } catch (error) {}
  }
  scannerStream = null;
  cameraTrack = null;
  html5Scanner = null;
  video.srcObject = null;
  video.style.display = 'block';
  reader.innerHTML = '';
  reader.style.display = 'none';
  status.textContent = 'Camera arretee';
  button.textContent = 'Ouvrir camera';
  button.style.background = '';
  button.style.color = '';
  button.style.borderColor = '';
  const pb = document.getElementById('pauseScanButton');
  if (pb) { pb.style.display = 'none'; pb.textContent = '⏸ Pause'; pb.style.background = ''; pb.style.color = ''; pb.style.borderColor = ''; }
  if (cameraUsageMode === 'search') {
    document.getElementById('cameraPreview').srcObject = null;
    document.getElementById('cameraPreview').style.display = 'block';
    document.getElementById('html5Reader').style.display = 'none';
    document.getElementById('scannerStatus').textContent = 'Camera arretee';
    const cb = document.getElementById('cameraButton');
    cb.textContent = 'Ouvrir camera';
    cb.style.background = ''; cb.style.color = ''; cb.style.borderColor = '';
  } else {
    document.getElementById('searchCameraPreview').srcObject = null;
    document.getElementById('searchCameraPreview').style.display = 'block';
    document.getElementById('searchHtml5Reader').style.display = 'none';
    document.getElementById('searchScannerStatus').textContent = 'Camera arretee';
    const scb = document.getElementById('searchCameraButton');
    scb.textContent = 'Ouvrir camera';
    scb.style.background = ''; scb.style.color = ''; scb.style.borderColor = '';
  }
  cameraUsageMode = 'scan';
}

function responsiveScanBox(viewfinderWidth, viewfinderHeight) {
  const minEdge = Math.min(viewfinderWidth, viewfinderHeight);
  return {
    width: Math.max(280, Math.floor(viewfinderWidth * 0.92)),
    height: Math.max(82, Math.floor(minEdge * 0.16))
  };
}

function html5ScannerConfig() {
  if (!('Html5QrcodeSupportedFormats' in window)) return {};
  return {
    formatsToSupport: [
      Html5QrcodeSupportedFormats.EAN_13, Html5QrcodeSupportedFormats.EAN_8,
      Html5QrcodeSupportedFormats.UPC_A,  Html5QrcodeSupportedFormats.UPC_E,
      Html5QrcodeSupportedFormats.CODE_128, Html5QrcodeSupportedFormats.CODE_39,
      Html5QrcodeSupportedFormats.ITF
    ].filter(Boolean)
  };
}

// ── Audio / haptic feedback ───────────────────────────────────────────────────
function playBeep() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 1400;
    osc.type = 'sine';
    gain.gain.setValueAtTime(0.35, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.13);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.13);
  } catch (e) {}
}

function vibratePhone() {
  try { navigator.vibrate && navigator.vibrate(80); } catch (e) {}
}

function flashScannerSuccess() {
  const {reader} = getCameraDom();
  reader.style.outline = '5px solid #16a34a';
  reader.style.outlineOffset = '-5px';
  window.setTimeout(() => {
    reader.style.outline = '';
    reader.style.outlineOffset = '';
  }, 500);
}

// ── Decode callback ───────────────────────────────────────────────────────────
// Quagga returns UPC-A barcodes in 13-digit EAN-13 form (leading 0 added).
// Strip it so we store/show the real 12-digit UPC-A printed on the product.
function normalizeScannedBarcode(code) {
  const digits = String(code || '').replace(/\D/g, '');
  if (digits.length === 13 && digits.startsWith('0')) return digits.slice(1);
  return digits;
}

function onDecodedCode(decodedText, instant=false) {
  const rawValue = normalizeScannedBarcode(decodedText);
  if (!rawValue || scanPaused) return;
  if (!confirmStableCameraBarcode(rawValue, instant)) return;
  lastOcrCandidate = '';
  scanPaused = true;
  playBeep();
  vibratePhone();
  flashScannerSuccess();
  setScannedBarcode(rawValue);
  getCameraDom().status.textContent = '✓ ' + rawValue;

  if (cameraUsageMode === 'search') {
    doSearchValue(rawValue);
  } else {
    lookupScanFromInput(true, rawValue);
  }

  window.setTimeout(() => {
    scanPaused = false;
    if (scannerStream || html5Scanner) {
      getCameraDom().status.textContent = 'Pret a scanner...';
    }
  }, 1200);
}

function confirmStableCameraBarcode(barcode, instant=false) {
  const normalized = barcode.trim();
  if (!looksLikeBarcode(normalized)) return false;
  const now = Date.now();

  // Debounce: same code won't fire again for 1500ms
  if (normalized === lastAcceptedCameraBarcode && now - lastAcceptedCameraAt < 1500) {
    return false;
  }

  // Instant accept for high-confidence reads:
  //  - native hardware BarcodeDetector (never hallucinates), or
  //  - Quagga reads with very low decode error (< 0.08)
  // These are trustworthy on the first frame → truly instantaneous.
  if (instant) {
    lastAcceptedCameraBarcode = normalized;
    lastAcceptedCameraAt = now;
    resetCameraCandidate();
    return true;
  }

  // Marginal reads: require the SAME code on 2 consecutive frames.
  // A real barcode is stable frame-to-frame; an artifact differs every frame.
  if (normalized === cameraCandidateBarcode) {
    cameraCandidateCount += 1;
  } else {
    cameraCandidateBarcode = normalized;
    cameraCandidateCount = 1;
  }
  window.clearTimeout(cameraCandidateTimer);
  cameraCandidateTimer = window.setTimeout(resetCameraCandidate, 300);
  if (cameraCandidateCount < 2) return false;
  lastAcceptedCameraBarcode = normalized;
  lastAcceptedCameraAt = now;
  resetCameraCandidate();
  return true;
}

function resetCameraCandidate() {
  cameraCandidateBarcode = '';
  cameraCandidateCount = 0;
  window.clearTimeout(cameraCandidateTimer);
  cameraCandidateTimer = null;
}

// ── Input helpers ─────────────────────────────────────────────────────────────
function getActiveBarcodeInput() {
  return cameraUsageMode === 'search'
    ? document.getElementById('searchInput')
    : document.getElementById('scanInput');
}

function setScannedBarcode(barcode) {
  const input = getActiveBarcodeInput();
  const nextValue = String(barcode || '').trim();
  try {
    input.focus({preventScroll: true});
  } catch (error) {
    try { input.focus(); } catch (innerError) {}
  }
  input.value = nextValue;
  input.setAttribute('value', nextValue);
  input.dispatchEvent(new Event('input', {bubbles: true}));
  input.dispatchEvent(new Event('change', {bubbles: true}));
  if (typeof input.setSelectionRange === 'function') {
    input.setSelectionRange(input.value.length, input.value.length);
  }
  if (input.id === 'scanInput') persistScanDraft();
}

function clearScanInput() {
  const input = document.getElementById('scanInput');
  input.value = '';
  input.blur();
  currentScanProduct = null;
  lastLookedUpBarcode = '';
  activeLookupBarcode = '';
  scanPaused = false;
  resetCameraCandidate();
  persistScanDraft();
  if (scannerStream || html5Scanner) getCameraDom().status.textContent = 'Recherche code...';
}

function clearSearchInput() {
  window.clearTimeout(searchTimer);
  document.getElementById('searchInput').value = '';
  document.getElementById('searchResults').innerHTML = '';
  if (scannerStream || html5Scanner) getCameraDom().status.textContent = 'Recherche code...';
}

// ── Barcode pattern helpers ───────────────────────────────────────────────────
function looksLikeBarcode(value) {
  return /^[0-9A-Za-z.-]{6,}$/.test(value);
}

function looksLikeCompleteRetailBarcode(value) {
  return /^[0-9]{8}$|^[0-9]{12}$|^[0-9]{13}$|^[0-9]{14}$/.test(value);
}

// ── Hardware scanner support ──────────────────────────────────────────────────
function focusScanInput() {
  const input = document.getElementById('scanInput');
  if (input && document.getElementById('scan').classList.contains('active')) {
    input.focus({preventScroll: true});
  }
}

function handleScanInputKey(event) {
  if (event.key !== 'Enter' && event.key !== 'Tab') return;
  const value = document.getElementById('scanInput').value.trim();
  if (!value) return;
  if (!requireEditorSession('utiliser le scan')) { event.preventDefault(); return; }
  event.preventDefault();
  lookupScanFromInput(true);
}

function handleHardwareScannerKey(event) {
  if (!document.getElementById('scan').classList.contains('active')) return;
  if (event.ctrlKey || event.altKey || event.metaKey) return;

  const target = event.target;
  const tag = target && target.tagName ? target.tagName.toLowerCase() : '';
  const isEditable = tag === 'input' || tag === 'textarea' || tag === 'select' || target?.isContentEditable;
  if (isEditable) return;

  if (event.key === 'Enter' || event.key === 'Tab') {
    if (hardwareScanBuffer.length >= 6) {
      event.preventDefault();
      setScannedBarcode(hardwareScanBuffer);
      hardwareScanBuffer = '';
      lookupScanFromInput(true);
    }
    return;
  }

  if (event.key.length !== 1) return;
  hardwareScanBuffer += event.key;
  window.clearTimeout(hardwareScanTimer);
  hardwareScanTimer = window.setTimeout(() => {
    if (looksLikeCompleteRetailBarcode(hardwareScanBuffer)) {
      setScannedBarcode(hardwareScanBuffer);
      lookupScanFromInput(true);
    } else if (looksLikeBarcode(hardwareScanBuffer)) {
      setScannedBarcode(hardwareScanBuffer);
      document.getElementById('scannerStatus').textContent = 'Code rempli - appuyez sur Verifier';
    }
    hardwareScanBuffer = '';
  }, 500);
}

function showCameraHint(message) {
  document.getElementById('scanResult').innerHTML = `<div class="msg error">${esc(message)}</div>`;
}

// ── Device support display ────────────────────────────────────────────────────
function updateDeviceSupport() {
  const cameraSupport = document.getElementById('cameraSupport');
  const scannerSupport = document.getElementById('scannerSupport');
  const supportHint = document.getElementById('supportHint');
  if (!cameraSupport || !scannerSupport) return;
  const hasCamera = !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
  const isSecure = window.isSecureContext || location.hostname === '127.0.0.1' || location.hostname === 'localhost';
  const hasNative = 'BarcodeDetector' in window;
  const hasZXing = !!window.ZXing;

  cameraSupport.textContent = hasCamera && isSecure ? 'Disponible' : 'HTTPS requis';
  cameraSupport.style.background = hasCamera && isSecure ? '#ecfdf5' : '#fef2f2';
  cameraSupport.style.color = hasCamera && isSecure ? '#065f46' : '#991b1b';

  if (nativeScanActive || hasNative) {
    scannerSupport.textContent = 'Natif (rapide)';
    scannerSupport.style.background = '#ecfdf5';
    scannerSupport.style.color = '#065f46';
    if (supportHint) supportHint.textContent = 'Ce navigateur utilise le scanner natif du telephone (BarcodeDetector). C est le mode le plus rapide, equivalent a Google Lens.';
  } else if (zxingActive || hasZXing) {
    scannerSupport.textContent = 'ZXing (bon)';
    scannerSupport.style.background = '#fefce8';
    scannerSupport.style.color = '#854d0e';
    if (supportHint) supportHint.textContent = 'Ce navigateur utilise ZXing. C est bon mais plus lent que le mode natif. Sur Android, utilisez Chrome pour obtenir le scanner natif.';
  } else {
    scannerSupport.textContent = hasNative ? 'Natif disponible' : 'ZXing / Quagga';
    scannerSupport.style.background = '#ecfdf5';
    scannerSupport.style.color = '#065f46';
    if (supportHint) supportHint.textContent = hasNative
      ? 'Scanner natif disponible. Ouvrez la camera pour l activer.'
      : 'Sur Android, utilisez Chrome pour le scanner natif (le plus rapide). Sur iPhone, Quagga est utilise automatiquement.';
  }

  if (!isSecure) {
    if (supportHint) supportHint.textContent = 'Pour la camera sur telephone, utilisez une URL HTTPS.';
    cameraSupport.textContent = 'HTTPS requis';
    cameraSupport.style.background = '#fef2f2';
    cameraSupport.style.color = '#991b1b';
  }
}
