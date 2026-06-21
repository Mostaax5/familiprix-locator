# Scanner — how the barcode reading works (READ BEFORE TOUCHING `static/scanner.js`)

This document exists because getting the scanner to read both **normal product
barcodes** and **small glossy étiquette / liquidation labels** — fast and
accurately, on iPhone — took a very long debugging session. If it ever breaks
again, read this first. Do **not** re-derive it from scratch.

---

## The big picture: 3 decode engines run in parallel

When the camera opens (`startCamera`), the app picks a path:

1. **Native `BarcodeDetector`** — hardware/OS barcode reader.
   - Available on **Android Chrome** and **Chrome desktop**. **NOT on iPhone Safari**
     (Safari has no `BarcodeDetector`, even iOS 17/18). So on iPhone we always
     fall through to the Quagga path below.
   - When available: instant, never hallucinates → accepted on the first read.

2. **Quagga2** (`startQuaggaScanner`) — the iPhone/Firefox path. Reads **normal
   product barcodes** fast. Uses `halfSample: true` (see "Critical settings").

3. Running **in parallel with Quagga**, two extra engines started inside
   `startQuaggaScanner`:
   - **ZXing** (`startZXingParallel`) — reads **small / dense étiquette barcodes**
     that Quagga can't, because ZXing scans at **full resolution**.
   - **OCR / Tesseract** (`maybeRunOcrFallback`) — last resort: reads the
     **printed digits** under the barcode when the bars themselves can't be decoded.

Whichever engine returns a valid, checksum-passing barcode first wins. Every
result goes through `onDecodedCode()`.

---

## Why each engine exists (the hard-won reasons)

### Quagga uses `halfSample: true` — and must keep it
`halfSample: true` downsamples each camera frame 2× before decoding. This is
**mandatory on iPhone**: without it, the JS CPU cannot process full-resolution
frames fast enough and Quagga effectively processes **0 frames per second**
(camera looks on, reads nothing). We learned this the hard way — `halfSample:false`
= "doesn't read anything".

The downside: halfSample destroys the **thin bars of small étiquette barcodes**,
so Quagga reads them wrong (or as a too-short EAN-8) or not at all. That is why
ZXing exists alongside it.

### ZXing reads the small barcodes Quagga can't
ZXing scans the image at **full resolution** (no halfSample) so thin bars survive.
It gives the **exact** barcode (not a guess). This is the engine that finally made
étiquettes work.

**CRITICAL ZXing API note** — the method `decodeFromCanvas()` **does not exist**
in the loaded `@zxing/library` UMD build. Calling it throws `TypeError` on every
frame (silent — caught by the try/catch) so ZXing appears to "do nothing". The
**correct** still-image decode path is:

```js
const source = new ZXing.HTMLCanvasElementLuminanceSource(canvas);
const bitmap = new ZXing.BinaryBitmap(new ZXing.HybridBinarizer(source));
const result = multiFormatReader.decode(bitmap);   // hints via setHints()
multiFormatReader.reset();                          // REQUIRED between frames
```

If ZXing ever stops working, check the diagnostic panel (below) for
`ZXing lit : (TypeError)` or `(SomeError)` — that means the API/build changed.

### OCR (Tesseract) reads the printed digits — last resort
Every retail barcode has its number printed below the bars. When bars can't be
decoded, OCR reads those digits. Two things that were essential to get right:

- **Crop wide** (`_ocrCrop`, ~88%×75% of frame). The barcode is often in the
  upper-middle, not centred — a tight crop cut the digits off.
- **Parse PER LINE** (`ocrBarcodeCandidate`). Tesseract reads the bar pattern as
  one garbage line and the printed digits as a separate line. Only by checking
  each line separately do we isolate the clean 13-digit row. Joining everything
  gave 16-digit garbage like `2616303324322832`.
- Accept only an **exact-length (8/12/13/14) run that passes checksum**, seen
  **twice within a short window** (`ocrHistory`) to tolerate one-digit flicker.

OCR is the weakest link (mobile Tesseract is slow + imperfect). If étiquettes
ever need to be bulletproof, the proper upgrade is **server-side OCR** (a
`/api/read-digits` endpoint) — far more accurate, works on every device, but the
user chose to keep everything client-side / free.

---

## Accuracy guards (why it doesn't read random numbers anymore)

Earlier versions read random numbers. The causes and fixes, all still in place:

- **Quagga decode-error gate** (`quaggaDecodeError`): every Quagga read carries a
  per-bar error score. `> 0.30` → rejected outright. `< 0.10` → trusted, accepted
  instantly. In between → must appear on **2 consecutive frames**
  (`confirmStableCameraBarcode`). Real barcodes are stable frame-to-frame;
  halfSample artifacts differ every frame and never repeat.
- **Checksum** (`validateRetailBarcode`): every candidate (camera, ZXing, OCR)
  must pass the EAN/UPC mod-10 checksum. 8/12/13/14-digit lengths only.
- **OCR**: tight whole-number matching + 2-in-window agreement (above). The OCR
  fallback that scanned a wide band and tried every substring was the original
  "random numbers" generator — do **not** reintroduce substring slicing.

## Leading-zero normalisation
Quagga returns UPC-A barcodes in 13-digit EAN-13 form (a `0` prepended).
`normalizeScannedBarcode()` strips a leading `0` from 13-digit reads so we store
the real 12-digit UPC-A. The product lookup tries both forms anyway, so existing
data still matches.

---

## The on-screen diagnostic panel (your debugging lifeline)

A live panel can show, with **no devtools / no Mac needed**:
```
moteurs  Quagga:✓  ZXing:✓  OCR:✓
Quagga lit : 24221696   (erreur 0.207)
ZXing  lit : 3616303242282 ✓
OCR    lit : 3616303242282 ✓
```
This single view tells you which engines loaded and exactly what each reads
(even rejected reads). It is what finally cracked the étiquette problem.

**It is OFF by default** (clean UI for employees). To turn it on:
1. Open the site, open the browser console (or just run the line via a bookmarklet).
2. `localStorage.setItem('familiprixScanDebug','1')`
3. Reopen the camera. The black panel appears at the bottom.
- Turn off: `localStorage.removeItem('familiprixScanDebug')`

On iPhone you can enable it by connecting the phone to a Mac (Safari → Develop →
iPhone → Console) once, or by adding a temporary button — but the flag persists
in localStorage so you only set it once.

---

## Critical settings (current working values — change with care)

In `startQuaggaScanner` (`static/scanner.js`):
- `width/height ideal: 1920×1080` — high capture res so small bars keep detail.
- `area: {top:'15%', right:'5%', left:'5%', bottom:'15%'}` — wide scan area.
- `locator: {patchSize:'small', halfSample:true}` — `small` finds small barcodes;
  `halfSample:true` is **mandatory for iPhone speed** (see above).
- `frequency: 20`.
- `readers: ['ean_reader','upc_reader','ean_8_reader','upc_e_reader']` — EAN-13
  first so a 13-digit code isn't misread as EAN-8.
- ZXing parallel interval: 200 ms, near-full-frame crop (96%×90%).
- OCR interval: 600 ms, crop 88%×75%, per-line parsing.

## What NOT to do (mistakes already made — don't repeat)
- ❌ Don't set `halfSample:false` — iPhone reads nothing.
- ❌ Don't use `ZXing ... decodeFromCanvas()` — it doesn't exist → TypeError.
- ❌ Don't make OCR slice every substring — it fabricates random valid barcodes.
- ❌ Don't accept Quagga reads on a single frame without the decode-error gate —
  halfSample artifacts pass checksum occasionally.
- ❌ Don't tighten the ZXing/OCR crop to the centre — barcodes sit in the upper
  part of the frame and get cut off.
- ❌ Don't add elements (scan-line, overlays) **inside** the camera viewport —
  Quagga processes them as image content and gets false reads.

## Files
- `static/scanner.js` — all scanning logic (this document's subject). **Protected
  module — every change must be intentional and targeted.**
- `static/api.js` — backend API calls (unrelated to scanning).
- It depends on app-side functions: `lookupScanFromInput`, `doSearchValue`,
  `persistScanDraft`, `requireEditorSession`, `esc` (in the split JS modules).
