// Camera acquisition is lazy; typing/searching never downloads a scanner engine.
export async function startScanner(video, onCode, onStatus) {
  let stopped = false,
    stream = null,
    reader = null,
    timer = null;
  const stop = () => {
    stopped = true;
    clearTimeout(timer);
    reader?.reset();
    stream?.getTracks().forEach((t) => t.stop());
    video.srcObject = null;
  };
  if (!navigator.mediaDevices?.getUserMedia) {
    onStatus(
      "Caméra indisponible. Saisissez le code-barres dans la recherche.",
    );
    return stop;
  }
  try {
    const supported = globalThis.BarcodeDetector
      ? await BarcodeDetector.getSupportedFormats()
      : [];
    const formats = ["ean_13", "ean_8", "upc_a", "upc_e"].filter((f) =>
      supported.includes(f),
    );
    if (formats.length) {
      const detector = new BarcodeDetector({ formats });
      stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: { ideal: "environment" } },
        audio: false,
      });
      if (stopped) {
        stop();
        return stop;
      }
      video.srcObject = stream;
      await video.play();
      const tick = async () => {
        if (stopped) return;
        try {
          const codes = await detector.detect(video);
          if (codes.length && !stopped) {
            const code = codes[0].rawValue;
            stop();
            onCode(code);
            return;
          }
        } catch {
          /* Next frame may be ready. */
        }
        if (!stopped) timer = setTimeout(tick, 160);
      };
      onStatus("Placez le code-barres dans le cadre.");
      tick();
    } else {
      if (!globalThis.ZXing)
        await new Promise((resolve, reject) => {
          const script = document.createElement("script");
          script.src = "/static/vendor/zxing-library-0.21.3.min.js";
          script.onload = resolve;
          script.onerror = reject;
          document.head.append(script);
        });
      if (stopped) return stop;
      reader = new globalThis.ZXing.BrowserMultiFormatReader();
      await reader.decodeFromVideoDevice(undefined, video, (result) => {
        if (result && !stopped) {
          const code = result.getText();
          stop();
          onCode(code);
        }
      });
      onStatus("Placez le code-barres dans le cadre.");
    }
  } catch {
    stop();
    onStatus(
      "Caméra indisponible ou accès refusé. Saisissez le code-barres dans la recherche.",
    );
  }
  return stop;
}
