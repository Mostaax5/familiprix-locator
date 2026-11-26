import { el, productCard, productDetail, comparison } from "./view.js";

const $ = (id) => document.getElementById(id);
const state = {
  query: "",
  category: "",
  aisle: "",
  next: null,
  sequence: 0,
  controller: null,
  categories: new Map(),
  selected: new Map(),
  summary: null,
  auth: null,
  interpreted: false,
};
let inputTimer,
  stopScanner = () => {},
  scanGeneration = 0;
let detailSequence = 0;

async function api(url, { signal, ...options } = {}) {
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) controller.abort();
  const timer = setTimeout(abort, 12000);
  try {
    const response = await fetch(url, {
      cache: "no-store",
      ...options,
      signal: controller.signal,
    });
    const body = await response.json();
    if (!response.ok)
      throw new Error(
        response.status === 503
          ? "Le catalogue est momentanément indisponible. Réessayez dans un instant."
          : body.error || "Impossible de charger les produits.",
      );
    return body;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}
function notice(text, error = false) {
  $("notice").textContent = text;
  $("notice").hidden = !text;
  $("notice").classList.toggle("error", error);
}
function syncComparison() {
  $("compare-bar").hidden = !state.selected.size;
  $("compare-count").textContent =
    `${state.selected.size} produit${state.selected.size > 1 ? "s" : ""}`;
  $("compare-open").disabled = state.selected.size < 2;
  document.querySelectorAll(".compare-label input").forEach((n) => {
    n.checked = state.selected.has(n.dataset.key);
  });
}
function select(doc, checked) {
  if (checked && !state.selected.has(doc.key) && state.selected.size >= 3) {
    notice("Vous pouvez comparer jusqu’à trois produits à la fois.");
    return false;
  }
  if (checked) state.selected.set(doc.key, doc);
  else state.selected.delete(doc.key);
  syncComparison();
  return checked;
}
async function detail(key) {
  const sequence = ++detailSequence;
  const dialog = $("detail");
  $("detail-content").replaceChildren(el("p", "subtle", "Chargement…"));
  dialog.showModal();
  try {
    const doc = await api(
      "/api/catalogue/item?" + new URLSearchParams({ key }),
    );
    if (dialog.open && sequence === detailSequence)
      $("detail-content").replaceChildren(productDetail(doc));
  } catch (error) {
    if (sequence === detailSequence)
      $("detail-content").replaceChildren(el("p", "subtle", error.message));
  }
}
function render(result, append) {
  if (!append) $("results").replaceChildren();
  const fragment = document.createDocumentFragment();
  result.items.forEach((doc) =>
    fragment.append(
      productCard(doc, {
        categories: state.categories,
        selected: state.selected,
        onOpen: detail,
        onCompare: select,
      }),
    ),
  );
  result.items.forEach((doc) => {
    if (state.selected.has(doc.key)) state.selected.set(doc.key, doc);
  });
  $("results").append(fragment);
  $("result-count").textContent =
    `${result.total.toLocaleString("fr-CA")} produit${result.total > 1 ? "s" : ""}`;
  $("empty").hidden = result.total !== 0;
  state.next = result.next_offset;
  $("more").hidden = state.next === null;
  $("suggestions").replaceChildren();
  for (const suggestion of result.suggestions || []) {
    const button = el("button", "secondary", `Essayer « ${suggestion} »`);
    button.type = "button";
    button.addEventListener("click", () => {
      $("query").value = suggestion;
      search();
    });
    $("suggestions").append(button);
  }
  if (result.interpretation) {
    state.interpreted = true;
    notice(
      "Votre demande semble correspondre à ce rayon. Voici les produits du catalogue à explorer.",
    );
  }
}
async function search(append = false) {
  clearTimeout(inputTimer);
  state.controller?.abort();
  state.controller = new AbortController();
  const { signal } = state.controller;
  const sequence = ++state.sequence;
  if (!append) state.interpreted = false;
  state.query = $("query").value.trim();
  state.category = $("category").value;
  state.aisle = $("aisle").value;
  const params = new URLSearchParams({
    q: state.query,
    category: state.category,
    aisle: state.aisle,
    offset: append ? state.next || 0 : 0,
  });
  const urlParams = new URLSearchParams();
  for (const k of ["q", "category", "aisle"])
    if (params.get(k)) urlParams.set(k, params.get(k));
  history.replaceState(
    null,
    "",
    location.pathname + (urlParams.size ? "?" + urlParams : ""),
  );
  $("reset").hidden = !state.query && !state.category && !state.aisle;
  $("results").setAttribute("aria-busy", "true");
  $("more").disabled = true;
  $("interpret").hidden = true;
  notice("");
  if (!append) {
    $("results").replaceChildren(
      ...Array.from({ length: 4 }, () => el("div", "skeleton")),
    );
    $("empty").hidden = true;
    $("more").hidden = true;
  }
  try {
    const result =
      append && state.interpreted
        ? await api("/api/catalogue/interpret", {
            signal,
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-CSRF-Token": state.auth?.csrf_token || "",
            },
            body: JSON.stringify({ q: state.query, offset: state.next || 0 }),
          })
        : await api("/api/catalogue/search?" + params, { signal });
    if (sequence !== state.sequence) return;
    render(result, append);
    // Local retrieval is instant. Optional AI gets a separate, bounded request
    // only for a miss, without blocking or rewriting already-matched products.
    if (
      !result.total &&
      state.query &&
      !state.category &&
      !state.aisle &&
      state.auth?.authenticated &&
      state.summary?.assistant !== "local"
    ) {
      notice("Recherche du rayon correspondant à votre demande…");
      try {
        const interpreted = await api("/api/catalogue/interpret", {
          signal,
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRF-Token": state.auth.csrf_token || "",
          },
          body: JSON.stringify({ q: state.query }),
        });
        if (sequence !== state.sequence) return;
        notice("");
        render(interpreted, false);
      } catch (error) {
        if (sequence === state.sequence && !signal.aborted)
          notice(
            "L’assistance est indisponible. La recherche par nom et code-barres reste disponible.",
          );
      }
    }
  } catch (error) {
    if (sequence !== state.sequence || signal.aborted) return;
    if (!append) $("results").replaceChildren();
    notice(
      error.name === "AbortError"
        ? "La recherche prend trop de temps. Réessayez."
        : error.message,
      true,
    );
    $("result-count").textContent = "Recherche indisponible";
  } finally {
    if (sequence === state.sequence) {
      $("results").setAttribute("aria-busy", "false");
      $("more").disabled = false;
    }
  }
}
async function boot() {
  const params = new URLSearchParams(location.search);
  $("query").value = params.get("q") || "";
  $("search-form").addEventListener("submit", (e) => {
    e.preventDefault();
    search();
  });
  $("query").addEventListener("input", () => {
    clearTimeout(inputTimer);
    state.controller?.abort();
    ++state.sequence;
    inputTimer = setTimeout(() => search(), 180);
  });
  ["category", "aisle"].forEach((id) =>
    $(id).addEventListener("change", () => search()),
  );
  $("reset").addEventListener("click", () => {
    $("query").value = "";
    $("category").value = "";
    $("aisle").value = "";
    search();
  });
  $("more").addEventListener("click", () => search(true));
  $("compare-clear").addEventListener("click", () => {
    state.selected.clear();
    syncComparison();
  });
  $("compare-open").addEventListener("click", () => {
    $("comparison-content").replaceChildren(
      comparison([...state.selected.values()]),
    );
    $("comparison").showModal();
  });
  document
    .querySelectorAll("[data-close]")
    .forEach((b) =>
      b.addEventListener("click", () => $(b.dataset.close).close()),
    );
  $("scanner").addEventListener("close", () => {
    ++scanGeneration;
    stopScanner();
  });
  $("scan-open").addEventListener("click", async () => {
    const generation = ++scanGeneration;
    $("scanner").showModal();
    $("scan-status").textContent = "Préparation de la caméra…";
    const { startScanner } = await import("./scanner.js");
    if (generation !== scanGeneration) return;
    const stop = await startScanner(
      $("scan-video"),
      (code) => {
        if (generation !== scanGeneration) return;
        $("query").value = code;
        $("scanner").close();
        search();
      },
      (text) => {
        if (generation === scanGeneration) $("scan-status").textContent = text;
      },
    );
    if (generation !== scanGeneration) stop();
    else stopScanner = stop;
  });
  window.addEventListener("pagehide", () => {
    ++scanGeneration;
    stopScanner();
    state.controller?.abort();
  });
  window.addEventListener("offline", () => {
    $("connection").textContent = "Connexion interrompue";
    notice(
      "Vous êtes hors ligne. Reconnectez-vous pour consulter les emplacements à jour.",
    );
  });
  window.addEventListener("online", () => {
    $("connection").textContent = "Catalogue du magasin";
    search();
  });
  const authPromise = api("/api/auth/status")
    .then((a) => {
      state.auth = a;
    })
    .catch(() => {});
  try {
    state.summary = await api("/api/catalogue/summary");
    $("catalogue-count").textContent =
      `${state.summary.products.toLocaleString("fr-CA")} produits · ${state.summary.placements.toLocaleString("fr-CA")} emplacements`;
    for (const c of state.summary.categories) {
      state.categories.set(c.id, c.label);
      const o = el("option", "", `${c.label} (${c.count})`);
      o.value = c.id;
      $("category").append(o);
    }
    for (const aisle of state.summary.aisles) {
      const o = el("option", "", `Allée ${aisle}`);
      o.value = aisle;
      $("aisle").append(o);
    }
    $("category").value = params.get("category") || "";
    $("aisle").value = params.get("aisle") || "";
  } catch {
    $("catalogue-count").textContent = "Catalogue indisponible";
  }
  await authPromise;
  await search();
}
boot();
