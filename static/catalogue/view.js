// Render data as text nodes. Neither source text nor model output becomes HTML.
export function el(tag, className = "", text = "") {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== "") node.textContent = text;
  return node;
}

const forms = {
  tablet: "comprimés",
  capsule: "capsules",
  liquid: "liquide",
  cream: "crème",
  spray: "vaporisateur",
  shampoo: "shampoing",
  conditioner: "revitalisant",
  refill: "recharge",
  device: "appareil",
};
const flavours = {
  mint: "menthe",
  grape: "raisin",
  strawberry: "fraise",
  cherry: "cerise",
  lemon: "citron",
  vanilla: "vanille",
  chocolate: "chocolat",
  unflavoured: "sans saveur",
};
const labels = {
  brand: "Marque",
  package_size: "Format",
  package_unit: "Unité",
  strength: "Concentration",
  dosage_form: "Forme",
  flavour: "Saveur",
  colour: "Couleur",
  variant: "Variante",
  ingredients: "Ingrédients",
  manufacturer: "Fabricant",
  category: "Catégorie",
  compatibility: "Compatibilité",
  purpose: "Usage indiqué par la source",
  official_name_fr: "Nom français",
  official_name_en: "Nom anglais",
};
export function attributeText(field, values = []) {
  return values
    .map((raw) => {
      if (field === "formats") return forms[raw] || raw;
      if (field === "flavours") return flavours[raw] || raw;
      if (!raw.includes(":")) return raw;
      const [amount, unit] = raw.split(":");
      const number = Number(amount);
      if (!Number.isFinite(number)) return "";
      if (unit === "unit") return `${number.toLocaleString("fr-CA")} unités`;
      if (field === "sizes" && unit === "mg")
        return `${(number / 1000).toLocaleString("fr-CA")} g`;
      return `${number.toLocaleString("fr-CA")} ${unit}`;
    })
    .filter(Boolean)
    .join(" · ");
}
export function locationText(p) {
  const side = { Gauche: "côté A", Droite: "côté B" }[p.side] || p.side;
  return `Allée ${p.aisle} · ${side} · Section ${p.section || "1"} · Tablette ${p.shelf} · Position ${p.position}`;
}
export function visual(doc, categories = new Map()) {
  const node = el("div", "product-visual");
  const fallback = el(
    "span",
    "product-monogram",
    doc.title
      .split(/\s+/)
      .slice(0, 2)
      .map((s) => s[0] || "")
      .join("")
      .toUpperCase(),
  );
  fallback.setAttribute("aria-hidden", "true");
  node.append(fallback);
  if (doc.image_url) {
    const img = el("img");
    img.src = doc.image_url;
    img.alt = "";
    img.loading = "lazy";
    img.decoding = "async";
    img.referrerPolicy = "no-referrer";
    img.addEventListener("load", () => {
      fallback.hidden = true;
    });
    img.addEventListener("error", () => {
      img.remove();
      fallback.hidden = false;
    });
    node.append(img);
  }
  const category = categories.get(doc.concepts[0]);
  if (category) node.append(el("span", "category-label", category));
  return node;
}
export function chips(doc) {
  const node = el("div", "fact-chips");
  for (const key of ["strengths", "sizes", "counts", "formats", "flavours"]) {
    const text = attributeText(key, doc.attributes[key]);
    if (text) node.append(el("span", "fact-chip", text));
  }
  return node;
}
export function productCard(doc, { categories, selected, onOpen, onCompare }) {
  const article = el("article", "product-card");
  const open = el("button", "product-open");
  open.type = "button";
  open.setAttribute("aria-label", `Voir ${doc.title}`);
  const body = el("div", "card-body");
  body.append(
    el("div", "brand-label", doc.facts.brand?.value || "Catalogue magasin"),
    el("h3", "product-title", doc.title),
    chips(doc),
  );
  open.append(visual(doc, categories), body);
  open.addEventListener("click", () => onOpen(doc.key));
  const location = el("div", "product-location");
  if (doc.locations.length) {
    const p = doc.locations[0];
    location.append(el("span", "", `Allée ${p.aisle} · Tablette ${p.shelf}`));
    location.append(
      el(
        "small",
        "",
        `${{ Gauche: "Côté A", Droite: "Côté B" }[p.side] || p.side} · Section ${p.section || "1"} · Position ${p.position}`,
      ),
    );
    if (doc.locations.length > 1)
      location.append(
        el(
          "small",
          "",
          `+ ${doc.locations.length - 1} autre${doc.locations.length > 2 ? "s" : ""} emplacement${doc.locations.length > 2 ? "s" : ""}`,
        ),
      );
  } else location.append(el("span", "", "Emplacement non renseigné"));
  const label = el("label", "compare-label");
  const checkbox = el("input");
  checkbox.type = "checkbox";
  checkbox.checked = selected.has(doc.key);
  checkbox.dataset.key = doc.key;
  checkbox.addEventListener("change", () => {
    checkbox.checked = onCompare(doc, checkbox.checked);
  });
  label.append(checkbox, document.createTextNode("Comparer ce produit"));
  article.append(open, location, label);
  return article;
}
export function productDetail(doc) {
  const fragment = document.createDocumentFragment();
  const hero = el("div", "detail-hero");
  const heading = el("div");
  heading.append(
    el("h2", "", doc.title),
    el(
      "p",
      "detail-barcode",
      `UPC ${doc.barcodes.join(" / ") || "non renseigné"}`,
    ),
    chips(doc),
  );
  hero.append(visual(doc), heading);
  fragment.append(hero);
  const locations = el("section", "detail-section");
  locations.append(
    el(
      "h3",
      "",
      `Où le trouver${doc.locations.length > 1 ? ` · ${doc.locations.length} emplacements` : ""}`,
    ),
  );
  for (const p of doc.locations) {
    const row = el("div", "location-row");
    row.append(el("strong", "", locationText(p)));
    if (p.in_stock === 0)
      row.append(el("span", "", "Signalé absent à cet emplacement"));
    locations.append(row);
  }
  if (!doc.locations.length)
    locations.append(
      el(
        "p",
        "subtle",
        "Ce produit figure au catalogue, mais aucun emplacement n’est renseigné.",
      ),
    );
  fragment.append(locations);
  const facts = el("section", "detail-section");
  facts.append(el("h3", "", "Les faits du produit"));
  const list = el("dl", "fact-list");
  for (const [key, label] of Object.entries(labels)) {
    if (
      !doc.facts[key] ||
      ["package_unit", "official_name_fr", "official_name_en"].includes(key)
    )
      continue;
    const f = doc.facts[key];
    const unit = doc.facts.package_unit?.value || "";
    const value =
      key === "package_size"
        ? `${f.value} ${unit === "un" ? "unités" : unit}`.trim()
        : f.value;
    const dd = el("dd");
    if (["ingredients", "purpose", "compatibility"].includes(key)) {
      const more = el("details");
      more.append(
        el("summary", "", "Lire les informations de la source"),
        el("p", "", value),
      );
      dd.append(more);
    } else dd.append(document.createTextNode(value));
    const source = el("small", "", `Source : ${f.source}`);
    if (f.url) {
      const link = el("a", "", "Consulter");
      link.href = f.url;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      source.append(document.createTextNode(" · "), link);
    }
    dd.append(source);
    list.append(el("dt", "", label), dd);
  }
  facts.append(
    list,
    el(
      "p",
      "subtle",
      "Les repères de format proviennent du nom du catalogue ou des données de la source. Aucune description n’est générée.",
    ),
  );
  fragment.append(facts);
  const evidence = el("details", "detail-section");
  evidence.append(
    el("summary", "", "Nom d’origine et informations de la source"),
  );
  for (const name of doc.original_labels) evidence.append(el("p", "", name));
  if (doc.store_codes.length)
    evidence.append(
      el("p", "", `Code magasin : ${doc.store_codes.join(", ")}`),
    );
  if (doc.description)
    evidence.append(
      el("p", "", doc.description.value),
      el("p", "subtle", `Source : ${doc.description.source}`),
    );
  if (doc.conflicts.length)
    evidence.append(
      el(
        "p",
        "subtle",
        "Certaines sources ne concordent pas. Les valeurs en conflit ne sont pas présentées comme des faits. Le produit et ses emplacements sont conservés.",
      ),
    );
  fragment.append(evidence);
  return fragment;
}
export function comparison(docs) {
  const wrap = el("div", "comparison-scroll");
  const table = el("table", "comparison-table");
  const caption = el(
    "caption",
    "sr-only",
    "Comparaison des faits des produits sélectionnés",
  );
  table.append(caption);
  const head = el("thead");
  const headings = el("tr");
  headings.append(el("th", "", "Produit"));
  for (const d of docs) {
    const th = el("th", "", d.title);
    th.scope = "col";
    headings.append(th);
  }
  head.append(headings);
  table.append(head);
  const body = el("tbody");
  const rows = [
    ["UPC", (d) => d.barcodes.join(" / ")],
    [
      "Concentration",
      (d) => attributeText("strengths", d.attributes.strengths),
    ],
    ["Format", (d) => attributeText("sizes", d.attributes.sizes)],
    ["Quantité", (d) => attributeText("counts", d.attributes.counts)],
    [
      "Forme",
      (d) =>
        d.facts.dosage_form?.value ||
        attributeText("formats", d.attributes.formats),
    ],
    [
      "Saveur",
      (d) =>
        d.facts.flavour?.value ||
        attributeText("flavours", d.attributes.flavours),
    ],
    ["Ingrédients", (d) => d.facts.ingredients?.value],
    ["Emplacements", (d) => d.locations.map(locationText).join("\n")],
  ];
  for (const [label, read] of rows) {
    const values = docs.map((d) => read(d) || "Non renseigné");
    const tr = el("tr", new Set(values).size > 1 ? "different" : "");
    const th = el("th", "", label);
    th.scope = "row";
    tr.append(th);
    for (const v of values) tr.append(el("td", "", v));
    body.append(tr);
  }
  table.append(body);
  wrap.append(table);
  return wrap;
}
