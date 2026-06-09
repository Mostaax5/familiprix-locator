async function apiFetch(url, options = {}) {
  const opts = {...options, cache: options.cache || 'no-store'};
  opts.headers = {...(options.headers || {})};
  const res = await fetch(url, opts);
  const contentType = res.headers.get('content-type') || '';
  const data = contentType.includes('application/json') ? await res.json() : await res.text();
  return {res, data};
}

async function apiGetProducts() {
  const {res, data} = await apiFetch('/api/products');
  if (!res.ok) throw new Error('products-fetch');
  return Array.isArray(data) ? data.map(normalizeProduct) : [];
}

async function apiSearchProducts(query) {
  const trimmed = String(query || '').trim();
  if (!trimmed) return [];
  const {res, data} = await apiFetch(`/api/products/search?q=${encodeURIComponent(trimmed)}&limit=40`);
  if (!res.ok) throw new Error('search-fetch');
  return Array.isArray(data) ? data.map(normalizeProduct) : [];
}

async function apiGetProductByBarcode(barcode) {
  const {res, data} = await apiFetch(`/api/products/barcode/${encodeURIComponent(barcode)}`);
  if (!res.ok) {
    const error = new Error(res.status === 404 ? 'not-found' : 'barcode-fetch');
    error.status = res.status;
    throw error;
  }
  return normalizeProduct(data);
}

async function apiAddProduct(payload) {
  try {
    const {res, data} = await apiFetch('/api/products', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur serveur pendant l ajout.'};
  } catch (error) {
    return {success: false, error: 'Impossible d ajouter le produit pour le moment.'};
  }
}

async function apiUpdateProduct(product) {
  try {
    const {res, data} = await apiFetch(`/api/products/${product.id}`, {
      method: 'PUT',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(product)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur de sauvegarde.'};
  } catch (error) {
    return {success: false, error: 'Impossible de sauvegarder le produit pour le moment.'};
  }
}

async function apiDeleteProduct(id) {
  try {
    const {res, data} = await apiFetch(`/api/products/${id}`, {method:'DELETE', headers: getEditorHeaders()});
    return res.ok ? data : {success: false, error: data.error || 'Erreur de suppression.'};
  } catch (error) {
    return {success: false, error: 'Impossible de supprimer le produit pour le moment.'};
  }
}

async function apiGetLayoutAisles() {
  const {res, data} = await apiFetch('/api/layout/aisles');
  if (!res.ok) throw new Error('layout-fetch');
  return Array.isArray(data) ? data : [];
}

async function apiCreateLayoutAisle(payload) {
  const {res, data} = await apiFetch('/api/layout/aisles', {
    method: 'POST',
    headers: {'Content-Type':'application/json', ...getEditorHeaders()},
    body: JSON.stringify(payload)
  });
  return res.ok ? data : {success: false, error: data.error || 'Erreur de creation d allee.'};
}

async function apiUpdateLayoutAisle(aisle, payload) {
  const {res, data} = await apiFetch(`/api/layout/aisles/${encodeURIComponent(aisle)}`, {
    method: 'PUT',
    headers: {'Content-Type':'application/json', ...getEditorHeaders()},
    body: JSON.stringify(payload)
  });
  return res.ok ? data : {success: false, error: data.error || 'Erreur de mise a jour d allee.'};
}

async function apiDeleteLayoutAisle(aisle) {
  try {
    const {res, data} = await apiFetch(`/api/layout/aisles/${encodeURIComponent(aisle)}`, {
      method: 'DELETE',
      headers: getEditorHeaders()
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur de suppression d allee.'};
  } catch (error) {
    return {success: false, error: 'Impossible de supprimer l allee pour le moment.'};
  }
}

async function apiLookupOnline(barcode, signal) {
  try {
    const {data} = await apiFetch(`/api/products/lookup/${encodeURIComponent(barcode)}`, {signal});
    return data;
  } catch (error) {
    return {found: false};
  }
}

async function apiGenerateProductAssist(payload) {
  try {
    const {res, data} = await apiFetch('/api/products/assist', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Aide client indisponible pour le moment.'};
  } catch (error) {
    return {success: false, error: 'Aide client indisponible pour le moment.'};
  }
}

async function apiGenerateClientHelp(payload) {
  try {
    const {res, data} = await apiFetch('/api/client/help', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Reponse client indisponible pour le moment.'};
  } catch (error) {
    return {success: false, error: 'Reponse client indisponible pour le moment.'};
  }
}

async function apiGetSystemInfo() {
  const {res, data} = await apiFetch('/api/system/info');
  if (!res.ok) throw new Error('system-info');
  return data;
}
