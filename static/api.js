async function secureFetch(url, options = {}) {
  const target = new URL(String(url), window.location.origin);
  if (target.origin !== window.location.origin) {
    throw new Error('cross-origin-request-blocked');
  }
  const opts = {...options};
  const skipAuthHandling = Boolean(opts.skipAuthHandling);
  delete opts.skipAuthHandling;
  opts.cache = options.cache || 'no-store';
  opts.credentials = 'same-origin';
  opts.headers = {...(options.headers || {})};
  const method = String(opts.method || 'GET').toUpperCase();
  if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
    const csrfToken = window.AppLock?.csrfToken?.() || '';
    if (csrfToken && !opts.headers['X-CSRF-Token']) {
      opts.headers['X-CSRF-Token'] = csrfToken;
    }
  }
  const res = await fetch(`${target.pathname}${target.search}${target.hash}`, opts);
  if (!skipAuthHandling && (res.status === 401 || res.status === 428)) {
    let payload = {};
    try { payload = await res.clone().json(); } catch (_) {}
    window.AppLock?.handleAuthFailure?.(res.status, payload);
  }
  return res;
}

async function apiFetch(url, options = {}) {
  const res = await secureFetch(url, options);
  const contentType = res.headers.get('content-type') || '';
  const data = contentType.includes('application/json') ? await res.json() : await res.text();
  return {res, data};
}

async function apiGetProducts() {
  // 'no-cache' (NOT no-store): the browser keeps the last copy and revalidates
  // with If-None-Match — when nothing changed the server answers an instant 304
  // and the ~1 MB list is served from the phone's own cache.
  const {res, data} = await apiFetch('/api/products', {cache: 'no-cache'});
  if (!res.ok) throw new Error('products-fetch');
  return Array.isArray(data) ? data.map(normalizeProduct) : [];
}

async function apiSearchProducts(query, field='') {
  const trimmed = String(query || '').trim();
  if (!trimmed) return [];
  const fieldParam = field ? `&field=${encodeURIComponent(field)}` : '';
  const {res, data} = await apiFetch(`/api/products/search?q=${encodeURIComponent(trimmed)}&limit=40${fieldParam}`);
  if (!res.ok) throw new Error('search-fetch');
  return Array.isArray(data) ? data.map(normalizeProduct) : [];
}

async function apiClientFind(query, limit=30, signal) {
  const trimmed = String(query || '').trim();
  if (!trimmed) return [];
  try {
    const {res, data} = await apiFetch(`/api/client/find?q=${encodeURIComponent(trimmed)}&limit=${limit}`, {signal});
    return res.ok && Array.isArray(data) ? data.map(normalizeProduct) : [];
  } catch (e) {
    return [];
  }
}

async function apiSearchReference(query, limit=40) {
  const trimmed = String(query || '').trim();
  if (!trimmed) return [];
  try {
    const {res, data} = await apiFetch(`/api/products/reference-search?q=${encodeURIComponent(trimmed)}&limit=${limit}`);
    return res.ok && Array.isArray(data) ? data.map(normalizeProduct) : [];
  } catch (e) {
    return [];
  }
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
    return res.ok ? data : {success: false, error: data.error || 'Erreur serveur pendant l’ajout.'};
  } catch (error) {
    return {success: false, error: 'Impossible d’ajouter le produit pour le moment.'};
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
  const {res, data} = await apiFetch('/api/layout/aisles', {cache: 'no-cache'});   // ETag revalidation
  if (!res.ok) throw new Error('layout-fetch');
  return Array.isArray(data) ? data : [];
}

async function apiCreateLayoutAisle(payload) {
  try {
    const {res, data} = await apiFetch('/api/layout/aisles', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur de creation d allée.'};
  } catch (e) {
    // Without this, a network blip made « Créer l'allée » die silently.
    return {success: false, error: 'Impossible de créer l’allée pour le moment (réseau).'};
  }
}

async function apiUpdateLayoutAisle(aisle, payload) {
  try {
    const {res, data} = await apiFetch(`/api/layout/aisles/${encodeURIComponent(aisle)}`, {
      method: 'PUT',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur de mise a jour d allée.'};
  } catch (e) {
    return {success: false, error: 'Impossible de sauvegarder l’allée pour le moment (réseau).'};
  }
}

async function apiRemoveLayoutPart(aisle, endpoint, payload) {
  try {
    const {res, data} = await apiFetch(
      `/api/layout/aisles/${encodeURIComponent(aisle)}/${endpoint}`,
      {
        method: 'POST',
        headers: {'Content-Type':'application/json', ...getEditorHeaders()},
        body: JSON.stringify(payload)
      }
    );
    if (res.ok && data?.success) return data;
    return {
      success: false,
      error: (data && data.error) || 'Suppression impossible pour le moment.'
    };
  } catch (error) {
    return {success: false, error: 'Suppression impossible pour le moment (réseau).'};
  }
}

async function apiBulkMoveLayoutProducts(payload) {
  try {
    const {res, data} = await apiFetch('/api/layout/products/bulk-move', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {
      success: false,
      code: data?.code || '',
      error: data?.error || 'Deplacement impossible.'
    };
  } catch (error) {
    return {success: false, error: 'Deplacement impossible pour le moment (reseau).'};
  }
}

async function apiBulkDeleteLayoutProducts(payload) {
  try {
    const {res, data} = await apiFetch('/api/layout/products/bulk-delete', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {
      success: false,
      code: data?.code || '',
      error: data?.error || 'Suppression impossible.'
    };
  } catch (error) {
    return {success: false, error: 'Suppression impossible pour le moment (reseau).'};
  }
}

async function apiReorderLayoutAisles(payload) {
  try {
    const {res, data} = await apiFetch('/api/layout/aisles/reorder', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {
      success: false,
      code: data?.code || '',
      error: data?.error || 'Deplacement des allees impossible.'
    };
  } catch (error) {
    return {success: false, error: 'Deplacement des allees impossible pour le moment (reseau).'};
  }
}

async function apiMoveLayoutStructure(kind, payload) {
  if (kind !== 'section' && kind !== 'shelf') {
    return {success: false, error: 'Type de structure invalide.'};
  }
  try {
    const {res, data} = await apiFetch(`/api/layout/structure/move-${kind}`, {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {
      success: false,
      code: data?.code || '',
      error: data?.error || 'Deplacement impossible.'
    };
  } catch (error) {
    return {success: false, error: 'Deplacement impossible pour le moment (reseau).'};
  }
}

async function apiDeleteLayoutAisle(aisle, payload={}) {
  try {
    const {res, data} = await apiFetch(`/api/layout/aisles/${encodeURIComponent(aisle)}`, {
      method: 'DELETE',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(payload)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur de suppression d allée.'};
  } catch (error) {
    return {success: false, error: 'Impossible de supprimer l’allée pour le moment.'};
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

// Tag AI requests with the current store so training logs are grouped per location.
function _withStore(payload) {
  const store = (typeof getCurrentStoreName === 'function') ? getCurrentStoreName() : '';
  return {...payload, store};
}

async function apiGenerateProductAssist(payload) {
  try {
    const {res, data} = await apiFetch('/api/products/assist', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(_withStore(payload))
    });
    return res.ok ? data : {success: false, error: data.error || 'Aide client indisponible pour le moment.'};
  } catch (error) {
    return {success: false, error: 'Aide client indisponible pour le moment.'};
  }
}

async function apiGenerateClientHelp(payload, signal) {
  try {
    const {res, data} = await apiFetch('/api/client/help', {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(_withStore(payload)),
      signal
    });
    return res.ok ? data : {success: false, error: data.error || 'Reponse client indisponible pour le moment.'};
  } catch (error) {
    return {success: false, error: 'Reponse client indisponible pour le moment.'};
  }
}

async function apiGetProductImages(ids) {
  const cleanIds = [...new Set((ids || []).map(Number).filter(id => Number.isInteger(id) && id > 0))].slice(0, 100);
  if (!cleanIds.length) return {images: {}};
  try {
    const {res, data} = await apiFetch(`/api/products/images?ids=${encodeURIComponent(cleanIds.join(','))}`);
    return res.ok && data && typeof data === 'object' ? data : {images: {}};
  } catch (_) {
    return {images: {}};
  }
}

async function apiGetReferenceProductImages(barcodes) {
  const cleanBarcodes = [...new Set((barcodes || [])
    .map(value => String(value || '').replace(/\D/g, ''))
    .filter(Boolean))].slice(0, 80);
  if (!cleanBarcodes.length) return {images: {}};
  try {
    const {res, data} = await apiFetch(
      `/api/products/reference-images?barcodes=${encodeURIComponent(cleanBarcodes.join(','))}`
    );
    return res.ok && data && typeof data === 'object' ? data : {images: {}};
  } catch (_) {
    return {images: {}};
  }
}

async function apiSetProductStock(id, inStock) {
  try {
    const {res, data} = await apiFetch(`/api/products/${id}/stock`, {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({in_stock: !!inStock})
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur.'};
  } catch (error) {
    return {success: false, error: 'Impossible de changer le statut.'};
  }
}

async function apiSetFlippedLabel(id, flipped, underneath) {
  try {
    const body = {flipped: !!flipped};
    if (underneath !== undefined) body.underneath = String(underneath || '');
    const {res, data} = await apiFetch(`/api/products/${id}/flipped-label`, {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify(body)
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur.'};
  } catch (error) {
    return {success: false, error: 'Impossible de changer l étiquette.'};
  }
}

async function apiSetIsPlano(id, isPlano) {
  try {
    const {res, data} = await apiFetch(`/api/products/${id}/plano`, {
      method: 'POST',
      headers: {'Content-Type':'application/json', ...getEditorHeaders()},
      body: JSON.stringify({is_plano: !!isPlano})
    });
    return res.ok ? data : {success: false, error: data.error || 'Erreur.'};
  } catch (error) {
    return {success: false, error: 'Impossible de changer le statut plano.'};
  }
}

async function apiGetSystemInfo() {
  const {res, data} = await apiFetch('/api/system/info');
  if (!res.ok) throw new Error('system-info');
  return data;
}
