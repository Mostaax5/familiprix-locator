const CACHE_NAME = 'familiprix-locator-v28';
const OFFLINE_CACHE = [
  '/',
  '/manifest.json',
  '/static/icon.svg'
];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(OFFLINE_CACHE)));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(names =>
      Promise.all(names.filter(name => name !== CACHE_NAME).map(name => caches.delete(name)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;

  if (url.pathname.startsWith('/api/')) {
    // Pass the request through UNCHANGED: each API call carries its own cache
    // mode ('no-store' by default; 'no-cache' for the big product/layout lists
    // so the browser can revalidate with ETag and reuse its stored copy on 304).
    // Forcing no-store here used to defeat that entirely.
    event.respondWith(fetch(event.request));
    return;
  }

  event.respondWith(staleWhileRevalidate(event.request));
});

// App shell strategy: serve INSTANTLY from the device's copy and refresh it in
// the background (stale-while-revalidate). The old network-first re-downloaded
// every JS/CSS file over the store wifi before showing anything — the single
// biggest part of "the app takes forever to open". Updates land one open later.
// Data stays fresh: /api/* never goes through this cache (see above).
async function staleWhileRevalidate(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  const refresh = fetch(request).then(response => {
    if (response && response.ok && shouldCache(request)) {
      cache.put(request, response.clone());
    }
    return response;
  }).catch(() => null);
  if (cached) return cached;                       // instant paint
  const fresh = await refresh;                     // first-ever visit: network
  if (fresh) return fresh;
  if (request.mode === 'navigate') {
    const fallback = await cache.match('/');
    if (fallback) return fallback;
  }
  return new Response('Hors ligne', {status: 503});
}

function shouldCache(request) {
  const url = new URL(request.url);
  return request.mode === 'navigate' || url.pathname === '/manifest.json' || url.pathname.startsWith('/static/');
}
