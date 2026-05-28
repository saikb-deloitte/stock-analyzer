/* Centaur Prism — service worker
 *
 * Strategy: cache-first for the app shell (HTML/CSS/icons),
 * network-only for all /api/* calls (we never want stale stock data).
 *
 * Bump CACHE_VERSION when you ship UI changes so users get a fresh shell.
 */

const CACHE_VERSION = 'cp-shell-v1';
const SHELL_ASSETS = [
  '/',
  '/static/manifest.json',
  '/static/icon.svg',
  '/static/icon-maskable.svg',
];

self.addEventListener('install', (event) => {
  // Pre-cache the app shell so the first offline load works
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  // Cleanup old shell caches when version bumps
  event.waitUntil(
    caches.keys().then((names) =>
      Promise.all(names.filter((n) => n !== CACHE_VERSION).map((n) => caches.delete(n)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // NEVER cache API calls — stock data must always be fresh
  if (url.pathname.startsWith('/api/')) {
    return;  // default = network
  }

  // For everything else (HTML, CSS, icons): cache-first, fall back to network
  if (event.request.method !== 'GET') return;

  event.respondWith(
    caches.match(event.request).then((cached) => {
      if (cached) return cached;
      return fetch(event.request).then((res) => {
        // Cache successful same-origin GETs only
        if (res.ok && url.origin === self.location.origin) {
          const clone = res.clone();
          caches.open(CACHE_VERSION).then((cache) => cache.put(event.request, clone));
        }
        return res;
      }).catch(() => {
        // Offline fallback for navigations: return the cached shell
        if (event.request.mode === 'navigate') {
          return caches.match('/');
        }
      });
    })
  );
});
