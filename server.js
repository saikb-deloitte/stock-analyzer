// Yahoo Finance CORS proxy — used by frontend for live quote ticks
const http = require('http');
const https = require('https');
const url = require('url');

const PORT = 3001;
let crumb = null;
let cookies = '';

function fetchCrumb() {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: 'finance.yahoo.com',
      path: '/',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
      }
    };
    const req = https.request(opts, res => {
      const raw = res.headers['set-cookie'] || [];
      cookies = raw.map(c => c.split(';')[0]).join('; ');
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        const m = body.match(/"crumb":"([^"]+)"/);
        if (m) { crumb = m[1].replace(/\\u002F/g, '/'); return resolve(crumb); }
        const m2 = body.match(/CrumbStore[^}]*"crumb":"([^"]+)"/);
        if (m2) { crumb = m2[1].replace(/\\u002F/g, '/'); return resolve(crumb); }
        reject(new Error('crumb not found'));
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function yahooGet(path) {
  return new Promise(async (resolve, reject) => {
    if (!crumb) { try { await fetchCrumb(); } catch (e) { return reject(e); } }
    const sep = path.includes('?') ? '&' : '?';
    const opts = {
      hostname: 'query1.finance.yahoo.com',
      path: `${path}${sep}crumb=${encodeURIComponent(crumb)}`,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Cookie': cookies,
      }
    };
    const req = https.request(opts, res => {
      if (res.statusCode === 401) { crumb = null; return reject(new Error('401')); }
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(new Error('parse error')); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json');

  if (req.method === 'OPTIONS') { res.writeHead(200); return res.end(); }

  const { pathname, query } = url.parse(req.url, true);

  try {
    if (pathname.startsWith('/api/chart/')) {
      const ticker = pathname.split('/')[3].toUpperCase();
      const range = query.range || '1y';
      const interval = query.interval || '1d';
      const data = await yahooGet(`/v8/finance/chart/${ticker}?range=${range}&interval=${interval}&includePrePost=false`);
      res.writeHead(200);
      return res.end(JSON.stringify(data));
    }

    if (pathname.startsWith('/api/quote/')) {
      const ticker = pathname.split('/')[3].toUpperCase();
      const modules = 'summaryDetail,financialData,defaultKeyStatistics,assetProfile,price,recommendationTrend';
      const data = await yahooGet(`/v10/finance/quoteSummary/${ticker}?modules=${encodeURIComponent(modules)}`);
      res.writeHead(200);
      return res.end(JSON.stringify(data));
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));
  } catch (err) {
    res.writeHead(500);
    res.end(JSON.stringify({ error: err.message }));
  }
});

fetchCrumb()
  .then(() => {
    server.listen(PORT, () => console.log(`Yahoo Finance proxy on port ${PORT}`));
  })
  .catch(err => {
    console.warn('Crumb pre-warm failed:', err.message);
    server.listen(PORT, () => console.log(`Yahoo Finance proxy on port ${PORT} (lazy crumb)`));
  });
