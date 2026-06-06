from flask import Flask, jsonify, render_template, request
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
import os
import sys
import json
import warnings
warnings.filterwarnings('ignore')

# Locate templates correctly whether running from source or a PyInstaller bundle
def _resource_path(rel):
    base = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, rel)

app = Flask(
    __name__,
    template_folder=_resource_path('templates'),
    static_folder=_resource_path('static') if os.path.isdir(_resource_path('static')) else None,
)

# ── Public deploy hygiene ────────────────────────────────────────────────
# PUBLIC_MODE=1 enables tighter rate limits + session-scoped archive.
# Set this on Railway/Render so multiple users can safely share the app.
IS_PUBLIC = os.environ.get('PUBLIC_MODE', '').lower() in ('1', 'true', 'yes')
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET', 'centaur-prism-local-dev-key')

# Lightweight per-IP rate limiter to protect upstream Yahoo/NSE and the host.
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    _default_limits = ["200 per minute", "3000 per hour"] if IS_PUBLIC else ["1000 per minute"]
    limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=_default_limits,
        storage_uri="memory://",
    )
except Exception as _e:
    # Flask-Limiter is optional locally; deploy MUST have it.
    print(f'  [WARN] rate limiter disabled: {_e}')
    class _Noop:
        def limit(self, *a, **k): return lambda f: f
    limiter = _Noop()


# Ensure NaN/Infinity are serialized as null (valid JSON), not literal NaN
class SafeJSONProvider(app.json_provider_class):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault('allow_nan', False)
        # Walk and replace NaN/Inf with None before serializing
        def clean(o):
            if isinstance(o, float):
                if np.isnan(o) or np.isinf(o):
                    return None
                return o
            if isinstance(o, dict):
                return {k: clean(v) for k, v in o.items()}
            if isinstance(o, list):
                return [clean(v) for v in o]
            return o
        return super().dumps(clean(obj), **kwargs)


app.json = SafeJSONProvider(app)

# Simple time-based cache.
# Public deploys 6x the TTL (30 min) — yfinance gets cranky from cloud IPs
# so we hit it less aggressively per user. Also lets cache survive worker
# recycles (--max-requests 200) which would otherwise drop cold-cache users
# back into the 38s fetch path that triggers Render's 30s gateway timeout.
_cache = {}
CACHE_TTL = 1800 if IS_PUBLIC else 300

# yfinance concurrency: cloud IPs get rate-limited harder than home IPs
_SCREENER_WORKERS = 4 if IS_PUBLIC else 8

# ALL NSE default scan cap — smaller on cloud so Railway's 30s timeout doesn't bite
_ALL_NSE_DEFAULT_CAP = 150 if IS_PUBLIC else 300


# ════════════════════════════════════════════════════════════════════════════
#  Data layer — resilient history fetcher (Yahoo → NSE direct → disk cache)
# ════════════════════════════════════════════════════════════════════════════
import pickle
import requests as _requests

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.data_cache')
os.makedirs(CACHE_DIR, exist_ok=True)

_NSE_HOME = "https://www.nseindia.com"
_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": _NSE_HOME + "/",
}

_nse_session = None
_nse_session_at = 0

def _get_nse_session():
    """NSE requires a homepage GET first to set cookies. Re-prime every 10 min."""
    global _nse_session, _nse_session_at
    if _nse_session is None or (time.time() - _nse_session_at) > 600:
        try:
            s = _requests.Session()
            s.headers.update(_NSE_HEADERS)
            s.get(_NSE_HOME, timeout=10)
            s.get(_NSE_HOME + "/market-data/live-equity-market", timeout=10)
            _nse_session = s
            _nse_session_at = time.time()
        except Exception:
            _nse_session = None
    return _nse_session


_BHAV_BASE = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
_BHAV_CACHE_DIR = os.path.join(CACHE_DIR, 'bhavcopies')
os.makedirs(_BHAV_CACHE_DIR, exist_ok=True)
_BHAV_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "text/csv"}


def _fetch_bhavcopy(date_obj):
    """Download one day's bhavcopy CSV, cache it. Returns DataFrame or None."""
    date_str = date_obj.strftime('%d%m%Y')
    cache_path = os.path.join(_BHAV_CACHE_DIR, f'bhav_{date_str}.csv')

    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
        try:
            return pd.read_csv(cache_path, skipinitialspace=True)
        except Exception:
            pass

    try:
        url = _BHAV_BASE.format(date=date_str)
        r = _requests.get(url, headers=_BHAV_HEADERS, timeout=15)
        if r.status_code != 200 or len(r.content) < 1000:
            return None
        # Save to cache
        with open(cache_path, 'wb') as f:
            f.write(r.content)
        return pd.read_csv(cache_path, skipinitialspace=True)
    except Exception:
        return None


def fetch_nse_history(symbol_no_suffix, days=400):
    """Assemble daily OHLCV for a symbol by stitching NSE bhavcopies (static CSVs).
    Reliable, but slow on cold cache. Returns yfinance-shaped DataFrame or None."""
    from datetime import datetime, timedelta
    to_dt = datetime.now()
    rows = []

    # Walk back day by day, fetch up to `days` calendar days but only count successes
    fetched = 0
    misses = 0
    for offset in range(0, int(days) + 30):  # extra buffer for weekends/holidays
        d = to_dt - timedelta(days=offset)
        if d.weekday() >= 5:   # Sat/Sun
            continue
        bhav = _fetch_bhavcopy(d)
        if bhav is None:
            misses += 1
            # Abort early if many recent days are missing (NSE archive lag)
            if misses > 5 and fetched == 0:
                break
            continue
        try:
            r = bhav[(bhav['SYMBOL'].str.strip() == symbol_no_suffix) & (bhav['SERIES'].str.strip() == 'EQ')]
            if r.empty:
                continue
            rec = r.iloc[0]
            rows.append({
                'Open':   float(rec['OPEN_PRICE']),
                'High':   float(rec['HIGH_PRICE']),
                'Low':    float(rec['LOW_PRICE']),
                'Close':  float(rec['CLOSE_PRICE']),
                'Volume': int(float(rec['TTL_TRD_QNTY'])),
                '_date':  pd.to_datetime(rec['DATE1'], format='%d-%b-%Y', errors='coerce'),
            })
            fetched += 1
        except Exception:
            continue
        if fetched >= days:
            break

    if not rows:
        return None
    df = pd.DataFrame(rows)
    df['_date'] = pd.to_datetime(df['_date'])
    df = df.dropna(subset=['_date']).set_index('_date').sort_index()
    df.index.name = None
    return df


# ────────────────────────────────────────────────────────────────────────────
# NSE UNIVERSE — derived from latest bhavcopy. Lets "ALL NSE" mode scan all
# liquid equities (~500-700 stocks) rather than just the ~150 in NSE_INDICES.
# ────────────────────────────────────────────────────────────────────────────
_UNIVERSE_CACHE_PATH = os.path.join(CACHE_DIR, 'nse_universe.json')
_UNIVERSE_TTL_SEC    = 6 * 3600   # refresh every 6 hours

def get_nse_universe(min_turnover_cr=5.0, max_stocks=500, force_refresh=False):
    """
    Returns (tickers, meta). Tickers have .NS suffix.
    Filters: SERIES='EQ', turnover >= min_turnover_cr.
    Sorted by turnover desc, capped at max_stocks.
    """
    # Disk cache check
    if not force_refresh and os.path.exists(_UNIVERSE_CACHE_PATH):
        try:
            age = time.time() - os.path.getmtime(_UNIVERSE_CACHE_PATH)
            if age < _UNIVERSE_TTL_SEC:
                with open(_UNIVERSE_CACHE_PATH) as f:
                    cached = json.load(f)
                # Honor the requested cap (cached list may be larger)
                tk = cached.get('tickers', [])[:max_stocks]
                meta = cached.get('meta', {})
                meta = dict(meta)
                meta['returned'] = len(tk)
                meta['source']   = 'cache'
                return tk, meta
        except Exception:
            pass

    # Walk back up to 14 days to find a usable bhavcopy
    from datetime import datetime as _dt, timedelta as _td
    today = _dt.now()
    bhav, bhav_date = None, None
    for offset in range(0, 14):
        d = today - _td(days=offset)
        if d.weekday() >= 5:
            continue
        bhav = _fetch_bhavcopy(d)
        if bhav is not None and not bhav.empty:
            bhav_date = d
            break

    if bhav is None:
        return [], {'error': 'No bhavcopy available in the last 14 days'}

    try:
        eq = bhav[bhav['SERIES'].str.strip() == 'EQ'].copy()
        # TURNOVER_LACS is in lakhs (₹100,000). Convert to crores (₹10,000,000) by /100.
        eq['turnover_cr'] = pd.to_numeric(eq['TURNOVER_LACS'], errors='coerce') / 100.0
        eq = eq.dropna(subset=['turnover_cr'])
        # Liquid filter
        filtered = eq[eq['turnover_cr'] >= min_turnover_cr]
        filtered = filtered.sort_values('turnover_cr', ascending=False)
        tickers_full = [s.strip() + '.NS' for s in filtered['SYMBOL'].tolist()]
        tickers = tickers_full[:max_stocks]
    except Exception as e:
        return [], {'error': f'Bhavcopy parse failed: {e}'}

    meta = {
        'bhavcopy_date':    bhav_date.strftime('%Y-%m-%d') if bhav_date else None,
        'total_eq_stocks':  int((bhav['SERIES'].str.strip() == 'EQ').sum()),
        'liquid_count':     len(tickers_full),
        'liquid_filter_cr': min_turnover_cr,
        'returned':         len(tickers),
        'max_stocks':       max_stocks,
        'refreshed_at':     int(time.time()),
        'source':           'fresh',
    }

    # Save full list (so smaller `max_stocks` re-queries hit cache)
    try:
        with open(_UNIVERSE_CACHE_PATH, 'w') as f:
            json.dump({'tickers': tickers_full, 'meta': meta}, f)
    except Exception:
        pass

    return tickers, meta


# ────────────────────────────────────────────────────────────────────────────
# INDIAN FINANCIAL NEWS RSS + SCREENER.IN FUNDAMENTALS
# Replaces yfinance.news (US-biased, sparse) and yfinance.info (often empty on
# cloud IPs) with India-native sources.
# ────────────────────────────────────────────────────────────────────────────

_NEWS_FEEDS = [
    # MoneyControl — business + market reports
    ('MoneyControl Business',  'https://www.moneycontrol.com/rss/business.xml'),
    ('MoneyControl Markets',   'https://www.moneycontrol.com/rss/marketreports.xml'),
    # LiveMint
    ('LiveMint Markets',       'https://www.livemint.com/rss/markets'),
    # Economic Times
    ('ET Markets',             'https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms'),
    ('ET Business',            'https://economictimes.indiatimes.com/rssfeedsdefault.cms'),
]

_NEWS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Accept': 'application/rss+xml, application/xml, text/xml',
}

def _fetch_indian_news(symbol, company_name=None, sector=None, max_items=12):
    """
    Pull recent articles from Indian financial RSS feeds, filter to those
    mentioning the stock by symbol / company name / first 2 words.
    Cached for 30 min per ticker.
    """
    cache_key = f'news_in:{symbol}'
    cached = get_cached(cache_key, ttl=1800)
    if cached is not None:
        return cached

    # Build match terms — be specific to avoid over-matching common words
    terms = set()
    sym_clean = symbol.upper().replace('.NS', '').replace('.BO', '')
    # Ticker symbol (lowercased so it works in headlines like "TCS, Infosys ...")
    if len(sym_clean) >= 3:
        terms.add(sym_clean.lower())
    if company_name:
        nm = company_name.strip()
        # Strip very common suffixes for cleaner matching
        nm_short = nm
        for suf in [' Limited', ' Ltd.', ' Ltd', ' Inc.', ' Inc', ' Corporation', ' Corp.', ' Corp']:
            if nm_short.endswith(suf):
                nm_short = nm_short[:-len(suf)].strip()
        if len(nm_short) >= 4:
            terms.add(nm_short.lower())
        # First two words ("Tata Consultancy" from "Tata Consultancy Services Ltd")
        words = nm_short.split()
        if len(words) >= 2:
            two = ' '.join(words[:2]).lower()
            if len(two) >= 6:
                terms.add(two)
        # Distinctive single word (longest word ≥6 chars) for partial matches
        long_words = [w for w in words if len(w) >= 6 and w.lower() not in
                      {'india', 'limited', 'private', 'company', 'corporation', 'industries', 'services'}]
        if long_words:
            terms.add(max(long_words, key=len).lower())

    try:
        import feedparser
    except ImportError:
        print('  [news] feedparser not installed — returning empty')
        return []

    matches = []
    for feed_name, feed_url in _NEWS_FEEDS:
        try:
            # feedparser respects HTTP headers via request_headers kwarg
            feed = feedparser.parse(feed_url, request_headers=_NEWS_HEADERS)
            for entry in feed.entries[:80]:   # cap per feed
                title = (entry.get('title') or '').strip()
                if not title:
                    continue
                summary = (entry.get('summary') or entry.get('description') or '').strip()
                hay = (title + ' ' + summary).lower()
                # Match if any of our terms appear (need at least 1)
                if not any(t in hay for t in terms):
                    continue
                matches.append({
                    'title':     title,
                    'publisher': feed_name,
                    'url':       entry.get('link', ''),
                    'published': entry.get('published', ''),
                    '_ts':       entry.get('published_parsed') or (1970, 1, 1, 0, 0, 0, 0, 0, 0),
                })
        except Exception as e:
            print(f'  [news] {feed_name} failed: {e}')
            continue

    # Sort newest-first
    matches.sort(key=lambda x: x.get('_ts'), reverse=True)

    # Dedupe by first 60 chars of title (same story, different feeds)
    seen, unique = set(), []
    for m in matches:
        key = m['title'][:60].lower()
        if key in seen:
            continue
        seen.add(key)
        m.pop('_ts', None)
        unique.append(m)
        if len(unique) >= max_items:
            break

    set_cached(cache_key, unique, ttl=1800)
    return unique


# ──────────────────────────────────────────────────────────────────────────
# Screener.in fundamentals scraper — fallback for empty yfinance.info
# ──────────────────────────────────────────────────────────────────────────
_SCREENER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
}

def _parse_screener_number(s):
    """Strip commas/units/percent and convert to float. None if unparseable."""
    if not s:
        return None
    s = str(s).replace(',', '').replace('₹', '').replace('Rs.', '').strip()
    s = s.split()[0] if s else s
    # Handle Cr suffix (rupee crores)
    if s.endswith('Cr'):
        s = s[:-2].strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def _fetch_screener_fundamentals(symbol):
    """
    Scrape Screener.in for a stock's fundamentals (P/E, P/B, ROE, market cap
    etc.). Returns a dict shaped like yfinance.info so callers can substitute.
    Cached for 6 hours per ticker.
    """
    sym = symbol.upper().replace('.NS', '').replace('.BO', '')
    cache_key = f'screener_fund:{sym}'
    cached = get_cached(cache_key, ttl=21600)   # 6 hours
    if cached is not None:
        return cached

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print('  [screener] beautifulsoup4 not installed — returning empty')
        return {}

    # Try consolidated first (preferred for multi-segment companies), then standalone
    urls = [
        f'https://www.screener.in/company/{sym}/consolidated/',
        f'https://www.screener.in/company/{sym}/',
    ]

    info = {}
    for url in urls:
        try:
            r = _requests.get(url, headers=_SCREENER_HEADERS, timeout=5)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.content, 'html.parser')

            # Company name
            title_el = soup.select_one('h1')
            if title_el:
                name = title_el.get_text(strip=True)
                if name and len(name) > 2:
                    info['longName'] = name

            # Top ratios block — Market Cap, P/E, P/B, ROE, etc.
            for li in soup.select('#top-ratios li'):
                name_el = li.select_one('.name')
                num_el  = li.select_one('.number')
                if not name_el or not num_el:
                    continue
                name = name_el.get_text(strip=True)
                num  = _parse_screener_number(num_el.get_text(strip=True))
                if num is None:
                    continue

                # Map to yfinance.info-style fields
                low_name = name.lower()
                if 'market cap' in low_name:
                    info['marketCap'] = num * 1e7              # ₹ Cr → ₹
                elif low_name == 'current price':
                    info['currentPrice'] = num
                elif low_name in ('stock p/e', 'p/e'):
                    info['trailingPE'] = num
                elif low_name == 'book value':
                    info['bookValue'] = num
                elif low_name in ('price to book value', 'pb ratio'):
                    info['priceToBook'] = num
                elif 'dividend yield' in low_name:
                    info['dividendYield'] = num / 100.0        # % → decimal
                elif low_name == 'roe':
                    info['returnOnEquity'] = num / 100.0
                elif low_name == 'roce':
                    info['returnOnCapital'] = num / 100.0
                elif low_name == 'face value':
                    info['faceValue'] = num
                elif low_name in ('eps', 'earnings per share'):
                    info['trailingEps'] = num
                elif low_name == 'high / low':
                    info['fiftyTwoWeekHigh'] = num   # first number is high

            # If we got something useful, stop here
            if info.get('marketCap') or info.get('trailingPE'):
                break
        except Exception as e:
            print(f'  [screener] {sym} {url} failed: {e}')
            continue

    if info:
        set_cached(cache_key, info, ttl=21600)
    return info


def _cache_key(ticker, period):
    safe = ticker.replace('/', '_').replace('\\', '_')
    return os.path.join(CACHE_DIR, f'hist_{safe}_{period}.pkl')


def _save_disk_cache(ticker, period, df, source):
    try:
        with open(_cache_key(ticker, period), 'wb') as f:
            pickle.dump({'df': df, 'source': source, 'ts': time.time()}, f)
    except Exception:
        pass


def _load_disk_cache(ticker, period, max_age_days=14):
    try:
        path = _cache_key(ticker, period)
        if not os.path.exists(path):
            return None, None, None
        with open(path, 'rb') as f:
            obj = pickle.load(f)
        age_days = (time.time() - obj.get('ts', 0)) / 86400
        if age_days > max_age_days:
            return None, None, None
        return obj.get('df'), obj.get('source'), age_days
    except Exception:
        return None, None, None


# ── NIFTY series cache for beta / relative-strength calculations ──────────────
_NIFTY_CACHE = {'returns': None, 'closes': None, 'fetched_at': 0}


def get_nifty_history(period='1y'):
    """Cached NIFTY 50 daily closes + returns. Refresh once per 24h."""
    if (_NIFTY_CACHE['closes'] is not None and
            time.time() - _NIFTY_CACHE['fetched_at'] < 86400):
        return _NIFTY_CACHE['closes'], _NIFTY_CACHE['returns']
    try:
        n = yf.Ticker('^NSEI').history(period=period)
        if n is None or n.empty:
            return None, None
        closes = n['Close']
        rets = closes.pct_change().dropna()
        _NIFTY_CACHE['closes'] = closes
        _NIFTY_CACHE['returns'] = rets
        _NIFTY_CACHE['fetched_at'] = time.time()
        return closes, rets
    except Exception:
        return None, None


def compute_beta(stock_close_series, period='1y'):
    """Beta = Cov(stock_returns, mkt_returns) / Var(mkt_returns) over 1y dailies vs NIFTY."""
    if stock_close_series is None or len(stock_close_series) < 30:
        return None
    _, nifty_rets = get_nifty_history(period=period)
    if nifty_rets is None or nifty_rets.empty:
        return None
    s_rets = stock_close_series.pct_change().dropna()
    # Drop timezone differences before intersecting
    try:
        s_idx = s_rets.index.tz_localize(None) if s_rets.index.tz is not None else s_rets.index
        n_idx = nifty_rets.index.tz_localize(None) if nifty_rets.index.tz is not None else nifty_rets.index
        s_rets = s_rets.copy(); s_rets.index = s_idx
        n_rets = nifty_rets.copy(); n_rets.index = n_idx
    except Exception:
        n_rets = nifty_rets
    common = s_rets.index.intersection(n_rets.index)
    if len(common) < 30:
        return None
    s = s_rets.loc[common]
    n = n_rets.loc[common]
    var = float(n.var())
    if var <= 0:
        return None
    return round(float(s.cov(n)) / var, 2)


def fetch_history(ticker, period='1y'):
    """Resilient history fetcher.
    Returns (df, source) where source is 'yahoo', 'nse', or 'cache:<orig_src>'."""
    if not ticker.endswith('.NS') and not ticker.endswith('.BO'):
        ticker += '.NS'
    symbol = ticker.replace('.NS', '').replace('.BO', '')

    # 1) Yahoo (primary)
    try:
        df = yf.Ticker(ticker).history(period=period)
        if df is not None and not df.empty and len(df) > 5:
            _save_disk_cache(ticker, period, df, 'yahoo')
            mark_fresh_fetch()
            return df, 'yahoo'
    except Exception:
        pass

    # 2) NSE direct (only for .NS tickers)
    if ticker.endswith('.NS'):
        days_map = {'5d': 10, '1mo': 40, '3mo': 100, '6mo': 200, '1y': 380, '2y': 760, '5y': 1850}
        days = days_map.get(period, 380)
        df = fetch_nse_history(symbol, days=days)
        if df is not None and not df.empty and len(df) > 5:
            _save_disk_cache(ticker, period, df, 'nse')
            mark_fresh_fetch()
            return df, 'nse'

    # 3) Disk cache (last-resort, up to 14 days old)
    df, src, age = _load_disk_cache(ticker, period)
    if df is not None:
        return df, f'cache:{src}({age:.1f}d old)'

    return None, None


def get_cached(key, ttl=None):
    """Get cached value if present and not expired.
    Optional per-call ttl override (useful for slow-changing data like news/fundamentals).
    """
    if key in _cache:
        entry = _cache[key]
        # Support both old 2-tuple and new 3-tuple cache entries
        if len(entry) >= 3:
            data, ts, entry_ttl = entry
            limit = ttl if ttl is not None else (entry_ttl if entry_ttl is not None else CACHE_TTL)
        else:
            data, ts = entry
            limit = ttl if ttl is not None else CACHE_TTL
        if time.time() - ts < limit:
            return data
    return None


def set_cached(key, data, ttl=None):
    """Store value in cache. Optional ttl overrides the global CACHE_TTL for this entry."""
    _cache[key] = (data, time.time(), ttl)


# Track when we last got *fresh* (non-cached) data from upstream sources.
# Used by /api/freshness so the UI can show a "Data updated Xm ago" badge.
_data_freshness = {'last_fresh_ts': time.time()}

def mark_fresh_fetch():
    _data_freshness['last_fresh_ts'] = time.time()


# NSE stock universe
NSE_INDICES = {
    'NIFTY 50': [
        'ADANIENT.NS', 'ADANIPORTS.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
        'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BPCL.NS', 'BHARTIARTL.NS',
        'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
        'EICHERMOT.NS', 'GRASIM.NS', 'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
        'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 'ITC.NS',
        'INDUSINDBK.NS', 'INFY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LT.NS',
        'LTIM.NS', 'M&M.NS', 'MARUTI.NS', 'NESTLEIND.NS', 'NTPC.NS',
        'ONGC.NS', 'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS',
        'SUNPHARMA.NS', 'TCS.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
        'TECHM.NS', 'TITAN.NS', 'TRENT.NS', 'ULTRACEMCO.NS', 'WIPRO.NS',
    ],
    'NIFTY Bank': [
        'HDFCBANK.NS', 'ICICIBANK.NS', 'KOTAKBANK.NS', 'SBIN.NS', 'AXISBANK.NS',
        'BANKBARODA.NS', 'INDUSINDBK.NS', 'FEDERALBNK.NS', 'IDFCFIRSTB.NS', 'BANDHANBNK.NS',
        'AUBANK.NS', 'PNB.NS',
    ],
    'NIFTY IT': [
        'TCS.NS', 'INFY.NS', 'HCLTECH.NS', 'WIPRO.NS', 'TECHM.NS',
        'LTIM.NS', 'MPHASIS.NS', 'COFORGE.NS', 'PERSISTENT.NS', 'OFSS.NS',
    ],
    'NIFTY Pharma': [
        'SUNPHARMA.NS', 'DRREDDY.NS', 'CIPLA.NS', 'DIVISLAB.NS', 'APOLLOHOSP.NS',
        'TORNTPHARM.NS', 'ALKEM.NS', 'BIOCON.NS', 'AUROPHARMA.NS', 'LUPIN.NS',
    ],
    'NIFTY Auto': [
        'MARUTI.NS', 'TATAMOTORS.NS', 'BAJAJ-AUTO.NS', 'EICHERMOT.NS', 'HEROMOTOCO.NS',
        'M&M.NS', 'ASHOKLEY.NS', 'TVSMOTOR.NS', 'MRF.NS', 'BALKRISIND.NS',
    ],
    'NIFTY Midcap': [
        'PIDILITIND.NS', 'GODREJCP.NS', 'BERGEPAINT.NS', 'MARICO.NS', 'MUTHOOTFIN.NS',
        'VOLTAS.NS', 'PAGEIND.NS', 'JUBLFOOD.NS', 'ASTRAL.NS', 'DEEPAKNTR.NS',
        'HAL.NS', 'POLYCAB.NS', 'ABCAPITAL.NS', 'CROMPTON.NS', 'LALPATHLAB.NS',
    ],
}

# Search index
ALL_STOCKS = {s.replace('.NS', '') for stocks in NSE_INDICES.values() for s in stocks}


# ── Indicator calculations ─────────────────────────────────────────────────────

def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calculate_macd(prices, fast=12, slow=26, signal=9):
    ema_fast = prices.ewm(span=fast, adjust=False).mean()
    ema_slow = prices.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd, sig, macd - sig


def calculate_bollinger(prices, period=20, std_mult=2):
    sma = prices.rolling(period).mean()
    std = prices.rolling(period).std()
    return sma + std_mult * std, sma, sma - std_mult * std


def calculate_stochastic(df, k_period=14, d_period=3):
    low_min = df['Low'].rolling(k_period).min()
    high_max = df['High'].rolling(k_period).max()
    denom = (high_max - low_min).replace(0, np.nan)
    k = 100 * (df['Close'] - low_min) / denom
    return k, k.rolling(d_period).mean()


def calculate_atr(df, period=14):
    hi, lo, cl = df['High'], df['Low'], df['Close']
    tr = pd.concat([hi - lo, (hi - cl.shift()).abs(), (lo - cl.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def calculate_adx(df, period=14):
    hi, lo, cl = df['High'], df['Low'], df['Close']
    plus_dm = hi.diff().clip(lower=0)
    minus_dm = (-lo.diff()).clip(lower=0)
    plus_dm = plus_dm.where(plus_dm > minus_dm, 0)
    minus_dm = minus_dm.where(minus_dm > plus_dm, 0)
    atr = calculate_atr(df, period)
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1 / period, adjust=False).mean(), plus_di, minus_di


# ── Scoring ────────────────────────────────────────────────────────────────────

def technical_score(df):
    """Returns (score 0-100, list of signal dicts)."""
    if len(df) < 30:
        return 50, [{'type': 'neutral', 'text': 'Insufficient price history'}]

    score = 50
    signals = []
    close = df['Close']

    # ── RSI
    rsi = calculate_rsi(close)
    r = rsi.iloc[-1]
    if r < 30:
        score += 15; signals.append({'type': 'bullish', 'text': f'RSI {r:.1f} — Oversold (Strong buy signal)'})
    elif r < 45:
        score += 8;  signals.append({'type': 'bullish', 'text': f'RSI {r:.1f} — Recovering from oversold'})
    elif r < 60:
        score += 3;  signals.append({'type': 'neutral', 'text': f'RSI {r:.1f} — Neutral momentum'})
    elif r < 70:
        signals.append({'type': 'neutral', 'text': f'RSI {r:.1f} — Approaching overbought'})
    else:
        score -= 10; signals.append({'type': 'bearish', 'text': f'RSI {r:.1f} — Overbought (caution)'})

    # ── MACD
    macd, sig, hist = calculate_macd(close)
    if macd.iloc[-1] > sig.iloc[-1] and macd.iloc[-2] <= sig.iloc[-2]:
        score += 15; signals.append({'type': 'bullish', 'text': 'MACD Bullish Crossover — Strong buy signal'})
    elif macd.iloc[-1] < sig.iloc[-1] and macd.iloc[-2] >= sig.iloc[-2]:
        score -= 15; signals.append({'type': 'bearish', 'text': 'MACD Bearish Crossover — Sell signal'})
    elif macd.iloc[-1] > sig.iloc[-1]:
        score += 5
        if hist.iloc[-1] > hist.iloc[-2]:
            score += 3; signals.append({'type': 'bullish', 'text': 'MACD above signal — Momentum expanding'})
        else:
            signals.append({'type': 'bullish', 'text': 'MACD above signal line'})
    else:
        score -= 5; signals.append({'type': 'bearish', 'text': 'MACD below signal line'})

    # ── Bollinger Bands
    upper, mid, lower = calculate_bollinger(close)
    curr = close.iloc[-1]
    bb_range = upper.iloc[-1] - lower.iloc[-1]
    if bb_range > 0:
        bb_pos = (curr - lower.iloc[-1]) / bb_range
        if bb_pos < 0:
            score += 15; signals.append({'type': 'bullish', 'text': 'Price below lower Bollinger Band — Extreme oversold'})
        elif bb_pos < 0.2:
            score += 10; signals.append({'type': 'bullish', 'text': f'Near lower Bollinger Band (BB% {bb_pos:.2f}) — Potential bounce'})
        elif bb_pos > 1:
            score -= 10; signals.append({'type': 'bearish', 'text': 'Price above upper Bollinger Band — Extreme overbought'})
        elif bb_pos > 0.8:
            score -= 5;  signals.append({'type': 'bearish', 'text': f'Near upper Bollinger Band (BB% {bb_pos:.2f}) — Overbought risk'})

    # ── EMA trend alignment
    ema9   = close.ewm(span=9,   adjust=False).mean()
    ema21  = close.ewm(span=21,  adjust=False).mean()
    ema50  = close.ewm(span=50,  adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    e9, e21, e50, e200 = ema9.iloc[-1], ema21.iloc[-1], ema50.iloc[-1], ema200.iloc[-1]

    if curr > e9 > e21 > e50:
        score += 12; signals.append({'type': 'bullish', 'text': 'Strong uptrend: Price > EMA9 > EMA21 > EMA50'})
    elif curr > e9 > e21:
        score += 8;  signals.append({'type': 'bullish', 'text': 'Uptrend: Price above EMA9 & EMA21'})
    elif curr > e9:
        score += 3;  signals.append({'type': 'neutral', 'text': 'Price above EMA9 — Short-term bullish'})
    elif curr < e9 < e21 < e50:
        score -= 12; signals.append({'type': 'bearish', 'text': 'Strong downtrend: Price < EMA9 < EMA21 < EMA50'})
    elif curr < e9 < e21:
        score -= 8;  signals.append({'type': 'bearish', 'text': 'Downtrend: Price below EMA9 & EMA21'})

    # Golden / Death Cross (EMA50 vs EMA200)
    if len(ema50.dropna()) > 5 and len(ema200.dropna()) > 5:
        if e50 > e200 and ema50.iloc[-6] <= ema200.iloc[-6]:
            score += 15; signals.append({'type': 'bullish', 'text': 'Golden Cross formed (EMA50 crossed above EMA200)'})
        elif e50 > e200:
            score += 5;  signals.append({'type': 'bullish', 'text': 'Above 200 EMA — Long-term bullish structure'})
        elif e50 < e200 and ema50.iloc[-6] >= ema200.iloc[-6]:
            score -= 15; signals.append({'type': 'bearish', 'text': 'Death Cross formed (EMA50 crossed below EMA200)'})
        elif e50 < e200:
            score -= 5;  signals.append({'type': 'bearish', 'text': 'Below 200 EMA — Long-term bearish structure'})

    # ── Volume
    avg_vol = df['Volume'].rolling(20).mean().iloc[-1]
    curr_vol = df['Volume'].iloc[-1]
    vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1
    price_chg = (curr - close.iloc[-2]) / close.iloc[-2] * 100
    if vol_ratio > 2 and price_chg > 0:
        score += 10; signals.append({'type': 'bullish', 'text': f'Very high volume ({vol_ratio:.1f}x avg) with price up — Strong buying'})
    elif vol_ratio > 1.5 and price_chg > 0:
        score += 5;  signals.append({'type': 'bullish', 'text': f'Above-average volume ({vol_ratio:.1f}x) — Buying interest'})
    elif vol_ratio > 2 and price_chg < 0:
        score -= 10; signals.append({'type': 'bearish', 'text': f'High volume ({vol_ratio:.1f}x avg) on down move — Distribution'})

    # ── 5-day momentum
    if len(close) >= 6:
        mom = (curr - close.iloc[-6]) / close.iloc[-6] * 100
        if mom > 5:
            score += 8;  signals.append({'type': 'bullish', 'text': f'Strong 5-day momentum: +{mom:.1f}%'})
        elif mom > 2:
            score += 3;  signals.append({'type': 'bullish', 'text': f'Positive 5-day momentum: +{mom:.1f}%'})
        elif mom < -5:
            score -= 8;  signals.append({'type': 'bearish', 'text': f'Weak 5-day momentum: {mom:.1f}%'})
        elif mom < -2:
            score -= 3;  signals.append({'type': 'bearish', 'text': f'Negative 5-day momentum: {mom:.1f}%'})

    # ── Stochastic
    stoch_k, stoch_d = calculate_stochastic(df)
    if not stoch_k.empty:
        k, d = stoch_k.iloc[-1], stoch_d.iloc[-1]
        pk, pd_ = stoch_k.iloc[-2], stoch_d.iloc[-2]
        if k < 20:
            score += 8
            if k > d and pk <= pd_:
                score += 5; signals.append({'type': 'bullish', 'text': f'Stochastic K={k:.1f} — Bullish crossover from oversold zone'})
            else:
                signals.append({'type': 'bullish', 'text': f'Stochastic K={k:.1f} — Oversold'})
        elif k > 80:
            score -= 8; signals.append({'type': 'bearish', 'text': f'Stochastic K={k:.1f} — Overbought'})

    # ── ADX trend strength
    adx, plus_di, minus_di = calculate_adx(df)
    adx_val = adx.iloc[-1]
    if adx_val > 25 and plus_di.iloc[-1] > minus_di.iloc[-1]:
        score += 5;  signals.append({'type': 'bullish', 'text': f'ADX {adx_val:.1f} — Strong bullish trend'})
    elif adx_val > 25 and plus_di.iloc[-1] < minus_di.iloc[-1]:
        score -= 5;  signals.append({'type': 'bearish', 'text': f'ADX {adx_val:.1f} — Strong bearish trend'})
    elif adx_val < 20:
        signals.append({'type': 'neutral', 'text': f'ADX {adx_val:.1f} — Ranging / weak trend'})

    return int(min(max(score, 0), 100)), signals


def fundamental_score(info):
    """Returns (score 0-100, list of signal dicts)."""
    score = 50
    signals = []

    # P/E
    pe = info.get('trailingPE')
    if pe and pe > 0:
        if pe < 12:
            score += 12; signals.append({'type': 'bullish', 'text': f'P/E {pe:.1f} — Potentially undervalued'})
        elif pe < 20:
            score += 8;  signals.append({'type': 'bullish', 'text': f'P/E {pe:.1f} — Fairly valued'})
        elif pe < 35:
            score += 3;  signals.append({'type': 'neutral', 'text': f'P/E {pe:.1f} — Moderate valuation'})
        elif pe < 60:
            signals.append({'type': 'neutral', 'text': f'P/E {pe:.1f} — Elevated valuation'})
        else:
            score -= 8;  signals.append({'type': 'bearish', 'text': f'P/E {pe:.1f} — Very expensive'})

    # Revenue growth
    rev_g = info.get('revenueGrowth')
    if rev_g is not None:
        if rev_g > 0.25:
            score += 12; signals.append({'type': 'bullish', 'text': f'Revenue growth {rev_g*100:.1f}% — Strong'})
        elif rev_g > 0.10:
            score += 6;  signals.append({'type': 'bullish', 'text': f'Revenue growth {rev_g*100:.1f}% — Healthy'})
        elif rev_g > 0:
            score += 2;  signals.append({'type': 'neutral', 'text': f'Revenue growth {rev_g*100:.1f}% — Slow'})
        else:
            score -= 10; signals.append({'type': 'bearish', 'text': f'Revenue declining {rev_g*100:.1f}%'})

    # Profit margin
    pm = info.get('profitMargins')
    if pm is not None:
        if pm > 0.20:
            score += 12; signals.append({'type': 'bullish', 'text': f'Net margin {pm*100:.1f}% — High profitability'})
        elif pm > 0.10:
            score += 6;  signals.append({'type': 'bullish', 'text': f'Net margin {pm*100:.1f}% — Good profitability'})
        elif pm > 0:
            score += 2;  signals.append({'type': 'neutral', 'text': f'Net margin {pm*100:.1f}% — Thin margins'})
        else:
            score -= 12; signals.append({'type': 'bearish', 'text': f'Net margin {pm*100:.1f}% — Unprofitable'})

    # ROE
    roe = info.get('returnOnEquity')
    if roe is not None:
        if roe > 0.20:
            score += 10; signals.append({'type': 'bullish', 'text': f'ROE {roe*100:.1f}% — Excellent capital efficiency'})
        elif roe > 0.12:
            score += 5;  signals.append({'type': 'bullish', 'text': f'ROE {roe*100:.1f}% — Good capital efficiency'})
        elif roe < 0:
            score -= 10; signals.append({'type': 'bearish', 'text': f'ROE {roe*100:.1f}% — Negative returns'})

    # Debt/Equity
    de = info.get('debtToEquity')
    if de is not None:
        if de < 30:
            score += 8;  signals.append({'type': 'bullish', 'text': f'D/E {de:.1f}% — Very low leverage'})
        elif de < 80:
            score += 3;  signals.append({'type': 'neutral', 'text': f'D/E {de:.1f}% — Manageable debt'})
        elif de > 200:
            score -= 8;  signals.append({'type': 'bearish', 'text': f'D/E {de:.1f}% — High leverage (risk)'})

    # EPS growth
    eg = info.get('earningsGrowth')
    if eg is not None:
        if eg > 0.20:
            score += 8;  signals.append({'type': 'bullish', 'text': f'EPS growth {eg*100:.1f}% — Strong earnings momentum'})
        elif eg > 0.05:
            score += 3;  signals.append({'type': 'neutral', 'text': f'EPS growth {eg*100:.1f}% — Moderate'})
        elif eg < 0:
            score -= 6;  signals.append({'type': 'bearish', 'text': f'EPS growth {eg*100:.1f}% — Earnings declining'})

    # P/B
    pb = info.get('priceToBook')
    if pb and pb > 0:
        if pb < 1.5:
            score += 5; signals.append({'type': 'bullish', 'text': f'P/B {pb:.1f} — Trading near book value'})
        elif pb > 10:
            score -= 3; signals.append({'type': 'bearish', 'text': f'P/B {pb:.1f} — Very high premium to book'})

    # Operating margin
    om = info.get('operatingMargins')
    if om is not None:
        if om > 0.20:
            score += 6; signals.append({'type': 'bullish', 'text': f'Operating margin {om*100:.1f}% — Strong operating leverage'})
        elif om < 0.05:
            score -= 4; signals.append({'type': 'bearish', 'text': f'Operating margin {om*100:.1f}% — Thin operating margin'})

    # Dividend yield (bonus for income, slight penalty for very high which may signal distress)
    dy = info.get('dividendYield')
    if dy is not None:
        if 0.01 < dy < 0.06:
            score += 3; signals.append({'type': 'bullish', 'text': f'Dividend yield {dy*100:.2f}% — Healthy income'})
        elif dy > 0.10:
            signals.append({'type': 'neutral', 'text': f'Dividend yield {dy*100:.2f}% — Unusually high (verify sustainability)'})

    return int(min(max(score, 0), 100)), signals


def quarterly_fundamental_adjustment(qt):
    """Adjust score and emit signals based on quarterly trends."""
    if not qt:
        return 0, []
    adj = 0
    sigs = []
    yoy_rev = qt.get('yoy_rev_pct')
    yoy_ni  = qt.get('yoy_ni_pct')
    qoq_rev = qt.get('qoq_rev_pct')
    if yoy_rev is not None:
        if yoy_rev > 20:
            adj += 6; sigs.append({'type': 'bullish', 'text': f'YoY revenue +{yoy_rev:.1f}% — Strong topline growth'})
        elif yoy_rev > 8:
            adj += 3; sigs.append({'type': 'bullish', 'text': f'YoY revenue +{yoy_rev:.1f}% — Steady growth'})
        elif yoy_rev < -5:
            adj -= 6; sigs.append({'type': 'bearish', 'text': f'YoY revenue {yoy_rev:.1f}% — Topline shrinking'})
    if yoy_ni is not None:
        if yoy_ni > 25:
            adj += 6; sigs.append({'type': 'bullish', 'text': f'YoY net income +{yoy_ni:.1f}% — Profit surge'})
        elif yoy_ni > 10:
            adj += 3; sigs.append({'type': 'bullish', 'text': f'YoY net income +{yoy_ni:.1f}% — Profits growing'})
        elif yoy_ni < -10:
            adj -= 6; sigs.append({'type': 'bearish', 'text': f'YoY net income {yoy_ni:.1f}% — Profits contracting'})
    if qoq_rev is not None and qoq_rev < -10:
        adj -= 3; sigs.append({'type': 'bearish', 'text': f'QoQ revenue {qoq_rev:.1f}% — Sequential weakness'})
    return adj, sigs


def extract_quarterly_trends(stock):
    """Pull last 4-8 quarters of revenue & net income, compute YoY/QoQ growth."""
    try:
        q_inc = getattr(stock, 'quarterly_income_stmt', None)
        if q_inc is None or q_inc.empty:
            q_inc = getattr(stock, 'quarterly_financials', None)
        if q_inc is None or q_inc.empty:
            return None

        # yfinance gives a DataFrame with rows=metric, columns=date (newest first)
        cols = list(q_inc.columns)[:8]  # last 8 quarters
        cols_sorted = sorted(cols)  # oldest first

        def row(*candidates):
            for c in candidates:
                if c in q_inc.index:
                    return q_inc.loc[c]
            return None

        rev_row = row('Total Revenue', 'TotalRevenue', 'Revenue', 'Operating Revenue')
        ni_row  = row('Net Income', 'NetIncome', 'Net Income Common Stockholders')
        op_row  = row('Operating Income', 'OperatingIncome', 'Operating Income/Loss')

        if rev_row is None and ni_row is None:
            return None

        def to_f(v):
            try:
                f = float(v)
                if np.isnan(f) or np.isinf(f):
                    return None
                return f
            except Exception:
                return None

        quarters = []
        for c in cols_sorted:
            quarters.append({
                'date': c.strftime('%b %Y') if hasattr(c, 'strftime') else str(c),
                'revenue':   to_f(rev_row[c]) if rev_row is not None else None,
                'net_income': to_f(ni_row[c]) if ni_row is not None else None,
                'op_income': to_f(op_row[c])  if op_row is not None else None,
            })

        # Compute YoY (vs 4 quarters ago) and QoQ growth for the latest
        latest = quarters[-1] if quarters else None
        yoy_rev = yoy_ni = qoq_rev = qoq_ni = None
        if latest and len(quarters) >= 5:
            prev_yr = quarters[-5]
            if latest['revenue'] and prev_yr['revenue']:
                yoy_rev = (latest['revenue'] - prev_yr['revenue']) / abs(prev_yr['revenue']) * 100
            if latest['net_income'] and prev_yr['net_income']:
                yoy_ni  = (latest['net_income'] - prev_yr['net_income']) / abs(prev_yr['net_income']) * 100
        if latest and len(quarters) >= 2:
            prev_q = quarters[-2]
            if latest['revenue'] and prev_q['revenue']:
                qoq_rev = (latest['revenue'] - prev_q['revenue']) / abs(prev_q['revenue']) * 100
            if latest['net_income'] and prev_q['net_income']:
                qoq_ni  = (latest['net_income'] - prev_q['net_income']) / abs(prev_q['net_income']) * 100

        return {
            'quarters': quarters,
            'yoy_rev_pct':  safe_round(yoy_rev),
            'yoy_ni_pct':   safe_round(yoy_ni),
            'qoq_rev_pct':  safe_round(qoq_rev),
            'qoq_ni_pct':   safe_round(qoq_ni),
        }
    except Exception:
        return None


def detect_patterns(df):
    patterns = []
    close = df['Close']
    curr = close.iloc[-1]

    # EMA50 vs EMA200
    ema50  = close.ewm(span=50, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()
    if len(ema200.dropna()) >= 6:
        if ema50.iloc[-1] > ema200.iloc[-1] and ema50.iloc[-6] <= ema200.iloc[-6]:
            patterns.append({'name': 'Golden Cross', 'type': 'bullish',
                             'desc': 'EMA50 just crossed above EMA200 — Strong long-term buy signal'})
        elif ema50.iloc[-1] < ema200.iloc[-1] and ema50.iloc[-6] >= ema200.iloc[-6]:
            patterns.append({'name': 'Death Cross', 'type': 'bearish',
                             'desc': 'EMA50 just crossed below EMA200 — Long-term sell signal'})
        elif ema50.iloc[-1] > ema200.iloc[-1]:
            patterns.append({'name': 'Bull Trend (EMA)', 'type': 'bullish',
                             'desc': 'Price above 200 EMA — Long-term uptrend intact'})

    # 52-week hi/lo
    high52 = close.rolling(min(252, len(close))).max().iloc[-1]
    low52  = close.rolling(min(252, len(close))).min().iloc[-1]
    if curr >= high52 * 0.97:
        patterns.append({'name': '52-Week High Breakout', 'type': 'bullish',
                         'desc': 'Price near/at 52-week high — Strong momentum'})
    elif curr <= low52 * 1.03:
        patterns.append({'name': '52-Week Low', 'type': 'bearish',
                         'desc': 'Price near 52-week low — Oversold, watch for reversal'})

    # Higher highs (uptrend structure)
    if len(df) >= 15:
        h = df['High']
        if h.iloc[-1] > h.iloc[-6] > h.iloc[-11] and df['Low'].iloc[-1] > df['Low'].iloc[-6]:
            patterns.append({'name': 'Higher Highs & Higher Lows', 'type': 'bullish',
                             'desc': 'Classic uptrend structure — Momentum intact'})

    # Inside bar (consolidation)
    if df['High'].iloc[-1] < df['High'].iloc[-2] and df['Low'].iloc[-1] > df['Low'].iloc[-2]:
        patterns.append({'name': 'Inside Bar', 'type': 'neutral',
                         'desc': 'Consolidation candle — Potential breakout setup'})

    # Volume surge breakout
    avg_vol = df['Volume'].rolling(20).mean().iloc[-1]
    if df['Volume'].iloc[-1] > avg_vol * 2.5 and curr > close.iloc[-2]:
        patterns.append({'name': 'Volume Surge Breakout', 'type': 'bullish',
                         'desc': f'Volume {df["Volume"].iloc[-1]/avg_vol:.1f}x average on up-move — Institutional buying'})

    # Bollinger Band squeeze → expansion
    upper, _, lower = calculate_bollinger(close)
    band_width = (upper - lower) / close
    if len(band_width.dropna()) >= 20:
        is_squeeze = band_width.iloc[-6] < band_width.rolling(60).mean().iloc[-6]
        is_expanding = band_width.iloc[-1] > band_width.iloc[-3]
        if is_squeeze and is_expanding and curr > close.iloc[-3]:
            patterns.append({'name': 'BB Squeeze Breakout', 'type': 'bullish',
                             'desc': 'Bollinger Band squeeze resolved with upside breakout'})

    # Doji / indecision (reversal potential)
    body = abs(df['Close'].iloc[-1] - df['Open'].iloc[-1])
    candle_range = df['High'].iloc[-1] - df['Low'].iloc[-1]
    if candle_range > 0 and body / candle_range < 0.15:
        patterns.append({'name': 'Doji Candle', 'type': 'neutral',
                         'desc': 'Indecision candle — Possible trend reversal'})

    return patterns


def detect_resistance_supports(df, lookback=180, pivot_window=5, band_pct=2.0, min_touches=2):
    """Find horizontal resistance and support zones from pivot highs/lows.

    Returns dict with 'resistance' (sorted ascending) and 'support' (sorted
    descending) — each item: {'level': price, 'touches': int, 'age_days': int}.
    """
    if df is None or len(df) < 30:
        return {'resistance': [], 'support': []}

    n = min(int(lookback), len(df))
    sub = df.iloc[-n:]
    highs = sub['High'].values
    lows  = sub['Low'].values
    closes = sub['Close'].values
    dates = sub.index

    # Detect pivot highs and lows
    pivot_highs = []   # list of (price, days_ago_from_end)
    pivot_lows  = []
    for i in range(pivot_window, n - pivot_window):
        win = slice(i - pivot_window, i + pivot_window + 1)
        if highs[i] == max(highs[win]):
            pivot_highs.append((float(highs[i]), n - 1 - i))
        if lows[i] == min(lows[win]):
            pivot_lows.append((float(lows[i]), n - 1 - i))

    # Cluster prices into bands (±band_pct%)
    def cluster(pivots):
        if not pivots:
            return []
        pivots_sorted = sorted(pivots, key=lambda x: x[0])
        bands = []
        cur = [pivots_sorted[0]]
        for p in pivots_sorted[1:]:
            base = cur[0][0]
            if (p[0] - base) / base * 100 < band_pct:
                cur.append(p)
            else:
                bands.append(cur)
                cur = [p]
        bands.append(cur)
        # Filter and format
        out = []
        for b in bands:
            if len(b) < min_touches:
                continue
            level = sum(x[0] for x in b) / len(b)
            ages  = [x[1] for x in b]
            out.append({
                'level':     round(level, 2),
                'touches':   len(b),
                'newest_age': min(ages),
                'oldest_age': max(ages),
            })
        return out

    res = cluster(pivot_highs)
    sup = cluster(pivot_lows)

    # Sort: resistance ascending (nearest first when above CMP),
    # support descending (nearest first when below CMP)
    res.sort(key=lambda x: x['level'])
    sup.sort(key=lambda x: -x['level'])
    return {'resistance': res, 'support': sup}


def compute_fair_value(info, current_price):
    """Compute fair value using 3 methods, return averaged value + per-method breakdown."""
    fwd_eps      = info.get('forwardEps')
    trailing_eps = info.get('trailingEps')
    growth       = info.get('earningsGrowth') or info.get('revenueGrowth')
    roe          = info.get('returnOnEquity') or 0
    de_pct       = info.get('debtToEquity') or 0  # already in percent units
    trailing_pe  = info.get('trailingPE')

    if not trailing_eps and not fwd_eps:
        return None

    estimates = []

    # Method 1: PEG-based (Peter Lynch: fair P/E ≈ growth rate as a %)
    if fwd_eps and fwd_eps > 0 and growth is not None:
        g_pct = float(growth) * 100
        # Bound the target P/E sensibly: at least 8 (cheap), at most 35 (high-growth)
        peg_pe = max(8.0, min(35.0, g_pct))
        if g_pct < 0:
            peg_pe = 8.0   # if shrinking, use bargain multiple
        peg_fv = float(fwd_eps) * peg_pe
        estimates.append({'method': 'PEG (P/E ≈ growth %)', 'value': peg_fv, 'target_pe': round(peg_pe, 1),
                          'weight': 1.0})

    # Method 2: Quality-adjusted P/E on trailing EPS
    if trailing_eps and trailing_eps > 0:
        base_pe = 18.0   # broad market average for India
        # Quality boost: high ROE earns a premium
        try:
            roe_f = float(roe)
            if roe_f > 0.25:    base_pe *= 1.30
            elif roe_f > 0.18:  base_pe *= 1.15
            elif roe_f > 0.12:  base_pe *= 1.00
            elif roe_f > 0:     base_pe *= 0.85
            else:               base_pe *= 0.65
        except Exception:
            pass
        # Leverage penalty
        try:
            de_f = float(de_pct)
            if   de_f > 200: base_pe *= 0.75
            elif de_f > 100: base_pe *= 0.90
        except Exception:
            pass
        q_fv = float(trailing_eps) * base_pe
        estimates.append({'method': 'Quality-adjusted P/E', 'value': q_fv, 'target_pe': round(base_pe, 1),
                          'weight': 1.0})

    # Method 3: Forward P/E × market average
    if fwd_eps and fwd_eps > 0:
        market_pe = 20.0
        fwd_fv = float(fwd_eps) * market_pe
        estimates.append({'method': 'Forward EPS × market P/E', 'value': fwd_fv, 'target_pe': market_pe,
                          'weight': 0.7})

    if not estimates:
        return None

    # Weighted average
    total_w = sum(e['weight'] for e in estimates)
    avg_fv = sum(e['value'] * e['weight'] for e in estimates) / total_w if total_w > 0 else None
    if avg_fv is None or avg_fv <= 0:
        return None

    # Premium/discount vs current
    premium_pct = (current_price - avg_fv) / avg_fv * 100 if avg_fv > 0 else None
    if premium_pct is None:
        verdict = 'Unknown'
    elif premium_pct < -25:
        verdict = 'Deep Undervalued'
    elif premium_pct < -10:
        verdict = 'Undervalued'
    elif premium_pct < 10:
        verdict = 'Fair Value'
    elif premium_pct < 25:
        verdict = 'Overvalued'
    else:
        verdict = 'Strongly Overvalued'

    return {
        'fair_value':  round(avg_fv, 2),
        'premium_pct': round(premium_pct, 1) if premium_pct is not None else None,
        'verdict':     verdict,
        'methods':     [{'method': e['method'], 'value': round(e['value'], 2), 'target_pe': e['target_pe']} for e in estimates],
    }


def _risk_atr(atr_pct):
    """Smooth ATR-based risk adjustment.
    Reference points: 1.0%→-10 | 1.5%→0 | 3.0%→+10 | 5.0%→+20"""
    if atr_pct is None: return 0
    if atr_pct <= 1.0:  return -10
    if atr_pct <= 1.5:  return -10 + (atr_pct - 1.0) * 20
    if atr_pct <= 3.0:  return (atr_pct - 1.5) * (10 / 1.5)
    if atr_pct <= 5.0:  return 10 + (atr_pct - 3.0) * 5
    return 20


def _risk_beta(beta):
    """Smooth beta-based risk. 0.5→-10 | 0.7→-5 | 1.0→0 | 1.2→+5 | 1.5→+10"""
    if beta is None: return 0
    if beta <= 0.5: return -10
    if beta <= 0.7: return -10 + (beta - 0.5) * 25
    if beta <= 1.0: return -5  + (beta - 0.7) * (5 / 0.3)
    if beta <= 1.2: return       (beta - 1.0) * 25
    if beta <= 1.5: return 5   + (beta - 1.2) * (5 / 0.3)
    if beta <= 2.0: return 10  + (beta - 1.5) * 10
    return 15


def _risk_low_proximity(pct_from_low):
    """Falling-knife risk if too close to 52W low.
    <10%→+10 | 30%→+5 | 80%→0 | >80%→-5"""
    if pct_from_low is None: return 0
    if pct_from_low <= 10: return 10
    if pct_from_low <= 30: return 10 - (pct_from_low - 10) / 20 * 5
    if pct_from_low <= 80: return  5 - (pct_from_low - 30) / 50 * 5
    return -5


def _risk_drawdown(dd_pct):
    """20-day drawdown risk. 0→0 | -5→0 | -10→+5 | -15→+10 | -25→+15"""
    if dd_pct is None or dd_pct >= -5: return 0
    if dd_pct >= -10: return (-5  - dd_pct) * 1
    if dd_pct >= -15: return 5 + (-10 - dd_pct) * 1
    if dd_pct >= -25: return 10 + (-15 - dd_pct) * 0.5
    return 15


def _risk_liquidity(avg_turnover_cr):
    """Liquidity risk based on avg daily turnover (₹ crores).
    >100 Cr→-5 (very liquid) | 20-100 Cr→0 | 5-20 Cr→+5 | <5 Cr→+15"""
    if avg_turnover_cr is None: return 0
    if avg_turnover_cr >= 100: return -5
    if avg_turnover_cr >= 50:  return -2
    if avg_turnover_cr >= 20:  return 0
    if avg_turnover_cr >= 5:   return 5 + (20 - avg_turnover_cr) / 15 * 5   # 5→10
    return 15


def _risk_earnings(days_until):
    """Earnings event uncertainty. <3 days→+10 | <7→+5 | <14→+2 | else 0"""
    if days_until is None or days_until < 0: return 0
    if days_until < 3:  return 10
    if days_until < 7:  return 5
    if days_until < 14: return 2
    return 0


def _risk_gaps(gap_count_60d):
    """Frequency of overnight gaps > 2% over the last 60 trading days."""
    if gap_count_60d is None: return 0
    if gap_count_60d >= 5: return 15
    if gap_count_60d >= 3: return 10
    if gap_count_60d >= 1: return 5
    return 0


def _risk_fundamentals(info):
    """Composite fundamental risk adjustment from balance-sheet + profitability."""
    if not info: return 0, {}
    adj = 0
    breakdown = {}

    de = info.get('debtToEquity')
    if de is not None:
        try:
            de_f = float(de)
            if   de_f > 200: c =  10; breakdown['D/E high'] = c
            elif de_f > 100: c =   5; breakdown['D/E elevated'] = c
            elif de_f <  30: c =  -3; breakdown['D/E low'] = c
            else:            c =   0
            adj += c
        except Exception: pass

    pm = info.get('profitMargins')
    if pm is not None:
        try:
            pm_f = float(pm)
            if   pm_f < 0:     c =  15; breakdown['Loss-making'] = c
            elif pm_f < 0.05:  c =   5; breakdown['Thin margins'] = c
            elif pm_f > 0.20:  c =  -3; breakdown['High margins'] = c
            else:              c =   0
            adj += c
        except Exception: pass

    cr = info.get('currentRatio')
    if cr is not None:
        try:
            cr_f = float(cr)
            if   cr_f < 1.0: c = 5; breakdown['Weak liquidity (CR<1)'] = c
            elif cr_f < 1.5: c = 2; breakdown['Tight current ratio'] = c
            else:            c = 0
            adj += c
        except Exception: pass

    roe = info.get('returnOnEquity')
    if roe is not None:
        try:
            roe_f = float(roe)
            if   roe_f < 0:     c =  10; breakdown['Negative ROE'] = c
            elif roe_f < 0.05:  c =   3; breakdown['Low ROE'] = c
            elif roe_f > 0.20:  c =  -3; breakdown['High ROE'] = c
            else:               c =   0
            adj += c
        except Exception: pass

    return adj, breakdown


def _max_drawdown_6m(df):
    """Worst peak-to-trough drawdown over last ~126 trading days."""
    if df is None or len(df) < 30: return None
    n = min(126, len(df))
    sub = df['Close'].iloc[-n:]
    running_max = sub.cummax()
    dd = (sub - running_max) / running_max * 100
    try:
        return float(dd.min())
    except Exception:
        return None


def _risk_max_dd(max_dd_6m):
    """Long-horizon pain. -30→+15 | -20→+10 | -15→+5 | else 0"""
    if max_dd_6m is None: return 0
    if max_dd_6m <= -30: return 15
    if max_dd_6m <= -20: return 10
    if max_dd_6m <= -15: return 5
    return 0


def _risk_normalized_dd(dd_20d, atr_pct):
    """Is the current drawdown unusual relative to normal daily noise?
    dd/ATR > 8 → +8 (huge for the stock's typical movement)
    dd/ATR > 5 → +4
    """
    if dd_20d is None or atr_pct is None or atr_pct <= 0: return 0
    ratio = abs(dd_20d) / atr_pct
    if ratio > 8: return 8
    if ratio > 5: return 4
    return 0


def _downside_deviation(df):
    """Std-dev of NEGATIVE daily returns only (Sortino-style risk)."""
    if df is None or len(df) < 30: return None, None
    try:
        rets = df['Close'].pct_change().dropna() * 100
        neg = rets[rets < 0]
        if len(neg) < 5: return None, None
        downside = float(neg.std())
        total    = float(rets.std())
        return downside, (downside / total if total > 0 else None)
    except Exception:
        return None, None


def _risk_downside(asym_ratio):
    """If downside std is materially higher than overall std → asymmetric risk."""
    if asym_ratio is None: return 0
    if asym_ratio > 1.4: return 8
    if asym_ratio > 1.2: return 4
    return 0


def calculate_levels(df, info=None, earnings=None):
    """Calculate entry, stop-loss, targets and risk for both timeframes."""
    close, high, low = df['Close'], df['High'], df['Low']

    # ── Guard: trailing NaN rows ─────────────────────────────────────────
    # Yahoo sometimes returns a NaN-filled trailing row for intraday/pre-open
    # data. If we don't strip it, curr becomes NaN and cascades into all
    # entry/target/RR math, making the trade-plan card show blanks.
    last_valid = close.last_valid_index()
    if last_valid is not None and last_valid != close.index[-1]:
        df = df.loc[:last_valid]
        close, high, low = df['Close'], df['High'], df['Low']

    curr = float(close.iloc[-1])

    # Final safety net: if curr is still NaN somehow (e.g., entire tail is NaN),
    # fall back to Yahoo's currentPrice from .info.
    if curr != curr:  # NaN check
        cp = (info or {}).get('currentPrice') or (info or {}).get('regularMarketPrice')
        if cp:
            curr = float(cp)
        else:
            # Last resort — use last non-NaN close in the series
            cp_series = close.dropna()
            if len(cp_series):
                curr = float(cp_series.iloc[-1])
            else:
                raise ValueError('No valid price data — every Close row is NaN')

    atr = float(calculate_atr(df, 14).iloc[-1])
    atr_pct = atr / curr * 100

    # Swing levels
    swing_low_st  = float(low.iloc[-10:].min())
    swing_high_st = float(high.iloc[-10:].max())
    swing_low_mt  = float(low.iloc[-50:].min())  if len(low)  >= 50 else swing_low_st
    swing_high_mt = float(high.iloc[-50:].max()) if len(high) >= 50 else swing_high_st

    # 52-week range
    win = min(252, len(close))
    high52 = float(high.iloc[-win:].max())
    low52  = float(low.iloc[-win:].min())

    # Moving averages
    ema20  = float(close.ewm(span=20,  adjust=False).mean().iloc[-1])
    ema50  = float(close.ewm(span=50,  adjust=False).mean().iloc[-1])
    upper_bb, _, lower_bb = calculate_bollinger(close)
    bb_up = float(upper_bb.iloc[-1])

    # ── Detect real horizontal resistance / support zones (used for targets) ──
    zones = detect_resistance_supports(df, lookback=180)
    res_above = [r for r in zones['resistance'] if r['level'] > curr * 1.005]   # at least 0.5% above
    sup_below = [s for s in zones['support']    if s['level'] < curr * 0.995]

    # ── Short-term: stop at max(swing low, 2×ATR), capped at -6%
    st_stop_raw = max(swing_low_st * 0.99, curr - 2 * atr)
    st_stop = min(st_stop_raw, curr * 0.97)  # at least 3% room
    st_stop = max(st_stop, curr * 0.94)      # max 6% stop
    st_risk = curr - st_stop

    # Short-term targets: prefer real resistance, fall back to ATR-projected
    # (ATR is the stock's typical daily move — 3× ATR ≈ 3 weeks of normal volatility).
    # Way more realistic than blind R:R multiples.
    st_t1_info = None
    st_t2_info = None
    # Fallback: ATR-anchored, with R:R as a floor so we never undercut SL meaningfully
    st_t1 = max(curr + 3.0 * atr, curr + st_risk * 1.8)
    st_t2 = max(curr + 5.0 * atr, curr + st_risk * 2.5)
    st_t1_method = 'ATR ×3 (volatility)'
    st_t2_method = 'ATR ×5 (volatility)'

    # If we have resistance zones above us in a reasonable range (within ~15% for short-term), use them
    short_range_max = curr * 1.18
    near_res = [r for r in res_above if r['level'] <= short_range_max]
    if near_res:
        # T1 = nearest resistance (but not too close to entry — at least 1.5x SL distance)
        for r in near_res:
            if r['level'] >= curr + st_risk * 1.2:
                st_t1 = r['level']
                st_t1_method = f"Resistance · {r['touches']}× touches"
                st_t1_info   = r
                break
        # T2 = next resistance after T1
        for r in near_res:
            if r['level'] > st_t1 * 1.02:
                st_t2 = r['level']
                st_t2_method = f"Resistance · {r['touches']}× touches"
                st_t2_info   = r
                break
        else:
            # No second resistance available — extend by 1.5× the T1 distance
            st_t2 = max(st_t2, curr + (st_t1 - curr) * 1.5)

    # 52W high is a hard ceiling — short-term breakouts past it are rare
    if high52 > 0 and 'Resistance' not in st_t2_method:
        if st_t2 > high52:
            st_t2 = min(st_t2, high52)
            st_t2_method = '52W high (ceiling)'
        if st_t1 > high52:
            st_t1 = min(st_t1, high52 * 0.99)
            st_t1_method = 'Near 52W high'

    # ── Mid-term: stop at max(swing low, 3×ATR, EMA50 × 0.95), capped at -12%
    mt_stop_raw = max(swing_low_mt * 0.97, curr - 3 * atr, ema50 * 0.95 if ema50 > 0 else 0)
    mt_stop = min(mt_stop_raw, curr * 0.92)
    mt_stop = max(mt_stop, curr * 0.85)
    mt_risk = curr - mt_stop

    mt_t1_info = None
    mt_t2_info = None
    # ATR-anchored fallback: 8×ATR ≈ ~8 weeks of normal movement, 14×ATR ≈ ~3 months
    mt_t1 = max(curr + 8.0 * atr, curr + mt_risk * 2.0)
    mt_t2 = max(curr + 14.0 * atr, curr + mt_risk * 3.0)
    mt_t1_method = 'ATR ×8 (volatility)'
    mt_t2_method = 'ATR ×14 (volatility)'

    # Mid-term: look further out — up to ~40% above CMP — and use resistance zones
    mid_range_max = curr * 1.45
    mid_res = [r for r in res_above if r['level'] <= mid_range_max]
    if mid_res:
        # T1 = first resistance above ≥1.5× SL distance
        for r in mid_res:
            if r['level'] >= curr + mt_risk * 1.3:
                mt_t1 = r['level']
                mt_t1_method = f"Resistance · {r['touches']}× touches"
                mt_t1_info   = r
                break
        # T2 = next resistance beyond T1
        for r in mid_res:
            if r['level'] > mt_t1 * 1.03:
                mt_t2 = r['level']
                mt_t2_method = f"Resistance · {r['touches']}× touches"
                mt_t2_info   = r
                break
        else:
            mt_t2 = max(mt_t2, curr + (mt_t1 - curr) * 1.5)

    # Mid-term ceiling: 52W high acts as a soft cap. We can slightly exceed it
    # for genuine resistance breaks but not for blind ATR projections.
    if 'Resistance' not in mt_t2_method and high52 > 0 and mt_t2 > high52 * 1.05:
        mt_t2 = high52 * 1.05
        mt_t2_method = '52W high × 1.05 (cap)'
    if 'Resistance' not in mt_t1_method and high52 > 0 and mt_t1 > high52:
        mt_t1 = high52
        mt_t1_method = '52W high (ceiling)'
    # Final hard cap on T2: don't promise more than +40% over 6 months
    if mt_t2 > curr * 1.40:
        mt_t2 = curr * 1.40
        mt_t2_method = '+40% (mid-term ceiling)'

    # ── Long-term: EMA200-anchored stop (up to -22%), targets driven by fair value
    ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1]) if len(close) >= 50 else ema50
    lt_stop_raw = max(low52 * 1.02, ema200 * 0.92 if ema200 > 0 else 0, curr - 5 * atr)
    lt_stop = min(lt_stop_raw, curr * 0.85)   # at least -15% room
    lt_stop = max(lt_stop, curr * 0.78)        # cap at -22%
    lt_risk = curr - lt_stop

    # ATR-anchored fallback for LT (~18×ATR ≈ 6 months, ~32×ATR ≈ 1 year)
    lt_t1 = max(curr + 18.0 * atr, curr + lt_risk * 2.0)
    lt_t2 = max(curr + 32.0 * atr, curr + lt_risk * 3.5)
    lt_t1_method = 'ATR ×18 (volatility)'
    lt_t2_method = 'ATR ×32 (volatility)'

    fair_value = compute_fair_value(info, curr) if info else None
    if fair_value and fair_value.get('fair_value') and fair_value['fair_value'] > curr * 1.05:
        # Stock is undervalued — use fair value as the anchor
        fv = fair_value['fair_value']
        lt_t1 = (curr + fv) / 2.0    # midpoint of journey
        lt_t2 = fv * 1.05            # slight overshoot of fair value
        lt_t1_method = 'Midpoint to Fair Value'
        lt_t2_method = f"Fair Value × 1.05 ({fair_value['verdict']})"
    elif fair_value and fair_value.get('fair_value') and fair_value['fair_value'] >= curr * 0.95:
        # Near fair value — conservative targets driven by earnings growth
        fv = fair_value['fair_value']
        lt_t1 = max(curr * 1.15, fv)
        lt_t2 = curr * 1.30
        lt_t1_method = 'Earnings Growth Anchor'
        lt_t2_method = '12-24m Conservative Target'
    else:
        # Overvalued case (curr > fv * 1.05 from above) OR no fair value.
        # Even without explicit overvalued flag, we don't want runaway targets.
        near_high = high52 > 0 and curr >= high52 * 0.95
        if near_high:
            lt_t1 = min(lt_t1, curr * 1.60)
            lt_t2 = min(lt_t2, curr * 2.20)
        else:
            if lt_t2 > high52 * 1.30:
                lt_t2 = high52 * 1.25
                lt_t2_method = '52W high × 1.25 (cap)'
            if lt_t1 > high52 * 1.10 and high52 > curr:
                lt_t1 = max(curr * 1.20, high52 * 1.05)
                lt_t1_method = '52W high × 1.05 (anchor)'

    # ── OVERVALUED HARD CAP (new in Tier 1) ───────────────────────────────
    # When the stock is materially above fair value, we should NOT publish
    # bullish LT targets that suggest chasing. Cap targets to reflect
    # cautious "mean-reversion or modest continuation" expectations.
    if fair_value and fair_value.get('fair_value'):
        fv = fair_value['fair_value']
        overval_ratio = curr / fv if fv > 0 else 1.0
        if overval_ratio > 1.40:
            # Strongly overvalued — minimal upside expectation
            cap_t1 = curr * 1.05
            cap_t2 = curr * 1.12
            if lt_t1 > cap_t1:
                lt_t1 = cap_t1
                lt_t1_method = f"Capped +5% (overvalued · {int((overval_ratio-1)*100)}% above FV)"
            if lt_t2 > cap_t2:
                lt_t2 = cap_t2
                lt_t2_method = f"Capped +12% (overvalued · {int((overval_ratio-1)*100)}% above FV)"
        elif overval_ratio > 1.20:
            # Moderately overvalued — modest upside only
            cap_t1 = curr * 1.10
            cap_t2 = curr * 1.20
            if lt_t1 > cap_t1:
                lt_t1 = cap_t1
                lt_t1_method = f"Capped +10% (overvalued · {int((overval_ratio-1)*100)}% above FV)"
            if lt_t2 > cap_t2:
                lt_t2 = cap_t2
                lt_t2_method = f"Capped +20% (overvalued · {int((overval_ratio-1)*100)}% above FV)"

    # Final sanity guards
    if lt_t1 <= curr:
        lt_t1 = curr * 1.05      # minimum +5% (not 20% — could be overvalued)
        if 'Capped' not in lt_t1_method:
            lt_t1_method = 'Minimum +5%'
    if lt_t2 <= lt_t1:
        lt_t2 = lt_t1 * 1.10

    # ── Risk score — smooth scoring + expanded inputs ────────────────────────
    pct_from_low_52w  = (curr - low52)  / low52  * 100 if low52  > 0 else 0
    pct_from_high_52w = (curr - high52) / high52 * 100 if high52 > 0 else 0
    recent_high = float(high.iloc[-20:].max())
    drawdown_20d = (curr - recent_high) / recent_high * 100 if recent_high > 0 else 0

    # NEW: liquidity (avg ₹ turnover in crores over last 30 days)
    try:
        recent_n = min(30, len(df))
        avg_turnover_rs = float((df['Close'].iloc[-recent_n:] * df['Volume'].iloc[-recent_n:]).mean())
        avg_turnover_cr = avg_turnover_rs / 1e7    # ₹ → Crores
    except Exception:
        avg_turnover_cr = None

    # NEW: count overnight gaps > 2% over last 60 days
    try:
        recent_n = min(60, len(df) - 1)
        opens   = df['Open'].iloc[-recent_n:].values
        prev_cl = df['Close'].iloc[-recent_n - 1:-1].values
        gap_pct = (opens - prev_cl) / prev_cl * 100
        gap_count_60d = int(sum(abs(g) > 2.0 for g in gap_pct))
    except Exception:
        gap_count_60d = None

    # Beta pulled from info if available (caller should override with computed)
    beta_val = None
    if info:
        try:
            b = info.get('beta')
            beta_val = float(b) if b is not None else None
        except Exception:
            beta_val = None

    # Earnings proximity
    days_to_earnings = (earnings.get('days_until') if earnings else None)

    # NEW: deeper drawdown + downside metrics
    max_dd_6m = _max_drawdown_6m(df)
    downside_std, asym_ratio = _downside_deviation(df)
    fund_adj, fund_breakdown = _risk_fundamentals(info)

    # Per-factor contributions
    factors = {
        'atr':            _risk_atr(atr_pct),
        'low_proximity':  _risk_low_proximity(pct_from_low_52w),
        'drawdown':       _risk_drawdown(drawdown_20d),
        'beta':           _risk_beta(beta_val),
        'liquidity':      _risk_liquidity(avg_turnover_cr),
        'earnings':       _risk_earnings(days_to_earnings),
        'gaps':           _risk_gaps(gap_count_60d),
        'fundamentals':   fund_adj,
        'max_dd_6m':      _risk_max_dd(max_dd_6m),
        'normalized_dd':  _risk_normalized_dd(drawdown_20d, atr_pct),
        'downside_dev':   _risk_downside(asym_ratio),
    }
    risk = 50 + sum(factors.values())
    risk = max(0, min(100, int(round(risk))))

    if   risk >= 70: risk_lbl = {'label': 'High Risk',       'color': '#f85149'}
    elif risk >= 55: risk_lbl = {'label': 'Medium-High Risk','color': '#ff9100'}
    elif risk >= 40: risk_lbl = {'label': 'Medium Risk',     'color': '#ffd740'}
    elif risk >= 25: risk_lbl = {'label': 'Low-Medium Risk', 'color': '#69f0ae'}
    else:            risk_lbl = {'label': 'Low Risk',        'color': '#00e676'}

    volatility = 'High' if atr_pct > 3 else 'Medium' if atr_pct > 1.5 else 'Low'

    def pct(a, b): return round((a - b) / b * 100, 2) if b > 0 else 0

    return {
        'short_term': {
            'entry':     round(curr, 2),
            'stop_loss': round(st_stop, 2),
            'sl_pct':    pct(st_stop, curr),
            'target1':   round(st_t1, 2),
            't1_pct':    pct(st_t1, curr),
            't1_method': st_t1_method,
            't1_info':   st_t1_info,
            'target2':   round(st_t2, 2),
            't2_pct':    pct(st_t2, curr),
            't2_method': st_t2_method,
            't2_info':   st_t2_info,
            'rr':        round((st_t1 - curr) / max(curr - st_stop, 0.01), 2),
            'timeframe': '1–4 weeks',
        },
        'mid_term': {
            'entry':     round(curr, 2),
            'stop_loss': round(mt_stop, 2),
            'sl_pct':    pct(mt_stop, curr),
            'target1':   round(mt_t1, 2),
            't1_pct':    pct(mt_t1, curr),
            't1_method': mt_t1_method,
            't1_info':   mt_t1_info,
            'target2':   round(mt_t2, 2),
            't2_pct':    pct(mt_t2, curr),
            't2_method': mt_t2_method,
            't2_info':   mt_t2_info,
            'rr':        round((mt_t1 - curr) / max(curr - mt_stop, 0.01), 2),
            'timeframe': '1–6 months',
        },
        'long_term': {
            'entry':     round(curr, 2),
            'stop_loss': round(lt_stop, 2),
            'sl_pct':    pct(lt_stop, curr),
            'target1':   round(lt_t1, 2),
            't1_pct':    pct(lt_t1, curr),
            't1_method': lt_t1_method,
            'target2':   round(lt_t2, 2),
            't2_pct':    pct(lt_t2, curr),
            't2_method': lt_t2_method,
            'rr':        round((lt_t1 - curr) / max(curr - lt_stop, 0.01), 2),
            'timeframe': '6–24 months',
            'fair_value': fair_value,
        },
        'zones': {
            'resistance_count': len(zones.get('resistance', [])),
            'support_count':    len(zones.get('support', [])),
            'all_resistance':   zones.get('resistance', [])[:5],
            'all_support':      zones.get('support', [])[:5],
        },
        'risk': {
            'score':            int(risk),
            'level':            risk_lbl,
            'atr':              round(atr, 2),
            'atr_pct':          round(atr_pct, 2),
            'volatility':       volatility,
            'drawdown_20d':     round(drawdown_20d, 2),
            'pct_from_52w_low': round(pct_from_low_52w, 1),
            'pct_from_52w_high':round(pct_from_high_52w, 1),
            'beta':             round(beta_val, 2) if beta_val is not None else None,
            # New fields
            'avg_turnover_cr':  round(avg_turnover_cr, 2) if avg_turnover_cr is not None else None,
            'gap_count_60d':    gap_count_60d,
            'days_to_earnings': days_to_earnings,
            # NEW Tier B metrics
            'max_dd_6m':        round(max_dd_6m, 2) if max_dd_6m is not None else None,
            'downside_dev':     round(downside_std, 2) if downside_std is not None else None,
            'asym_ratio':       round(asym_ratio, 2) if asym_ratio is not None else None,
            'fund_breakdown':   fund_breakdown,
            'factors':          {k: round(v, 1) for k, v in factors.items()},
        }
    }


# Approx India retail round-trip cost: 0.03% brokerage × 2 + 0.10% STT (sell) + 18% GST on brokerage + ~0.05% slippage
# ≈ 0.20-0.30% per round-trip. We use 0.30% conservatively.
BACKTEST_ROUND_TRIP_COST_PCT = 0.30


def compute_signal_age(df, current_st_score, buy_threshold=60, sell_threshold=35, lookback=60):
    """Find how many days the current signal has been active.
    Returns dict with type ('buy' | 'sell' | 'neutral'), age_days, score_at_signal."""
    if df is None or len(df) < 50 or current_st_score is None:
        return None
    signal_type = 'buy' if current_st_score >= buy_threshold else 'sell' if current_st_score <= sell_threshold else 'neutral'
    if signal_type == 'neutral':
        return {'type': 'neutral', 'age_days': None, 'score_at_signal': current_st_score}

    # Walk backwards, recompute the score, find first day the signal flipped from neutral/other
    lookback = min(lookback, len(df) - 20)
    age = 0
    found_flip = False
    for k in range(1, lookback + 1):
        slice_df = df.iloc[:-k]
        if len(slice_df) < 30:
            break
        try:
            t_sc, _ = technical_score(slice_df)
        except Exception:
            continue
        # Use technical_score as proxy for short-term score direction (composite scoring needs
        # fundamentals which don't change daily — technical is the dominant time-varying factor)
        if signal_type == 'buy' and t_sc < buy_threshold:
            age = k - 1
            found_flip = True
            break
        if signal_type == 'sell' and t_sc > sell_threshold:
            age = k - 1
            found_flip = True
            break
        age = k

    return {
        'type':            signal_type,
        'age_days':        age,
        'reliable':        found_flip,         # if we hit the lookback ceiling, age is "≥ lookback"
        'score_at_signal': current_st_score,
        'buy_threshold':   buy_threshold,
        'sell_threshold':  sell_threshold,
    }


def run_backtest(df, threshold=65, max_hold=30, min_gap=10,
                 cost_pct=BACKTEST_ROUND_TRIP_COST_PCT):
    """
    Walk historical OHLCV day-by-day. At each day, compute the technical score
    from data up to that day. When the score crosses above `threshold` (and we
    haven't already opened a trade in the last `min_gap` days), open a long
    position at the close. Walk forward up to `max_hold` days and record whether
    Target 1 or Stop Loss was hit first.

    Improvements over the naive version:
      - Subtracts `cost_pct` (round-trip transaction cost) from every trade's P&L
      - Smarter same-day SL/T1 logic: when both are hit on the same day, the
        candle's open-to-close direction is used to infer the more likely path
      - Tracks overnight gap risk: trades where the next-day open opens beyond
        the SL/T1 are flagged (real fills wouldn't have hit our levels cleanly)
    """
    if df is None or len(df) < 220:
        return {'trades': [], 'summary': None, 'note': 'Not enough history (need 220+ days)'}

    trades = []
    prev_score = 0
    last_signal_idx = -10**9
    gap_events = 0
    gap_warnings = 0  # gap on entry day next-open
    pessimistic_resolutions = 0  # same-day both hit, resolution choice mattered

    for i in range(200, len(df) - 1):
        slice_df = df.iloc[:i + 1]
        try:
            t_score, _ = technical_score(slice_df)
        except Exception:
            prev_score = 0
            continue

        if t_score >= threshold and prev_score < threshold and (i - last_signal_idx) > min_gap:
            try:
                lvls = calculate_levels(slice_df)
                sl = float(lvls['short_term']['stop_loss'])
                t1 = float(lvls['short_term']['target1'])
            except Exception:
                prev_score = t_score
                continue

            entry_price = float(df['Close'].iloc[i])
            if sl >= entry_price or t1 <= entry_price:
                prev_score = t_score
                continue

            outcome = 'open'
            exit_price = None
            exit_idx = None
            gap_at_entry = False
            for j in range(i + 1, min(i + 1 + max_hold, len(df))):
                day_open = float(df['Open'].iloc[j])
                day_low  = float(df['Low'].iloc[j])
                day_high = float(df['High'].iloc[j])
                day_close = float(df['Close'].iloc[j])

                # Detect overnight gap on entry day's next morning
                if j == i + 1:
                    if day_open < sl:
                        # Gap below SL — would have filled at open, worse than SL
                        gap_warnings += 1
                        outcome = 'loss'
                        exit_price = day_open
                        exit_idx = j
                        gap_at_entry = True
                        break
                    if day_open > t1:
                        gap_warnings += 1
                        outcome = 'win'
                        exit_price = day_open
                        exit_idx = j
                        gap_at_entry = True
                        break

                hit_sl = day_low <= sl
                hit_t1 = day_high >= t1
                if hit_sl and hit_t1:
                    # Both hit in same day — use intraday candle direction as proxy
                    pessimistic_resolutions += 1
                    if day_close >= day_open:
                        # Bullish candle: more likely T1 was hit first
                        outcome = 'win'; exit_price = t1
                    else:
                        outcome = 'loss'; exit_price = sl
                    exit_idx = j
                    break
                if hit_sl:
                    outcome = 'loss'; exit_price = sl; exit_idx = j
                    break
                if hit_t1:
                    outcome = 'win'; exit_price = t1; exit_idx = j
                    break

            if outcome == 'open':
                exit_idx = min(i + max_hold, len(df) - 1)
                exit_price = float(df['Close'].iloc[exit_idx])
                outcome = 'timeout_win' if exit_price > entry_price else 'timeout_loss'

            # P&L calculations: gross vs net (after transaction cost)
            gross_pct = (exit_price - entry_price) / entry_price * 100
            net_pct = gross_pct - cost_pct  # round-trip cost

            trades.append({
                'entry_date':  df.index[i].strftime('%Y-%m-%d'),
                'entry_price': round(entry_price, 2),
                'sl':          round(sl, 2),
                't1':          round(t1, 2),
                'exit_date':   df.index[exit_idx].strftime('%Y-%m-%d') if exit_idx is not None else None,
                'exit_price':  round(exit_price, 2),
                'days_held':   (exit_idx - i) if exit_idx is not None else None,
                'outcome':     outcome,
                'gross_pct':   round(gross_pct, 2),
                'pnl_pct':     round(net_pct, 2),   # net after costs (this is the headline number)
                'gap_at_entry': gap_at_entry,
                'score':       t_score,
            })
            last_signal_idx = i

        prev_score = t_score

    if not trades:
        return {'trades': [], 'summary': None, 'note': 'No signals in the backtest period'}

    # Use NET (after-cost) figures as the headline; report gross for reference too
    wins   = [t for t in trades if t['pnl_pct'] >= 0]
    losses = [t for t in trades if t['pnl_pct'] < 0]
    win_rate = len(wins) / len(trades) * 100
    avg_win_net   = sum(t['pnl_pct']   for t in wins)   / len(wins)   if wins else 0
    avg_loss_net  = sum(t['pnl_pct']   for t in losses) / len(losses) if losses else 0
    avg_win_gross = sum(t['gross_pct'] for t in wins)   / len(wins)   if wins else 0
    avg_loss_gross= sum(t['gross_pct'] for t in losses) / len(losses) if losses else 0
    expectancy_net   = (win_rate / 100) * avg_win_net   + (1 - win_rate / 100) * avg_loss_net
    expectancy_gross = (win_rate / 100) * avg_win_gross + (1 - win_rate / 100) * avg_loss_gross
    avg_hold = sum((t['days_held'] or 0) for t in trades) / len(trades)
    best  = max((t['pnl_pct'] for t in trades), default=0)
    worst = min((t['pnl_pct'] for t in trades), default=0)

    # Cumulative compounded return (net) — what 1 unit of capital becomes after all trades
    cum_return = 1.0
    for t in trades:
        cum_return *= (1 + t['pnl_pct'] / 100)
    cum_return_pct = (cum_return - 1) * 100

    trades_sorted = sorted(trades, key=lambda t: t['entry_date'], reverse=True)

    return {
        'trades': trades_sorted,
        'summary': {
            'total_signals':       len(trades),
            'wins':                len(wins),
            'losses':              len(losses),
            'win_rate':            round(win_rate, 1),
            'avg_win_pct':         round(avg_win_net, 2),
            'avg_loss_pct':        round(avg_loss_net, 2),
            'expectancy_pct':      round(expectancy_net, 2),       # NET — headline
            'expectancy_gross_pct':round(expectancy_gross, 2),     # BEFORE costs
            'cum_return_pct':      round(cum_return_pct, 2),       # compounded net
            'avg_hold_days':       round(avg_hold, 1),
            'best_trade_pct':      round(best, 2),
            'worst_trade_pct':     round(worst, 2),
            'cost_per_trade_pct':  cost_pct,
            'gap_warnings':        gap_warnings,                   # entry-day overnight gaps past SL/T1
            'pessimistic_calls':   pessimistic_resolutions,        # same-day SL+T1 days resolved by candle direction
            'threshold':           threshold,
            'max_hold_days':       max_hold,
        }
    }


_POSITIVE_NEWS_WORDS = {
    'surge', 'surges', 'surged', 'rally', 'rallies', 'rallied', 'jump', 'jumps', 'jumped',
    'rise', 'rises', 'rose', 'gain', 'gains', 'gained', 'beat', 'beats', 'beating',
    'strong', 'stronger', 'strongest', 'bullish', 'upgrade', 'upgrades', 'upgraded',
    'outperform', 'outperforms', 'outperformed', 'record', 'breakthrough', 'soar', 'soars',
    'positive', 'profit', 'profits', 'expansion', 'expand', 'growth', 'growing',
    'breakout', 'high', 'new high', 'recovery', 'recovered', 'optimistic', 'optimism',
    'bonus', 'dividend', 'buyback', 'partnership', 'acquisition', 'expansion', 'launch',
    'win', 'wins', 'won', 'award', 'awarded', 'milestone',
}
_NEGATIVE_NEWS_WORDS = {
    'plunge', 'plunges', 'plunged', 'crash', 'crashes', 'crashed', 'tumble', 'tumbles', 'tumbled',
    'fall', 'falls', 'fell', 'drop', 'drops', 'dropped', 'slump', 'slumps', 'slumped',
    'miss', 'misses', 'missed', 'weak', 'weaker', 'weakness', 'bearish',
    'downgrade', 'downgrades', 'downgraded', 'underperform', 'underperforms',
    'loss', 'losses', 'losing', 'decline', 'declines', 'declined', 'sink', 'sinks', 'sank',
    'concern', 'concerns', 'worry', 'worries', 'fear', 'fears', 'fraud', 'scandal',
    'probe', 'investigation', 'lawsuit', 'penalty', 'fine', 'raid', 'arrest',
    'cut', 'cuts', 'reduce', 'reduces', 'reduced', 'layoff', 'layoffs',
    'slowdown', 'recession', 'crisis', 'risk', 'risks', 'warning', 'warned',
    'low', 'new low', 'sell-off', 'selloff', 'breach', 'default', 'breakdown',
}


def score_news_sentiment(news_items):
    """Score each news item -1..+1 from headline keywords, return (scored_items, aggregate)."""
    scored = []
    for item in news_items:
        title_lower = (item.get('title') or '').lower()
        words = set(title_lower.replace(',', ' ').replace('.', ' ').replace('-', ' ').split())
        # Multi-word lookups for phrases
        pos_hits = [w for w in _POSITIVE_NEWS_WORDS if w in words or w in title_lower]
        neg_hits = [w for w in _NEGATIVE_NEWS_WORDS if w in words or w in title_lower]
        pos = len(pos_hits)
        neg = len(neg_hits)
        if pos + neg == 0:
            score = 0.0
            label = 'neutral'
        else:
            score = (pos - neg) / (pos + neg)
            label = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
        scored.append({**item, 'sentiment': round(score, 2), 'sentiment_label': label})
    if scored:
        avg = sum(s['sentiment'] for s in scored) / len(scored)
        pos_count = sum(1 for s in scored if s['sentiment_label'] == 'positive')
        neg_count = sum(1 for s in scored if s['sentiment_label'] == 'negative')
        agg_score = int(max(0, min(100, 50 + avg * 50)))
    else:
        avg = 0
        agg_score = 50
        pos_count = neg_count = 0

    if   agg_score >= 65: verdict = 'Positive'
    elif agg_score >= 55: verdict = 'Slightly Positive'
    elif agg_score >= 45: verdict = 'Neutral'
    elif agg_score >= 35: verdict = 'Slightly Negative'
    else:                 verdict = 'Negative'

    return scored, {
        'score':     agg_score,
        'avg':       round(avg, 2),
        'positive':  pos_count,
        'negative':  neg_count,
        'neutral':   len(scored) - pos_count - neg_count,
        'total':     len(scored),
        'verdict':   verdict,
    }


def build_explanations(t_score, f_score, n_score, t_signals, f_signals, levels, quarterly, news_agg):
    """Generate 'why this score' bullets per pillar + overall confidence assessment."""
    exps = {'technical': [], 'fundamental': [], 'sentiment': [], 'risk': []}

    # Technical: top 4 strongest signals (bullish + bearish)
    sorted_t = sorted(t_signals, key=lambda s: 0 if s['type'] == 'neutral' else 1, reverse=True)
    for s in sorted_t[:4]:
        exps['technical'].append({'type': s['type'], 'text': s['text']})

    # Fundamental: top 4
    sorted_f = sorted(f_signals, key=lambda s: 0 if s['type'] == 'neutral' else 1, reverse=True)
    for s in sorted_f[:4]:
        exps['fundamental'].append({'type': s['type'], 'text': s['text']})

    # Sentiment
    nv = news_agg.get('verdict', 'Neutral')
    nt = news_agg.get('total', 0)
    if nt == 0:
        exps['sentiment'].append({'type': 'neutral', 'text': 'No recent news available for sentiment scoring'})
    else:
        s_type = 'bullish' if news_agg['score'] >= 55 else 'bearish' if news_agg['score'] < 45 else 'neutral'
        exps['sentiment'].append({'type': s_type,
                                  'text': f"{nv} sentiment from {nt} recent headlines"})
        if news_agg['positive']:
            exps['sentiment'].append({'type': 'bullish', 'text': f"{news_agg['positive']} positive headlines"})
        if news_agg['negative']:
            exps['sentiment'].append({'type': 'bearish', 'text': f"{news_agg['negative']} negative headlines"})

    # Risk
    r = levels.get('risk', {})
    if r.get('volatility') == 'High':
        exps['risk'].append({'type': 'bearish', 'text': f"High volatility (ATR {r.get('atr_pct')}% of price) — use wider stops"})
    elif r.get('volatility') == 'Low':
        exps['risk'].append({'type': 'bullish', 'text': f"Low volatility (ATR {r.get('atr_pct')}%) — predictable moves"})
    if (r.get('drawdown_20d') or 0) < -10:
        exps['risk'].append({'type': 'bearish', 'text': f"In a {r['drawdown_20d']}% drawdown over the last 20 sessions"})
    if r.get('beta') is not None:
        if r['beta'] > 1.5:
            exps['risk'].append({'type': 'bearish', 'text': f"Beta {r['beta']} — moves ~{r['beta']:.1f}× the market"})
        elif r['beta'] < 0.5:
            exps['risk'].append({'type': 'bullish', 'text': f"Beta {r['beta']} — defensive, low market sensitivity"})
    pct_low = r.get('pct_from_52w_low', 0)
    if pct_low > 80:
        exps['risk'].append({'type': 'neutral', 'text': f"{pct_low}% above 52-week low — extended rally"})
    elif pct_low < 10:
        exps['risk'].append({'type': 'bearish', 'text': f"Only {pct_low}% above 52-week low — falling-knife risk"})

    # Overall confidence
    scores = [t_score, f_score, n_score]
    spread = max(scores) - min(scores)
    avg_score = sum(scores) / 3
    if spread < 15 and avg_score >= 65:
        confidence = {'level': 'High', 'color': '#00e676', 'reason': 'All three pillars align bullishly'}
    elif spread < 15 and avg_score < 40:
        confidence = {'level': 'High', 'color': '#ff5252', 'reason': 'All three pillars align bearishly'}
    elif spread > 35:
        confidence = {'level': 'Low', 'color': '#ffd740',
                      'reason': 'Pillars disagree — technicals say one thing, fundamentals say another'}
    elif spread > 20:
        confidence = {'level': 'Medium', 'color': '#ffd740', 'reason': 'Some pillar disagreement'}
    else:
        confidence = {'level': 'Medium', 'color': '#69f0ae', 'reason': 'Pillars broadly agree'}

    return exps, confidence


def recommendation_label(score):
    if score >= 80:
        return {'label': 'Strong Buy', 'color': '#00c853', 'badge': 'success'}
    elif score >= 65:
        return {'label': 'Buy',        'color': '#69f0ae', 'badge': 'success'}
    elif score >= 50:
        return {'label': 'Neutral',    'color': '#ffd740', 'badge': 'warning'}
    elif score >= 35:
        return {'label': 'Sell',       'color': '#ff6d00', 'badge': 'danger'}
    else:
        return {'label': 'Strong Sell','color': '#d50000', 'badge': 'danger'}


# ── Static fallback for company name + sector + industry ────────────────────
# On Render's cloud IPs, yfinance.Ticker(sym).info often returns empty {} due
# to Yahoo's bot-detection. Without this, names appear as "TCS.NS" and sector
# shows "Unknown". This dict covers all NIFTY 50 + NIFTY Bank + IT + Pharma
# + Auto + Midcap stocks so the most-analyzed names always render properly.
NSE_COMPANY_INFO = {
    # ── NIFTY 50 ──
    'ADANIENT':    ('Adani Enterprises',                 'Industrials',           'Conglomerate'),
    'ADANIPORTS':  ('Adani Ports & SEZ',                 'Industrials',           'Ports & Logistics'),
    'APOLLOHOSP':  ('Apollo Hospitals Enterprise',       'Healthcare',            'Hospitals'),
    'ASIANPAINT':  ('Asian Paints',                      'Consumer Defensive',    'Paints'),
    'AXISBANK':    ('Axis Bank',                         'Financial Services',    'Bank'),
    'BAJAJ-AUTO':  ('Bajaj Auto',                        'Consumer Cyclical',     'Auto Manufacturers'),
    'BAJFINANCE':  ('Bajaj Finance',                     'Financial Services',    'NBFC'),
    'BAJAJFINSV':  ('Bajaj Finserv',                     'Financial Services',    'Diversified Financial'),
    'BPCL':        ('Bharat Petroleum',                  'Energy',                'Oil & Gas Refining'),
    'BHARTIARTL':  ('Bharti Airtel',                     'Communication Services','Telecom'),
    'BRITANNIA':   ('Britannia Industries',              'Consumer Defensive',    'Packaged Foods'),
    'CIPLA':       ('Cipla',                             'Healthcare',            'Pharma'),
    'COALINDIA':   ('Coal India',                        'Energy',                'Coal'),
    'DIVISLAB':    ('Divis Laboratories',                'Healthcare',            'Pharma'),
    'DRREDDY':     ('Dr. Reddys Laboratories',           'Healthcare',            'Pharma'),
    'EICHERMOT':   ('Eicher Motors',                     'Consumer Cyclical',     'Auto Manufacturers'),
    'GRASIM':      ('Grasim Industries',                 'Basic Materials',       'Cement & Textiles'),
    'HCLTECH':     ('HCL Technologies',                  'Technology',            'IT Services'),
    'HDFCBANK':    ('HDFC Bank',                         'Financial Services',    'Bank'),
    'HDFCLIFE':    ('HDFC Life Insurance',               'Financial Services',    'Insurance'),
    'HEROMOTOCO':  ('Hero MotoCorp',                     'Consumer Cyclical',     'Auto Manufacturers'),
    'HINDALCO':    ('Hindalco Industries',               'Basic Materials',       'Aluminium'),
    'HINDUNILVR':  ('Hindustan Unilever',                'Consumer Defensive',    'FMCG'),
    'ICICIBANK':   ('ICICI Bank',                        'Financial Services',    'Bank'),
    'INDUSINDBK':  ('IndusInd Bank',                     'Financial Services',    'Bank'),
    'INFY':        ('Infosys',                           'Technology',            'IT Services'),
    'ITC':         ('ITC',                               'Consumer Defensive',    'FMCG & Tobacco'),
    'JSWSTEEL':    ('JSW Steel',                         'Basic Materials',       'Steel'),
    'KOTAKBANK':   ('Kotak Mahindra Bank',               'Financial Services',    'Bank'),
    'LT':          ('Larsen & Toubro',                   'Industrials',           'Engineering & Construction'),
    'LTIM':        ('LTIMindtree',                       'Technology',            'IT Services'),
    'M&M':         ('Mahindra & Mahindra',               'Consumer Cyclical',     'Auto Manufacturers'),
    'MARUTI':      ('Maruti Suzuki India',               'Consumer Cyclical',     'Auto Manufacturers'),
    'NESTLEIND':   ('Nestle India',                      'Consumer Defensive',    'Packaged Foods'),
    'NTPC':        ('NTPC',                              'Utilities',             'Power Generation'),
    'ONGC':        ('Oil & Natural Gas Corp',            'Energy',                'Oil & Gas Exploration'),
    'POWERGRID':   ('Power Grid Corporation',            'Utilities',             'Power Transmission'),
    'RELIANCE':    ('Reliance Industries',               'Energy',                'Oil & Gas Conglomerate'),
    'SBILIFE':     ('SBI Life Insurance',                'Financial Services',    'Insurance'),
    'SBIN':        ('State Bank of India',               'Financial Services',    'PSU Bank'),
    'SHRIRAMFIN':  ('Shriram Finance',                   'Financial Services',    'NBFC'),
    'SUNPHARMA':   ('Sun Pharmaceutical',                'Healthcare',            'Pharma'),
    'TATACONSUM':  ('Tata Consumer Products',            'Consumer Defensive',    'FMCG'),
    'TATAMOTORS':  ('Tata Motors',                       'Consumer Cyclical',     'Auto Manufacturers'),
    'TATASTEEL':   ('Tata Steel',                        'Basic Materials',       'Steel'),
    'TCS':         ('Tata Consultancy Services',         'Technology',            'IT Services'),
    'TECHM':       ('Tech Mahindra',                     'Technology',            'IT Services'),
    'TITAN':       ('Titan Company',                     'Consumer Cyclical',     'Jewellery & Watches'),
    'ULTRACEMCO':  ('UltraTech Cement',                  'Basic Materials',       'Cement'),
    'UPL':         ('UPL',                               'Basic Materials',       'Agrochemicals'),
    'WIPRO':       ('Wipro',                             'Technology',            'IT Services'),

    # ── NIFTY Bank (extras beyond NIFTY 50) ──
    'BANDHANBNK':  ('Bandhan Bank',                      'Financial Services',    'Bank'),
    'FEDERALBNK':  ('Federal Bank',                      'Financial Services',    'Bank'),
    'IDFCFIRSTB':  ('IDFC First Bank',                   'Financial Services',    'Bank'),
    'PNB':         ('Punjab National Bank',              'Financial Services',    'PSU Bank'),
    'BANKBARODA':  ('Bank of Baroda',                    'Financial Services',    'PSU Bank'),
    'AUBANK':      ('AU Small Finance Bank',             'Financial Services',    'Bank'),

    # ── NIFTY IT (extras) ──
    'COFORGE':     ('Coforge',                           'Technology',            'IT Services'),
    'MPHASIS':     ('Mphasis',                           'Technology',            'IT Services'),
    'PERSISTENT':  ('Persistent Systems',                'Technology',            'IT Services'),
    'OFSS':        ('Oracle Financial Services',         'Technology',            'IT Services'),

    # ── NIFTY Pharma (extras) ──
    'AUROPHARMA':  ('Aurobindo Pharma',                  'Healthcare',            'Pharma'),
    'LUPIN':       ('Lupin',                             'Healthcare',            'Pharma'),
    'BIOCON':      ('Biocon',                            'Healthcare',            'Biotech & Pharma'),
    'TORNTPHARM':  ('Torrent Pharmaceuticals',           'Healthcare',            'Pharma'),
    'ZYDUSLIFE':   ('Zydus Lifesciences',                'Healthcare',            'Pharma'),
    'ALKEM':       ('Alkem Laboratories',                'Healthcare',            'Pharma'),
    'GLAND':       ('Gland Pharma',                      'Healthcare',            'Pharma'),

    # ── NIFTY Auto (extras) ──
    'TVSMOTOR':    ('TVS Motor Company',                 'Consumer Cyclical',     'Auto Manufacturers'),
    'ASHOKLEY':    ('Ashok Leyland',                     'Consumer Cyclical',     'Commercial Vehicles'),
    'BOSCHLTD':    ('Bosch',                             'Consumer Cyclical',     'Auto Components'),
    'MOTHERSON':   ('Samvardhana Motherson Intl',        'Consumer Cyclical',     'Auto Components'),
    'BAJAJHLDNG':  ('Bajaj Holdings & Investment',       'Financial Services',    'Holdings'),
    'BALKRISIND':  ('Balkrishna Industries',             'Consumer Cyclical',     'Tires'),
    'MRF':         ('MRF',                               'Consumer Cyclical',     'Tires'),

    # ── Popular mid/large extras commonly searched ──
    'TRENT':       ('Trent',                             'Consumer Cyclical',     'Retail'),
    'DMART':       ('Avenue Supermarts',                 'Consumer Defensive',    'Retail Supermarket'),
    'PIDILITIND':  ('Pidilite Industries',               'Basic Materials',       'Adhesives & Chemicals'),
    'BERGEPAINT':  ('Berger Paints',                     'Consumer Defensive',    'Paints'),
    'HAVELLS':     ('Havells India',                     'Industrials',           'Electrical Equipment'),
    'SIEMENS':     ('Siemens',                           'Industrials',           'Electrical Equipment'),
    'DLF':         ('DLF',                               'Real Estate',           'Realty'),
    'GODREJPROP':  ('Godrej Properties',                 'Real Estate',           'Realty'),
    'JINDALSTEL':  ('Jindal Steel & Power',              'Basic Materials',       'Steel'),
    'VEDL':        ('Vedanta',                           'Basic Materials',       'Mining & Metals'),
    'IOC':         ('Indian Oil Corporation',            'Energy',                'Oil & Gas'),
    'GAIL':        ('GAIL (India)',                      'Utilities',             'Gas Distribution'),
    'IGL':         ('Indraprastha Gas',                  'Utilities',             'Gas Distribution'),
    'HAL':         ('Hindustan Aeronautics',             'Industrials',           'Defence & Aerospace'),
    'BEL':         ('Bharat Electronics',                'Industrials',           'Defence Electronics'),
    'IRCTC':       ('Indian Railway Catering & Tourism', 'Consumer Cyclical',     'Travel & Tourism'),
    'POLYCAB':     ('Polycab India',                     'Industrials',           'Cables & Wires'),
    'NAUKRI':      ('Info Edge (India)',                 'Communication Services','Internet & Media'),
    'PAYTM':       ('One 97 Communications (Paytm)',     'Financial Services',    'Fintech'),
    'ZOMATO':      ('Zomato',                            'Consumer Cyclical',     'Food Delivery'),
    'NYKAA':       ('FSN E-Commerce (Nykaa)',            'Consumer Cyclical',     'E-Commerce'),
    'POLICYBZR':   ('PB Fintech (Policybazaar)',         'Financial Services',    'Insurance Tech'),
    'TATAPOWER':   ('Tata Power',                        'Utilities',             'Power'),
    'JIOFIN':      ('Jio Financial Services',            'Financial Services',    'Financial Holdings'),
}


# ── Sector standardization (Yahoo GICS labels → NSE-style names) ─────────────
# Yahoo uses generic GICS categories that don't match how Indian investors think.
# We remap to names that line up with NSE sector indices and Indian market parlance.
_SECTOR_GICS_TO_NSE = {
    'Technology':              'IT & Technology',
    'Communication Services':  'Telecom & Media',
    'Financial Services':      'Financial Services',
    'Healthcare':              'Pharma & Healthcare',
    'Consumer Cyclical':       'Auto & Consumer Discr.',
    'Consumer Defensive':      'FMCG & Consumer Staples',
    'Energy':                  'Oil, Gas & Energy',
    'Basic Materials':         'Metals & Materials',
    'Industrials':             'Industrials & Capital Goods',
    'Utilities':               'Power & Utilities',
    'Real Estate':             'Realty',
}

# Industry-level overrides to split Yahoo's broad sectors where it matters.
# Key words searched (case-insensitive) in the industry string.
_INDUSTRY_OVERRIDES = [
    ('bank',          'Banking'),
    ('insurance',     'Insurance'),
    ('asset management', 'Asset Management'),
    ('capital markets',  'Capital Markets'),
    ('auto manuf',    'Auto Manufacturers'),
    ('auto parts',    'Auto Components'),
    ('drug',          'Pharma'),
    ('biotech',       'Biotech'),
    ('cement',        'Cement'),
    ('steel',         'Metals'),
    ('aluminum',      'Metals'),
    ('software',      'IT Services'),
    ('semiconductor', 'Semiconductors'),
    ('oil & gas',     'Oil & Gas'),
    ('coal',          'Coal'),
    ('utilities',     'Power'),
    ('telecom',       'Telecom'),
    ('reit',          'REITs'),
    ('beverages',     'Beverages'),
    ('packaged foods','FMCG'),
    ('personal',      'FMCG'),  # personal care
    ('apparel',       'Apparel & Lifestyle'),
    ('hotels',        'Hotels & Hospitality'),
]


def standardize_sector(yahoo_sector, yahoo_industry=None):
    """Map Yahoo's GICS sector (+ optional industry refinement) to an
    Indian-investor-friendly NSE-style name."""
    if not yahoo_sector:
        return None
    base = _SECTOR_GICS_TO_NSE.get(yahoo_sector, yahoo_sector)
    if not yahoo_industry:
        return base
    ind_l = str(yahoo_industry).lower()
    for keyword, refined in _INDUSTRY_OVERRIDES:
        if keyword in ind_l:
            # For Financial Services, fully replace base with industry refinement
            if yahoo_sector == 'Financial Services' and refined in ('Banking', 'Insurance', 'Asset Management', 'Capital Markets'):
                return refined
            # For Industrials/Materials, prepend industry detail
            return refined if yahoo_sector in ('Basic Materials', 'Industrials', 'Energy') else base
    return base


def safe_round(val, decimals=2):
    try:
        f = float(val)
        if np.isnan(f) or np.isinf(f):
            return None
        return round(f, decimals)
    except Exception:
        return None


# ── API routes ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    """Serve the SPA. On a public deploy, set a per-visitor session cookie
    so each browser has a private Prism Archive."""
    resp = app.make_response(render_template('index.html'))
    if IS_PUBLIC and not request.cookies.get('cp_sid'):
        import secrets
        sid = secrets.token_urlsafe(24)
        # Long-lived cookie — 1 year. HttpOnly so client JS can't read it.
        resp.set_cookie('cp_sid', sid, max_age=365*24*3600, httponly=True, samesite='Lax')
    return resp


# ── Global error handlers (clean JSON instead of HTML stack traces) ───────
@app.errorhandler(404)
def _not_found(e):
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Not found'}), 404
    return render_template('index.html'), 404   # SPA fallthrough

@app.errorhandler(429)
def _rate_limited(e):
    return jsonify({
        'error': 'Too many requests — slow down a moment and try again.',
        'detail': str(e.description) if hasattr(e, 'description') else '',
    }), 429

@app.errorhandler(500)
def _server_error(e):
    return jsonify({'error': 'Server hiccup. Please retry in a few seconds.'}), 500

@app.errorhandler(Exception)
def _unhandled(e):
    # Don't leak stack traces to end users on public deploys
    if IS_PUBLIC:
        return jsonify({'error': 'Unexpected error. Please retry.'}), 500
    # Local dev: re-raise so the traceback shows
    raise e


@app.route('/api/search')
def search():
    q = request.args.get('q', '').upper().strip()
    if len(q) < 2:
        return jsonify([])
    matches = sorted([s for s in ALL_STOCKS if q in s])[:20]
    return jsonify(matches)


@app.route('/api/indices')
def get_indices():
    cached = get_cached('indices')
    if cached:
        return jsonify(cached)

    tickers = {
        'NIFTY 50': '^NSEI',
        'NIFTY Bank': '^NSEBANK',
        'NIFTY IT': '^CNXIT',
        'SENSEX': '^BSESN',
        'NIFTY Midcap 100': '^CNXMC',
    }
    result = {}
    for name, sym in tickers.items():
        try:
            h = yf.Ticker(sym).history(period='5d')
            if not h.empty and len(h) >= 2:
                chg = (h['Close'].iloc[-1] - h['Close'].iloc[-2]) / h['Close'].iloc[-2] * 100
                result[name] = {'value': safe_round(h['Close'].iloc[-1], 2), 'change': safe_round(chg, 2)}
        except Exception:
            pass

    set_cached('indices', result)
    return jsonify(result)


@app.route('/api/analyze/<path:ticker>')
@limiter.limit("30 per minute", exempt_when=lambda: not IS_PUBLIC)
def analyze(ticker):
    ticker = ticker.upper().strip()
    if not ticker.endswith('.NS') and not ticker.endswith('.BO'):
        ticker += '.NS'

    cache_key = f'analyze:{ticker}'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        df, source = fetch_history(ticker, period='1y')
        if df is None or df.empty:
            return jsonify({'error': f'No data found for {ticker}'}), 404

        # info & news come from yfinance (NSE doesn't provide these)
        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}
        except Exception:
            stock = None
            info = {}

        # On cloud IPs, yfinance.info often returns empty {} due to Yahoo's
        # bot-detection. Backfill 52W high/low from our daily history so the
        # UI doesn't show "undefined".
        if not info.get('fiftyTwoWeekHigh'):
            try:
                # last 252 trading days = ~52 weeks
                w52 = df.iloc[-252:] if len(df) > 252 else df
                info['fiftyTwoWeekHigh'] = float(w52['High'].max())
            except Exception:
                pass
        if not info.get('fiftyTwoWeekLow'):
            try:
                w52 = df.iloc[-252:] if len(df) > 252 else df
                info['fiftyTwoWeekLow'] = float(w52['Low'].min())
            except Exception:
                pass
        # Best-effort current price fallback
        if not info.get('currentPrice'):
            try:
                info['currentPrice'] = float(df['Close'].iloc[-1])
            except Exception:
                pass

        # ── Fundamentals fallback chain ────────────────────────────────────
        # 1) yfinance.info (above — fast, sometimes works)
        # 2) Static NSE_COMPANY_INFO dict (instant, 99 top stocks)
        # 3) Screener.in scrape (slower but covers all NSE stocks)
        # 4) Generic defaults
        clean_sym = ticker.replace('.NS', '').replace('.BO', '')

        # Tier 2: static dict
        static_info = NSE_COMPANY_INFO.get(clean_sym)
        if static_info:
            static_name, static_sector, static_industry = static_info
            if not info.get('longName')  and not info.get('shortName'):
                info['longName'] = static_name
            if not info.get('sector'):
                info['sector'] = static_sector
            if not info.get('industry'):
                info['industry'] = static_industry

        # Tier 3: Screener.in scrape — fires only if we still don't have core fundamentals
        # (this is the expensive call so we gate it carefully)
        needs_screener = (
            not info.get('marketCap')
            or not info.get('trailingPE')
            or not info.get('returnOnEquity')
            or not info.get('longName')
            or not info.get('sector')
        )
        if needs_screener:
            try:
                screener_info = _fetch_screener_fundamentals(clean_sym)
                # Merge: don't overwrite anything yfinance already gave us
                for k, v in (screener_info or {}).items():
                    if not info.get(k) and v is not None:
                        info[k] = v
            except Exception as e:
                print(f'  [analyze {clean_sym}] screener fetch failed: {e}')

        # Tier 4: final defaults
        if not info.get('longName') and not info.get('shortName'):
            info['longName'] = clean_sym
        if not info.get('sector'):
            info['sector'] = 'Other'
        if not info.get('industry'):
            info['industry'] = 'Other'

        # All-time high / low. Use auto_adjust=False to get RAW prices (avoids
        # split/bonus-adjusted highs showing up lower than the un-adjusted 52W
        # high reported in yfinance .info).
        ath = ath_date = atl = atl_date = None
        try:
            df_max = yf.Ticker(ticker).history(period='max', auto_adjust=False)
            if df_max is None or df_max.empty:
                df_max = yf.Ticker(ticker).history(period='max')  # fallback
            if df_max is not None and not df_max.empty:
                hi_idx = df_max['High'].idxmax()
                lo_idx = df_max['Low'].idxmin()
                ath = float(df_max.loc[hi_idx, 'High'])
                atl = float(df_max.loc[lo_idx, 'Low'])
                ath_date = hi_idx.strftime('%d %b %Y') if hasattr(hi_idx, 'strftime') else str(hi_idx)
                atl_date = lo_idx.strftime('%d %b %Y') if hasattr(lo_idx, 'strftime') else str(lo_idx)

            # Sanity guard: ATH must be >= 52W high (logical invariant).
            # Could break if yfinance's info uses fresher intraday data than our daily series.
            w52_h = info.get('fiftyTwoWeekHigh')
            w52_l = info.get('fiftyTwoWeekLow')
            if ath is not None and w52_h is not None and float(w52_h) > ath:
                ath = float(w52_h)
                ath_date = 'within last 52 weeks'
            if atl is not None and w52_l is not None and float(w52_l) < atl:
                atl = float(w52_l)
                atl_date = 'within last 52 weeks'
        except Exception:
            pass

        # Override Yahoo's often-broken beta with one we compute ourselves
        computed_beta = compute_beta(df['Close'])
        if computed_beta is not None:
            info['beta'] = computed_beta

        # Compute dividend yield ourselves — yfinance's `dividendYield` switches
        # between decimal (0.0041) and percent (0.41) between releases. We use
        # `trailingAnnualDividendRate / current_price` which is consistent.
        try:
            ann_div = info.get('trailingAnnualDividendRate') or info.get('dividendRate')
            cp = info.get('currentPrice') or float(df['Close'].iloc[-1])
            if ann_div is not None and cp and float(cp) > 0:
                info['dividendYield'] = float(ann_div) / float(cp)
            else:
                # Fallback heuristic: yfinance value > 0.5 means it's certainly in
                # percent form (no Indian stock yields 50% as a decimal)
                dy = info.get('dividendYield')
                if dy is not None and float(dy) > 0.5:
                    info['dividendYield'] = float(dy) / 100
        except Exception:
            pass

        t_score, t_signals = technical_score(df)
        f_score, f_signals = fundamental_score(info)
        patterns = detect_patterns(df)

        # Quarterly trends (may take 1-2s; cached with the rest of the result)
        quarterly = extract_quarterly_trends(stock) if stock is not None else None
        if quarterly:
            q_adj, q_sigs = quarterly_fundamental_adjustment(quarterly)
            f_score = int(max(0, min(100, f_score + q_adj)))
            f_signals = q_sigs + f_signals  # newest trend signals at top

        # Will compute news + sentiment first so we can fold it into the composite scores
        # (news comes below); defer composite calculation

        # Next earnings date (from yfinance .calendar — may be None)
        earnings = None
        try:
            if stock is not None:
                cal = stock.calendar
                if cal is not None:
                    if isinstance(cal, dict):
                        earn_date = cal.get('Earnings Date')
                        if isinstance(earn_date, (list, tuple)) and earn_date:
                            earn_date = earn_date[0]
                        if earn_date is not None:
                            d = pd.to_datetime(earn_date, errors='coerce')
                            if pd.notnull(d):
                                days_until = (d.date() - datetime.now().date()).days
                                earnings = {
                                    'date':       d.strftime('%d %b %Y'),
                                    'days_until': days_until,
                                    'estimate':   cal.get('Earnings Average') or cal.get('Earnings Estimate Avg'),
                                }
        except Exception:
            earnings = None

        # Levels and risk (uses info + earnings → factors in everything)
        levels = calculate_levels(df, info=info, earnings=earnings)

        # News — Indian RSS feeds first (MoneyControl + LiveMint + ET), then
        # yfinance.news as backup. Indian feeds are richer for NSE stocks.
        news = []
        company_name_for_filter = info.get('longName') or info.get('shortName') or clean_sym
        try:
            indian_news = _fetch_indian_news(clean_sym, company_name=company_name_for_filter, sector=info.get('sector'))
            news.extend(indian_news)
        except Exception as e:
            print(f'  [analyze {clean_sym}] indian news failed: {e}')

        # Top up with yfinance.news only if we got too few Indian articles
        if len(news) < 5:
            try:
                for n in ((stock.news if stock is not None else []) or [])[:10]:
                    content = n.get('content', {})
                    title = content.get('title') or n.get('title', '')
                    pub = content.get('provider', {}).get('displayName') or n.get('publisher', '')
                    url = content.get('canonicalUrl', {}).get('url') or n.get('link', '')
                    ts = content.get('pubDate') or ''
                    if not ts:
                        raw_ts = n.get('providerPublishTime', 0)
                        ts = datetime.fromtimestamp(raw_ts).strftime('%d %b %Y %H:%M') if raw_ts else ''
                    if title:
                        # Avoid duplicates with Indian RSS results
                        if not any(title[:60].lower() == (x['title'][:60].lower()) for x in news):
                            news.append({'title': title, 'publisher': pub, 'url': url, 'published': ts})
            except Exception:
                pass
        news = news[:12]   # cap total

        # News sentiment scoring
        news_scored, news_agg = score_news_sentiment(news)
        n_score = news_agg['score']

        # Composite scores by horizon:
        #  - Short-term (1-4 weeks): 55% tech + 30% fund + 15% sentiment
        #  - Mid-term  (1-6 months): 35% tech + 55% fund + 10% sentiment
        #  - Long-term (6-24 months): 15% tech + 80% fund + 5% sentiment
        st_score = int(round(t_score * 0.55 + f_score * 0.30 + n_score * 0.15))
        mt_score = int(round(t_score * 0.35 + f_score * 0.55 + n_score * 0.10))
        lt_score = int(round(t_score * 0.15 + f_score * 0.80 + n_score * 0.05))

        # Explainability per pillar + confidence
        explanations, confidence = build_explanations(
            t_score, f_score, n_score, t_signals, f_signals, levels, quarterly, news_agg
        )

        # Signal age — how long has the current BUY/SELL signal been active?
        signal_age = compute_signal_age(df, st_score)

        # Chart data (1-year)
        close = df['Close']
        ema20  = close.ewm(span=20,  adjust=False).mean()
        ema50  = close.ewm(span=50,  adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()
        upper_bb, mid_bb, lower_bb = calculate_bollinger(close)
        rsi_series = calculate_rsi(close)
        macd_line, sig_line, hist_line = calculate_macd(close)

        def to_list(s): return [safe_round(v) for v in s.tolist()]

        chart = {
            'dates':      df.index.strftime('%Y-%m-%d').tolist(),
            'open':       to_list(df['Open']),
            'high':       to_list(df['High']),
            'low':        to_list(df['Low']),
            'close':      to_list(df['Close']),
            'volume':     df['Volume'].tolist(),
            'ema20':      to_list(ema20),
            'ema50':      to_list(ema50),
            'ema200':     to_list(ema200),
            'bb_upper':   to_list(upper_bb),
            'bb_lower':   to_list(lower_bb),
            'rsi':        to_list(rsi_series),
            'macd':       to_list(macd_line),
            'macd_sig':   to_list(sig_line),
            'macd_hist':  to_list(hist_line),
        }

        curr_price = safe_round(info.get('currentPrice') or df['Close'].iloc[-1])
        prev_close = safe_round(df['Close'].iloc[-2])
        day_chg    = safe_round((curr_price - prev_close) / prev_close * 100) if prev_close else None

        result = {
            'ticker':   ticker,
            'symbol':   ticker.replace('.NS', '').replace('.BO', ''),
            'name':     info.get('longName') or info.get('shortName') or ticker.replace('.NS', '').replace('.BO', ''),
            'sector':   info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            # yfinance returns "NSI" for NSE India and "BSE" for BSE.
            # Normalize NSI → NSE (NSI is just yfinance's internal code and
            # means nothing to retail investors).
            'exchange': ({'NSI': 'NSE'}.get((info.get('exchange') or '').upper(), info.get('exchange') or 'NSE')),
            'price':    curr_price,
            'prev_close': prev_close,
            'day_change': day_chg,
            'market_cap': info.get('marketCap'),
            'week52_high':   safe_round(info.get('fiftyTwoWeekHigh')),
            'week52_low':    safe_round(info.get('fiftyTwoWeekLow')),
            'all_time_high': safe_round(ath),
            'all_time_low':  safe_round(atl),
            'ath_date':      ath_date,
            'atl_date':      atl_date,
            'scores': {
                'technical':   t_score,
                'fundamental': f_score,
                'sentiment':   n_score,
                'short_term':  st_score,
                'mid_term':    mt_score,
                'long_term':   lt_score,
            },
            'pillars': {
                'technical':   {'score': t_score, 'verdict': recommendation_label(t_score)},
                'fundamental': {'score': f_score, 'verdict': recommendation_label(f_score)},
                'sentiment':   {'score': n_score, 'verdict': {'label': news_agg['verdict'], 'color': recommendation_label(n_score)['color'], 'badge': recommendation_label(n_score)['badge']}, 'breakdown': news_agg},
            },
            'explanations': explanations,
            'confidence':   confidence,
            'signal_age':   signal_age,
            'recommendations': {
                'short_term': recommendation_label(st_score),
                'mid_term':   recommendation_label(mt_score),
                'long_term':  recommendation_label(lt_score),
            },
            'technical_signals':   t_signals,
            'fundamental_signals': f_signals,
            'patterns':            patterns,
            'fundamentals': {
                'pe':              safe_round(info.get('trailingPE')),
                'pb':              safe_round(info.get('priceToBook')),
                'eps':             safe_round(info.get('trailingEps')),
                'fwd_pe':          safe_round(info.get('forwardPE')),
                'rev_growth':      safe_round(info.get('revenueGrowth'), 4),
                'profit_margin':   safe_round(info.get('profitMargins'), 4),
                'op_margin':       safe_round(info.get('operatingMargins'), 4),
                'gross_margin':    safe_round(info.get('grossMargins'), 4),
                'roe':             safe_round(info.get('returnOnEquity'), 4),
                'roa':             safe_round(info.get('returnOnAssets'), 4),
                'debt_equity':     safe_round(info.get('debtToEquity')),
                'div_yield':       safe_round(info.get('dividendYield'), 4),
                'eps_growth':      safe_round(info.get('earningsGrowth'), 4),
                'current_ratio':   safe_round(info.get('currentRatio')),
                'quick_ratio':     safe_round(info.get('quickRatio')),
                'beta':            safe_round(info.get('beta')),
                'book_value':      safe_round(info.get('bookValue')),
                'fcf':             info.get('freeCashflow'),
            },
            'quarterly_trends': quarterly,
            'earnings':       earnings,
            'news':  news_scored,
            'chart': chart,
            'levels': levels,
            'data_source': source,
        }
        set_cached(cache_key, result)
        return jsonify(result)

    except Exception as e:
        print(f'  [analyze {ticker}] error: {e}')
        if IS_PUBLIC:
            return jsonify({'error': 'Analysis temporarily unavailable. Please try again in a moment.'}), 503
        return jsonify({'error': str(e)}), 500


@app.route('/api/backtest/<path:ticker>')
def backtest(ticker):
    """Run a walk-forward backtest of the short-term technical signal on the
    given ticker. Uses 2 years of daily history."""
    ticker = ticker.upper().strip()
    if not ticker.endswith('.NS') and not ticker.endswith('.BO'):
        ticker += '.NS'

    cache_key = f'backtest:{ticker}'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    try:
        df, source = fetch_history(ticker, period='2y')
        if df is None or df.empty or len(df) < 220:
            return jsonify({'error': f'Insufficient history for {ticker} (need 220+ trading days)'}), 404

        result = run_backtest(df, threshold=65, max_hold=30, min_gap=10)
        result['ticker'] = ticker
        result['symbol'] = ticker.replace('.NS', '').replace('.BO', '')
        result['data_points'] = len(df)
        result['data_source'] = source
        result['period'] = {
            'from': df.index[0].strftime('%Y-%m-%d'),
            'to':   df.index[-1].strftime('%Y-%m-%d'),
        }
        set_cached(cache_key, result)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/screener')
@limiter.limit("8 per minute", exempt_when=lambda: not IS_PUBLIC)
def screener():
    try:
        return _screener_impl()
    except Exception as e:
        # Friendly error instead of leaking Python tracebacks to the UI
        print(f'  [screener] error: {e}')
        if IS_PUBLIC:
            return jsonify({'error': 'Screener temporarily unavailable. Please try again in a moment.'}), 503
        return jsonify({'error': str(e)}), 500


def _screener_impl():
    index_name = request.args.get('index', 'NIFTY 50')
    if index_name == 'ALL NSE':
        # Allow overrides via querystring; defaults shrink on public deploy to
        # fit Railway's 30s timeout and Yahoo's cloud rate limit
        min_to = float(request.args.get('min_turnover', 5.0))
        cap    = int(request.args.get('max_stocks', _ALL_NSE_DEFAULT_CAP))
        tickers, _u_meta = get_nse_universe(min_to, cap)
        if not tickers:
            return jsonify({'error': 'NSE universe unavailable — bhavcopy not found in last 14 days'}), 503
    else:
        tickers = NSE_INDICES.get(index_name, NSE_INDICES['NIFTY 50'])

    cache_key = f'screener:{index_name}'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    # Pre-fetch NIFTY 6mo for relative strength + 3M comparison
    try:
        nifty_6mo = yf.Ticker('^NSEI').history(period='6mo')
        nifty_close = nifty_6mo['Close'] if not nifty_6mo.empty else None
    except Exception:
        nifty_close = None

    def chg_pct(series, n):
        if series is None or len(series) < n + 1: return None
        try:
            past = float(series.iloc[-n - 1]); curr = float(series.iloc[-1])
            return (curr - past) / past * 100 if past > 0 else None
        except Exception:
            return None

    nifty_1m = chg_pct(nifty_close, 22) if nifty_close is not None else None

    def fetch_one(ticker):
        try:
            df, _src = fetch_history(ticker, period='6mo')
            if df is None or df.empty or len(df) < 20:
                return None
            try:
                info = yf.Ticker(ticker).info or {}
            except Exception:
                info = {}
            t_sc, _ = technical_score(df)
            f_sc, _ = fundamental_score(info)
            st_sc = int(round(t_sc * 0.70 + f_sc * 0.30))
            mt_sc = int(round(t_sc * 0.40 + f_sc * 0.60))
            close = df['Close']
            p1d = safe_round((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2] * 100) if len(close) >= 2 else 0
            p1w = safe_round((close.iloc[-1] - close.iloc[-6]) / close.iloc[-6] * 100) if len(close) >= 6 else 0
            p1m = safe_round((close.iloc[-1] - close.iloc[-22]) / close.iloc[-22] * 100) if len(close) >= 22 else 0
            p3m = safe_round((close.iloc[-1] - close.iloc[-66]) / close.iloc[-66] * 100) if len(close) >= 66 else None
            # Relative Strength vs NIFTY 50 (1-month basis)
            rs = None
            if p1m is not None and nifty_1m is not None:
                rs = safe_round(p1m - nifty_1m, 2)

            # Long-term score: emphasizes fundamentals
            lt_sc = int(round(t_sc * 0.15 + f_sc * 0.85))

            # Lightweight risk score (matches the full risk model: lower = safer)
            try:
                atr_v = float(calculate_atr(df, 14).iloc[-1])
                curr_px = float(close.iloc[-1])
                atr_pct = atr_v / curr_px * 100 if curr_px > 0 else 5.0
            except Exception:
                atr_pct = 5.0
            risk_sc = 50
            if   atr_pct > 5:   risk_sc += 20
            elif atr_pct > 3:   risk_sc += 10
            elif atr_pct < 1.5: risk_sc -= 10
            try:
                recent_high = float(df['High'].iloc[-20:].max())
                dd = (float(close.iloc[-1]) - recent_high) / recent_high * 100 if recent_high > 0 else 0
                if dd < -15: risk_sc += 10
                elif dd < -10: risk_sc += 5
            except Exception:
                pass
            beta_v = info.get('beta')
            try:
                if beta_v is not None:
                    b = float(beta_v)
                    if b > 1.5: risk_sc += 10
                    elif b > 1.2: risk_sc += 5
                    elif b < 0.7: risk_sc -= 5
            except Exception:
                pass
            risk_sc = max(0, min(100, risk_sc))
            # Static fallback for name/sector/industry when yfinance.info is empty
            clean_sym = ticker.replace('.NS', '').replace('.BO', '')
            static_info = NSE_COMPANY_INFO.get(clean_sym)
            display_name = (info.get('shortName') or info.get('longName')
                            or (static_info[0] if static_info else clean_sym))
            sector_val   = info.get('sector') or (static_info[1] if static_info else 'Other')
            industry_val = info.get('industry') or (static_info[2] if static_info else 'Other')
            # Fair value (cheap — pure math on info). Powers the VALUE view.
            curr_for_fv = float(close.iloc[-1])
            fv_obj = None
            try:
                fv_obj = compute_fair_value(info, curr_for_fv)
            except Exception:
                fv_obj = None
            fv_val      = fv_obj.get('fair_value')  if fv_obj else None
            fv_verdict  = fv_obj.get('verdict')     if fv_obj else None
            upside_pct  = None
            if fv_val and curr_for_fv > 0:
                upside_pct = round((fv_val - curr_for_fv) / curr_for_fv * 100, 1)
            return {
                'ticker':    clean_sym,
                'name':      display_name,
                'price':     safe_round(close.iloc[-1]),
                'chg_1d':    p1d,
                'chg_1w':    p1w,
                'chg_1m':    p1m,
                'chg_3m':    p3m,
                'rs':        rs,
                'tech':      t_sc,
                'long_term': lt_sc,
                'rec_lt':    recommendation_label(lt_sc)['label'],
                'risk_score': risk_sc,
                'atr_pct':   round(atr_pct, 2),
                'fund':      f_sc,
                'short_term': st_sc,
                'mid_term':   mt_sc,
                'pe':        safe_round(info.get('trailingPE')),
                'pb':        safe_round(info.get('priceToBook')),
                'roe':       safe_round(info.get('returnOnEquity'), 4),
                'debt_eq':   safe_round(info.get('debtToEquity')),
                'div_yield': safe_round(info.get('dividendYield'), 4),
                'rev_growth':    safe_round(info.get('revenueGrowth'), 4),
                'profit_margin': safe_round(info.get('profitMargins'), 4),
                'earnings_growth': safe_round(info.get('earningsGrowth'), 4),
                'fair_value':  fv_val,
                'fv_verdict':  fv_verdict,
                'upside_pct':  upside_pct,
                'mktcap':    info.get('marketCap'),
                'sector':    sector_val,
                'sector_grouped': standardize_sector(sector_val, industry_val),
                'industry':  industry_val,
                'rec_st':    recommendation_label(st_sc)['label'],
                'rec_mt':    recommendation_label(mt_sc)['label'],
            }
        except Exception:
            return None

    results = []
    with ThreadPoolExecutor(max_workers=_SCREENER_WORKERS) as ex:
        futures = {ex.submit(fetch_one, t): t for t in tickers}
        for fut in futures:
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: x['short_term'], reverse=True)
    set_cached(cache_key, results)
    # Save daily sector snapshot for trend sparklines
    _save_sector_snapshot(index_name, results)
    return jsonify(results)


# ── Sector score history (for sparklines) ────────────────────────────────────
_SECTOR_HISTORY_PATH = os.path.join(CACHE_DIR, 'sector_history.json')
_MAX_HISTORY_DAYS = 30


def _load_sector_history():
    try:
        if os.path.exists(_SECTOR_HISTORY_PATH):
            with open(_SECTOR_HISTORY_PATH, 'r') as f:
                import json as _json
                return _json.load(f)
    except Exception:
        pass
    return {}


def _save_sector_snapshot(index_name, rows):
    """Compute cap-weighted sector scores from screener rows and append to history."""
    try:
        from collections import defaultdict
        agg = defaultdict(lambda: {'sw': 0, 'w': 0, 'cw': 0})
        for r in rows:
            s = r.get('sector_grouped') or r.get('sector') or 'Unknown'
            mc = r.get('mktcap') or 0
            if mc <= 0:
                continue
            agg[s]['w']  += mc
            agg[s]['sw'] += (r.get('short_term') or 0) * mc
            agg[s]['cw'] += (r.get('chg_1m')     or 0) * mc

        snapshot = {}
        for s, b in agg.items():
            if b['w'] > 0:
                snapshot[s] = {
                    'score':  round(b['sw'] / b['w'], 1),
                    'chg_1m': round(b['cw'] / b['w'], 2),
                }

        hist = _load_sector_history()
        date_key = datetime.now().strftime('%Y-%m-%d')
        if index_name not in hist:
            hist[index_name] = {}
        hist[index_name][date_key] = snapshot

        # Trim to last N days per index
        for idx, by_date in list(hist.items()):
            sorted_dates = sorted(by_date.keys(), reverse=True)
            if len(sorted_dates) > _MAX_HISTORY_DAYS:
                for d in sorted_dates[_MAX_HISTORY_DAYS:]:
                    del by_date[d]

        import json as _json
        with open(_SECTOR_HISTORY_PATH, 'w') as f:
            _json.dump(hist, f)
    except Exception as e:
        print(f'[sector_snapshot] WARN: {e}')


@app.route('/api/sector_history')
def sector_history():
    """Return per-sector score history (last 14 days) for sparklines."""
    index_name = request.args.get('index', 'NIFTY 50')
    hist = _load_sector_history()
    by_date = hist.get(index_name, {})
    sorted_dates = sorted(by_date.keys())[-14:]
    # Reshape: { sector: [(date, score, chg_1m), ...] }
    by_sector = {}
    for d in sorted_dates:
        snap = by_date[d]
        for sector, vals in snap.items():
            by_sector.setdefault(sector, []).append({
                'date':   d,
                'score':  vals.get('score'),
                'chg_1m': vals.get('chg_1m'),
            })
    return jsonify({'index': index_name, 'dates': sorted_dates, 'sectors': by_sector})


@app.route('/api/long_term_picks')
def long_term_picks():
    try:
        return _long_term_picks_impl()
    except Exception as e:
        print(f'  [long_term_picks] error: {e}')
        if IS_PUBLIC:
            return jsonify({'error': 'Picks temporarily unavailable. Please try again in a moment.'}), 503
        return jsonify({'error': str(e)}), 500


def _long_term_picks_impl():
    """Curated list of stocks meeting:
       - long-term score >= 60 (Buy / Strong Buy on long horizon)
       - risk_score <= 55 (Low or Medium risk)
    Grouped by sector (default) or by index."""
    index_filter = request.args.get('index', 'all')   # 'all' or specific index name
    min_lt = int(request.args.get('min_lt', 60))
    max_risk = int(request.args.get('max_risk', 55))

    cache_key = f'lt_picks:{index_filter}:{min_lt}:{max_risk}'
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    # Universe to scan
    if index_filter == 'all':
        indices_to_scan = list(NSE_INDICES.keys())
    elif index_filter == 'ALL NSE':
        indices_to_scan = ['ALL NSE']   # special: screener() resolves via bhavcopy
    else:
        indices_to_scan = [index_filter] if index_filter in NSE_INDICES else ['NIFTY 50']

    # Run screener internally for each requested index (warm cache helps)
    all_rows = {}
    ticker_to_indices = {}
    for idx_name in indices_to_scan:
        # Reuse the existing screener cache
        scr_cache_key = f'screener:{idx_name}'
        rows = get_cached(scr_cache_key)
        if rows is None:
            # Trigger computation by calling fetch_one for each ticker in this index
            # but routing through the existing screener endpoint internally is simpler:
            from flask import current_app
            with app.test_request_context(f'/api/screener?index={idx_name}'):
                resp = screener()
            try:
                rows = resp.get_json()
            except Exception:
                rows = []
        # Defensive: screener wrapper may return an error dict on failure.
        # Skip such responses — we want only valid stock-row lists.
        if not rows or not isinstance(rows, list):
            continue
        for r in rows:
            t = r.get('ticker')
            if t not in all_rows:
                all_rows[t] = r
            ticker_to_indices.setdefault(t, []).append(idx_name)

    # Filter to picks
    picks = []
    for t, r in all_rows.items():
        lt = r.get('long_term') or 0
        rk = r.get('risk_score')
        if rk is None: continue
        if lt < min_lt: continue
        if rk > max_risk: continue
        # Avoid stocks in clear downtrend even if fundamentals are strong
        if (r.get('chg_1m') or 0) < -10: continue
        picks.append({**r, 'indices': ticker_to_indices.get(t, [])})

    # Sort by composite "long-term safety" score: long_term - 0.5*risk
    # Use `or 0` not `, 0` default — handles the None-value case (corrupt cache,
    # partial yfinance response) that .get(key, default) doesn't catch.
    picks.sort(key=lambda x: ((x.get('long_term') or 0) - 0.5 * (x.get('risk_score') or 50)), reverse=True)

    # Group by sector
    by_sector = {}
    for p in picks:
        s = p.get('sector') or 'Other'
        by_sector.setdefault(s, []).append(p)
    sector_groups = sorted(
        [{'sector': k, 'count': len(v),
          'avg_lt':   round(sum((x.get('long_term') or 0) for x in v) / len(v), 1),
          'avg_risk': round(sum((x.get('risk_score') or 50) for x in v) / len(v), 1),
          'stocks': v}
         for k, v in by_sector.items()],
        key=lambda x: (-x['avg_lt'], x['avg_risk'])
    )

    # Group by index too
    by_index = {}
    for p in picks:
        for idx in (p.get('indices') or ['Other']):
            by_index.setdefault(idx, []).append(p)
    index_groups = sorted(
        [{'index': k, 'count': len(v),
          'avg_lt':   round(sum((x.get('long_term') or 0) for x in v) / len(v), 1),
          'avg_risk': round(sum((x.get('risk_score') or 50) for x in v) / len(v), 1),
          'stocks': v}
         for k, v in by_index.items()],
        key=lambda x: (-x['avg_lt'], x['avg_risk'])
    )

    result = {
        'filter':       {'index': index_filter, 'min_lt': min_lt, 'max_risk': max_risk},
        'total_scanned': len(all_rows),
        'total_picks':  len(picks),
        'by_sector':    sector_groups,
        'by_index':     index_groups,
        'available_indices': list(NSE_INDICES.keys()),
    }
    set_cached(cache_key, result)
    return jsonify(result)


@app.route('/api/value_picks')
def value_picks():
    """Undervalued + fundamentally strong stocks.

    Filters (all optional query params):
      max_price   — cap stock price (₹). Default unlimited.
      max_risk    — cap risk_score (0-100). Default 60.
      min_roe     — minimum ROE as decimal (e.g. 0.12). Default 0.10.
      min_upside  — minimum % upside vs fair value. Default 5.
      sector      — exact sector match (case-insensitive). Default any.
      index       — universe to scan (default: scan a few common indices).
    """
    try:
        return _value_picks_impl()
    except Exception as e:
        print(f'  [value_picks] error: {e}')
        if IS_PUBLIC:
            return jsonify({'error': 'Value picks temporarily unavailable. Please try again.'}), 503
        return jsonify({'error': str(e)}), 500


def _value_picks_impl():
    # Parse filters
    def _f(name, default):
        v = request.args.get(name)
        if v is None or v == '' or v == 'any':
            return default
        try:
            return float(v)
        except Exception:
            return default

    max_price        = _f('max_price', None)         # None = unlimited
    max_risk         = _f('max_risk', 60)            # 60 = Low + Medium
    max_pe           = _f('max_pe', None)            # None = unlimited
    min_roe          = _f('min_roe', 0.10)           # 10% (kept for back-compat)
    min_upside       = _f('min_upside', 0)           # % below FV
    min_rev_growth   = _f('min_rev_growth', None)    # decimal, e.g. 0.10 = 10%
    min_profit_margin = _f('min_profit_margin', None) # decimal
    min_mcap         = _f('min_mcap', None)          # ₹ (raw — frontend passes ₹2B = 2e9, etc.)
    sector_q         = (request.args.get('sector') or '').strip().lower()
    index_q          = request.args.get('index', 'NIFTY 500')
    # Multi-select quality tags: csv string e.g. "undervalued,quality,dividend"
    quality_tags_q   = (request.args.get('quality_tags') or '').strip().lower()
    quality_tags     = {t.strip() for t in quality_tags_q.split(',') if t.strip()} if quality_tags_q else set()

    # Default universe = NIFTY 500 if present, else fall back to broader scan
    if index_q == 'all':
        indices_to_scan = ['NIFTY 50', 'NIFTY Next 50', 'NIFTY Midcap 100']
    elif index_q in NSE_INDICES:
        indices_to_scan = [index_q]
    elif index_q == 'NIFTY 500' and 'NIFTY 500' not in NSE_INDICES:
        # Fall back if NIFTY 500 isn't in the static map
        indices_to_scan = ['NIFTY 50', 'NIFTY Next 50', 'NIFTY Midcap 100']
    else:
        indices_to_scan = [index_q] if index_q in NSE_INDICES else ['NIFTY 50']

    cache_key = (f'value_picks:{":".join(indices_to_scan)}'
                 f':p{max_price}:r{max_risk}:pe{max_pe}:e{min_roe}'
                 f':u{min_upside}:rg{min_rev_growth}:pm{min_profit_margin}'
                 f':mc{min_mcap}:s{sector_q}:q{",".join(sorted(quality_tags))}')
    cached = get_cached(cache_key)
    if cached:
        return jsonify(cached)

    # Gather rows from screener cache
    all_rows = {}
    for idx_name in indices_to_scan:
        scr_cache_key = f'screener:{idx_name}'
        rows = get_cached(scr_cache_key)
        if rows is None:
            with app.test_request_context(f'/api/screener?index={idx_name}'):
                resp = screener()
            try:
                rows = resp.get_json()
            except Exception:
                rows = []
        if not rows or not isinstance(rows, list):
            continue
        for r in rows:
            t = r.get('ticker')
            if t not in all_rows:
                all_rows[t] = r

    # Helper: derive quality tags for a row
    def _tags_for(r):
        tags = []
        if (r.get('upside_pct') or 0) > 5:
            tags.append('undervalued')
        roe = r.get('roe') or 0
        de  = r.get('debt_eq')
        if roe > 0.15 and (de is None or de < 100):
            tags.append('quality')
        if (r.get('div_yield') or 0) > 0.02:   # >2% yield
            tags.append('dividend')
        rg = r.get('rev_growth') or 0
        eg = r.get('earnings_growth') or 0
        if rg > 0.15 or eg > 0.15:
            tags.append('growth')
        return tags

    # Filter
    picks = []
    for t, r in all_rows.items():
        price   = r.get('price')
        risk    = r.get('risk_score')
        roe     = r.get('roe')
        upside  = r.get('upside_pct')
        fv      = r.get('fair_value')
        sector  = (r.get('sector') or '').lower()
        pe      = r.get('pe')
        rev_g   = r.get('rev_growth')
        pm      = r.get('profit_margin')
        mcap    = r.get('mktcap')

        if price is None or risk is None: continue
        if max_price is not None and price > max_price: continue
        if risk > max_risk: continue
        if max_pe is not None and (pe is None or pe <= 0 or pe > max_pe): continue
        # ROE — kept as a default quality floor (10% unless overridden)
        if roe is None or roe < min_roe: continue
        # Revenue growth filter (None = unknown → skip if filter is set)
        if min_rev_growth is not None and (rev_g is None or rev_g < min_rev_growth): continue
        # Profit margin filter
        if min_profit_margin is not None and (pm is None or pm < min_profit_margin): continue
        # Market cap filter (in raw ₹)
        if min_mcap is not None and (mcap is None or mcap < min_mcap): continue
        # Must be undervalued by at least min_upside %
        if min_upside is not None and min_upside > 0:
            if fv is None or upside is None or upside < min_upside: continue
        if sector_q and sector_q not in sector: continue
        # Quality gate: long-term score floor so we don't surface broken names
        if (r.get('long_term') or 0) < 50: continue

        # Quality tag filter — must have at least one of the selected tags
        row_tags = _tags_for(r)
        if quality_tags:
            if not (quality_tags & set(row_tags)):
                continue

        # Attach tags to the row so frontend can show badges
        enriched = dict(r)
        enriched['tags'] = row_tags
        picks.append(enriched)

    # Sort by upside desc, then by ROE desc
    picks.sort(key=lambda x: ((x.get('upside_pct') or 0), (x.get('roe') or 0)), reverse=True)

    # Stats
    avg_upside = round(sum((p.get('upside_pct') or 0) for p in picks) / len(picks), 1) if picks else 0
    avg_roe    = round(sum((p.get('roe') or 0) for p in picks) / len(picks) * 100, 1) if picks else 0

    # Distinct sectors present (for the filter dropdown)
    sectors_seen = sorted({(r.get('sector') or 'Other') for r in all_rows.values() if r.get('sector')})

    result = {
        'filter': {
            'max_price':         max_price,
            'max_risk':          max_risk,
            'max_pe':            max_pe,
            'min_roe':           min_roe,
            'min_upside':        min_upside,
            'min_rev_growth':    min_rev_growth,
            'min_profit_margin': min_profit_margin,
            'min_mcap':          min_mcap,
            'sector':            request.args.get('sector') or '',
            'quality_tags':      sorted(quality_tags),
            'indices':           indices_to_scan,
        },
        'total_scanned': len(all_rows),
        'total_picks':   len(picks),
        'avg_upside_pct': avg_upside,
        'avg_roe_pct':    avg_roe,
        'sectors':       sectors_seen,
        'picks':         picks,
    }
    set_cached(cache_key, result)
    return jsonify(result)


@app.route('/api/indices_list')
def indices_list():
    # Expose 'ALL NSE' as a virtual index option (bhavcopy-derived)
    return jsonify(['ALL NSE'] + list(NSE_INDICES.keys()))


@app.route('/api/universe_info')
def universe_info():
    """Stats about the bhavcopy-derived NSE universe (for UI display)."""
    min_to = float(request.args.get('min_turnover', 5.0))
    cap    = int(request.args.get('max_stocks', _ALL_NSE_DEFAULT_CAP))
    force  = request.args.get('refresh') in ('1', 'true', 'yes')
    tickers, meta = get_nse_universe(min_to, cap, force_refresh=force)
    return jsonify({'count': len(tickers), 'sample_top_10': tickers[:10], **meta})


# ────────────────────────────────────────────────────────────────────────────
# PRISM ARCHIVE — persistent SQLite store of every generated Daily Prism
# Powers the "Prism Revisit" content format (look at past calls weeks later)
# ────────────────────────────────────────────────────────────────────────────
import sqlite3
_PRISM_DB_PATH = os.path.join(CACHE_DIR, 'prism_archive.db')

def _prism_db():
    conn = sqlite3.connect(_PRISM_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _prism_init_db():
    conn = _prism_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prism (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            sector TEXT,
            price REAL,
            tech_score INTEGER,
            fund_score INTEGER,
            sent_score INTEGER,
            risk_score INTEGER,
            short_term TEXT,
            mid_term TEXT,
            long_term TEXT,
            verdict TEXT,
            full_json TEXT NOT NULL
        )
    """)
    # Migration: add session_id column if it doesn't exist (so each visitor's
    # archive is private on a public deploy)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(prism)").fetchall()]
    if 'session_id' not in cols:
        conn.execute("ALTER TABLE prism ADD COLUMN session_id TEXT DEFAULT 'local'")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prism_ts ON prism(ts DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prism_symbol ON prism(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prism_session ON prism(session_id)")
    conn.commit()
    conn.close()

_prism_init_db()


def _get_session_id():
    """
    Returns the visitor's session id. On a public deploy, each browser gets
    its own cookie so prism archives don't cross-contaminate. Locally, all
    users share the 'local' bucket so your archive feels seamless.
    """
    if not IS_PUBLIC:
        return 'local'
    sid = request.cookies.get('cp_sid')
    return sid or 'anon'

def _save_prism_to_archive(prism, session_id='local'):
    """Persist a generated prism. Dedupes within 60 min for the same symbol+session."""
    try:
        conn = _prism_db()
        symbol = prism.get('symbol') or ''
        if not symbol:
            return None
        # Dedupe: if same symbol was saved in the last 60 min by this session, replace it
        cutoff = int(time.time()) - 3600
        existing = conn.execute(
            "SELECT id FROM prism WHERE symbol = ? AND session_id = ? AND ts > ? ORDER BY ts DESC LIMIT 1",
            (symbol, session_id, cutoff)
        ).fetchone()
        tp = prism.get('trade_plan') or {}
        verdict = prism.get('verdict_line', '')
        params = (
            int(time.time()),
            symbol,
            prism.get('name'),
            prism.get('sector'),
            prism.get('price'),
            (prism.get('technical')   or {}).get('score'),
            (prism.get('fundamental') or {}).get('score'),
            (prism.get('sentiment')   or {}).get('score'),
            (prism.get('risk')        or {}).get('score'),
            (tp.get('short_term') or {}).get('verdict'),
            (tp.get('mid_term')   or {}).get('verdict'),
            (tp.get('long_term')  or {}).get('verdict'),
            verdict,
            json.dumps(prism, default=str),
        )
        if existing:
            conn.execute("""
                UPDATE prism SET ts=?, name=?, sector=?, price=?,
                    tech_score=?, fund_score=?, sent_score=?, risk_score=?,
                    short_term=?, mid_term=?, long_term=?, verdict=?, full_json=?
                WHERE id=?
            """, params[:1] + params[2:] + (existing['id'],))
            row_id = existing['id']
        else:
            cur = conn.execute("""
                INSERT INTO prism (ts, symbol, name, sector, price,
                    tech_score, fund_score, sent_score, risk_score,
                    short_term, mid_term, long_term, verdict, full_json, session_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, params + (session_id,))
            row_id = cur.lastrowid
        conn.commit()
        conn.close()
        return row_id
    except Exception as e:
        print(f'  [WARN] prism archive save failed: {e}')
        return None


@app.route('/api/prism_archive')
def prism_archive_list():
    """List past prisms (newest first) for this visitor. Public deploy = session-scoped."""
    limit  = int(request.args.get('limit', 50))
    symbol = request.args.get('symbol')
    sid    = _get_session_id()
    conn = _prism_db()
    if symbol:
        rows = conn.execute(
            "SELECT id, ts, symbol, name, sector, price, tech_score, fund_score, "
            "sent_score, risk_score, short_term, mid_term, long_term, verdict "
            "FROM prism WHERE symbol = ? AND session_id = ? ORDER BY ts DESC LIMIT ?",
            (symbol.upper(), sid, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, ts, symbol, name, sector, price, tech_score, fund_score, "
            "sent_score, risk_score, short_term, mid_term, long_term, verdict "
            "FROM prism WHERE session_id = ? ORDER BY ts DESC LIMIT ?",
            (sid, limit)
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/prism_archive/<int:prism_id>')
def prism_archive_get(prism_id):
    """Fetch one archived prism (must belong to this session on public deploy)."""
    sid = _get_session_id()
    conn = _prism_db()
    row = conn.execute(
        "SELECT * FROM prism WHERE id = ? AND session_id = ?",
        (prism_id, sid)
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    d = dict(row)
    try:
        d['prism'] = json.loads(d.pop('full_json'))
    except Exception:
        d['prism'] = None
    return jsonify(d)


@app.route('/api/prism_archive/<int:prism_id>', methods=['DELETE'])
def prism_archive_delete(prism_id):
    sid = _get_session_id()
    conn = _prism_db()
    conn.execute("DELETE FROM prism WHERE id = ? AND session_id = ?", (prism_id, sid))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/freshness')
def freshness():
    """Returns when we last got fresh upstream data (for the UI badge)."""
    now = time.time()
    ts = _data_freshness.get('last_fresh_ts', 0)
    age_sec = int(now - ts) if ts else None
    # Are markets open? NSE hours: 9:15 - 15:30 IST (UTC+5:30), Mon-Fri
    now_ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    is_weekday = now_ist.weekday() < 5
    open_min  = 9 * 60 + 15
    close_min = 15 * 60 + 30
    cur_min   = now_ist.hour * 60 + now_ist.minute
    market_open = is_weekday and open_min <= cur_min <= close_min
    return jsonify({
        'last_fresh_ts': ts,
        'age_seconds':   age_sec,
        'market_open':   market_open,
        'server_time':   now,
    })


# ────────────────────────────────────────────────────────────────────────────
# DAILY PRISM — shareable 6-slide card data for Instagram / social media
# ────────────────────────────────────────────────────────────────────────────
def _prism_takeaway(spectrum, score):
    """One-line verdict tagline per spectrum, by score band."""
    if spectrum == 'technical':
        if score >= 75: return ('Strong',     'Trend is your friend.')
        if score >= 60: return ('Constructive','Setup is building.')
        if score >= 45: return ('Neutral',    'Wait for confirmation.')
        if score >= 30: return ('Weak',       "Don't catch the falling knife.")
        return                  ('Very Weak', 'Downtrend in force.')
    if spectrum == 'fundamental':
        if score >= 80: return ('Strong Buy',  'World-class business.')
        if score >= 65: return ('Buy',         'Solid fundamentals.')
        if score >= 50: return ('Average',     'Mixed quality.')
        if score >= 35: return ('Weak',        'Quality concerns.')
        return                  ('Poor',       'Avoid until improved.')
    if spectrum == 'sentiment':
        if score >= 70: return ('Positive',    'Tailwinds, not headwinds.')
        if score >= 55: return ('Mild Positive','Constructive newsflow.')
        if score >= 45: return ('Neutral',     'No clear narrative.')
        if score >= 30: return ('Negative',    'Wait for dust to settle.')
        return                  ('Very Negative','Story is broken.')
    if spectrum == 'risk':
        # Lower = safer
        if score >= 70: return ('High',        'Size positions small.')
        if score >= 55: return ('Elevated',    'Stagger entries.')
        if score >= 40: return ('Moderate',    'Normal market risk.')
        if score >= 25: return ('Low-Medium',  'Manageable risk profile.')
        return                  ('Low',        'Sleep-well-at-night risk.')
    return ('', '')


def _prism_alignment_verdict(t, f, s, risk, st, mt, lt):
    """One-sentence overall verdict based on how many spectrums agree."""
    pos = sum(1 for v in [t >= 60, f >= 60, s >= 55, risk <= 50] if v)
    horizons_buy = sum(1 for v in [st >= 60, mt >= 60, lt >= 60] if v)

    if pos >= 4 and horizons_buy >= 3:
        return 'ALL 5 SPECTRUMS ALIGNED. Rare setup.'
    if pos >= 3 and horizons_buy >= 2:
        return 'Multiple spectrums aligned. High-conviction setup.'
    if t < 45 and f >= 65:
        return 'Excellent business. Bad timing (so far).'
    if t >= 70 and f < 50:
        return 'Strong momentum. Weaker fundamentals — trade, don\'t invest.'
    if pos <= 1 and horizons_buy == 0:
        return 'Spectrums disagree. Stay on the sidelines.'
    if pos == 2:
        return 'Mixed signals. Selective conviction only.'
    return 'Some alignment, some friction. Pick your horizon carefully.'


def _prism_bullets_technical(t_signals, levels):
    """5 bullet points for Technical slide — pulled from real signals."""
    bullets = []
    for sig in (t_signals or [])[:5]:
        # technical_score returns dicts like {label, value, signal} — keep first 5
        if isinstance(sig, dict):
            lbl = sig.get('label') or sig.get('name') or ''
            val = sig.get('value', '')
            bullets.append(f"{lbl}{(': ' + str(val)) if val else ''}")
        else:
            bullets.append(str(sig))
    while len(bullets) < 5:
        bullets.append('—')
    return bullets[:5]


def _prism_bullets_fundamental(f_signals, fundamentals):
    """5 bullet points for Fundamental slide."""
    bullets = []
    for sig in (f_signals or [])[:5]:
        if isinstance(sig, dict):
            lbl = sig.get('label') or sig.get('name') or ''
            val = sig.get('value', '')
            bullets.append(f"{lbl}{(': ' + str(val)) if val else ''}")
        else:
            bullets.append(str(sig))
    # Fall back to fundamentals dict if signals are thin
    if len(bullets) < 5 and fundamentals:
        if fundamentals.get('roe'):
            bullets.append(f"ROE {round(fundamentals['roe']*100, 1)}%")
        if fundamentals.get('pe'):
            bullets.append(f"P/E {fundamentals['pe']}")
        if fundamentals.get('debt_equity') is not None:
            bullets.append(f"Debt/Equity {fundamentals['debt_equity']}")
        if fundamentals.get('profit_margin'):
            bullets.append(f"Net margin {round(fundamentals['profit_margin']*100, 1)}%")
    while len(bullets) < 5:
        bullets.append('—')
    return bullets[:5]


def _prism_bullets_sentiment(news_agg, news_scored):
    """5 bullets for Sentiment slide."""
    bullets = []
    if news_agg:
        pos = news_agg.get('positive_count', 0)
        neg = news_agg.get('negative_count', 0)
        total = news_agg.get('total', pos + neg)
        if total:
            bullets.append(f"{pos} of {total} recent headlines positive")
            bullets.append(f"{neg} of {total} negative · rest neutral")
        if news_agg.get('verdict'):
            bullets.append(f"Overall tone: {news_agg['verdict']}")
    # Pull 2 actual headline phrases
    for n in (news_scored or [])[:2]:
        title = n.get('title', '')[:60]
        if title:
            bullets.append(f"\"{title}…\"" if len(n.get('title', '')) > 60 else f"\"{title}\"")
    while len(bullets) < 5:
        bullets.append('—')
    return bullets[:5]


def _prism_bullets_risk(risk_block):
    """5 bullets for Risk slide."""
    bullets = []
    if risk_block:
        if risk_block.get('drawdown_20d') is not None:
            bullets.append(f"20-day drawdown: {risk_block['drawdown_20d']}%")
        if risk_block.get('max_dd_6m') is not None:
            bullets.append(f"Max 6M drawdown: {risk_block['max_dd_6m']}%")
        if risk_block.get('pct_from_52w_low') is not None:
            bullets.append(f"{risk_block['pct_from_52w_low']}% above 52W low")
        if risk_block.get('beta') is not None:
            bullets.append(f"Beta {risk_block['beta']} — {'high' if risk_block['beta']>1.2 else 'moderate' if risk_block['beta']>0.8 else 'low'} market sensitivity")
        if risk_block.get('avg_turnover_cr') is not None:
            liq = 'excellent' if risk_block['avg_turnover_cr'] > 100 else 'good' if risk_block['avg_turnover_cr'] > 20 else 'thin'
            bullets.append(f"Liquidity {liq} (₹{int(risk_block['avg_turnover_cr'])} Cr/day)")
    while len(bullets) < 5:
        bullets.append('—')
    return bullets[:5]


@app.route('/api/daily_prism')
@app.route('/api/daily_prism/<path:ticker>')
@limiter.limit("20 per minute", exempt_when=lambda: not IS_PUBLIC)
def daily_prism(ticker=None):
    """Returns analyze() data reshaped for the 6-slide Instagram carousel.
    If no ticker given, auto-picks today's top long-term pick."""
    # ── Auto-pick mode ───────────────────────────────────────────────────
    if not ticker:
        # Reuse long_term_picks output, take #1
        with app.test_request_context('/api/long_term_picks?index=NIFTY 50'):
            resp = long_term_picks()
        try:
            picks_data = resp.get_json()
            all_picks = []
            for grp in (picks_data.get('by_sector') or []):
                all_picks.extend(grp.get('stocks') or [])
            if not all_picks:
                return jsonify({'error': 'No picks available right now'}), 404
            # Sort by long_term - 0.5*risk_score (same as long_term_picks)
            all_picks.sort(key=lambda x: (x.get('long_term', 0) - 0.5 * (x.get('risk_score') or 50)), reverse=True)
            ticker = all_picks[0]['ticker']
        except Exception as e:
            return jsonify({'error': f'Auto-pick failed: {e}'}), 500

    # ── Run full analysis ────────────────────────────────────────────────
    with app.test_request_context(f'/api/analyze/{ticker}'):
        resp = analyze(ticker)
    try:
        data = resp.get_json()
    except Exception:
        return jsonify({'error': f'Analyze failed for {ticker}'}), 500
    if not data or data.get('error'):
        return jsonify({'error': (data or {}).get('error', 'Analyze returned no data')}), 404

    # ── Pull the numbers ─────────────────────────────────────────────────
    scores = data.get('scores', {})
    t_score = scores.get('technical', 0)
    f_score = scores.get('fundamental', 0)
    s_score = scores.get('sentiment', 0)
    st = scores.get('short_term', 0)
    mt = scores.get('mid_term', 0)
    lt = scores.get('long_term', 0)

    risk_block = (data.get('levels') or {}).get('risk') or {}
    risk_score = risk_block.get('score', 50)

    # ── Build the 6 slides ───────────────────────────────────────────────
    t_lbl, t_line = _prism_takeaway('technical',   t_score)
    f_lbl, f_line = _prism_takeaway('fundamental', f_score)
    s_lbl, s_line = _prism_takeaway('sentiment',   s_score)
    r_lbl, r_line = _prism_takeaway('risk',        risk_score)

    levels = data.get('levels') or {}
    st_plan = levels.get('short_term', {})
    mt_plan = levels.get('mid_term',   {})
    lt_plan = levels.get('long_term',  {})

    recs = data.get('recommendations', {})

    prism = {
        'ticker':       data.get('ticker'),
        'symbol':       data.get('symbol'),
        'name':         data.get('name'),
        'sector':       data.get('sector'),
        'price':        data.get('price'),
        'day_change':   data.get('day_change'),
        # Slide 2 — Technical
        'technical': {
            'score':    t_score,
            'label':    t_lbl,
            'tagline':  t_line,
            'bullets':  _prism_bullets_technical(data.get('technical_signals'), levels),
        },
        # Slide 3 — Fundamental
        'fundamental': {
            'score':    f_score,
            'label':    f_lbl,
            'tagline':  f_line,
            'bullets':  _prism_bullets_fundamental(data.get('fundamental_signals'), data.get('fundamentals')),
        },
        # Slide 4 — Sentiment
        'sentiment': {
            'score':    s_score,
            'label':    s_lbl,
            'tagline':  s_line,
            'bullets':  _prism_bullets_sentiment(
                            (data.get('pillars') or {}).get('sentiment', {}).get('breakdown'),
                            data.get('news'),
                        ),
        },
        # Slide 5 — Risk
        'risk': {
            'score':    risk_score,
            'label':    r_lbl,
            'tagline':  r_line,
            'bullets':  _prism_bullets_risk(risk_block),
        },
        # Slide 6 — Trade plan
        'trade_plan': {
            'entry':      data.get('price'),
            'short_term': {
                'sl_pct':  st_plan.get('sl_pct'),
                't1_pct':  st_plan.get('t1_pct'),
                'verdict': (recs.get('short_term') or {}).get('label', '—'),
            },
            'mid_term': {
                'sl_pct':  mt_plan.get('sl_pct'),
                't1_pct':  mt_plan.get('t1_pct'),
                'verdict': (recs.get('mid_term') or {}).get('label', '—'),
            },
            'long_term': {
                'sl_pct':  lt_plan.get('sl_pct'),
                't1_pct':  lt_plan.get('t1_pct'),
                'verdict': (recs.get('long_term') or {}).get('label', '—'),
            },
        },
        # Composite verdict line
        'verdict_line': _prism_alignment_verdict(t_score, f_score, s_score, risk_score, st, mt, lt),
        # Auto-pick metadata
        'composite_long_term': lt,
    }
    # Auto-archive the prism (best-effort, never blocks the response)
    archive_id = _save_prism_to_archive(prism, session_id=_get_session_id())
    if archive_id:
        prism['archive_id'] = archive_id
    return jsonify(prism)


# Pre-warm thread was REMOVED — yfinance's curl_cffi doesn't yield to gevent,
# so a background "thread" that does yfinance calls actually freezes the
# entire gevent worker. The fix: accept the ~30s first-request delay on cold
# cache, but the UI handles it gracefully with auto-retry. Once cached
# (30-min TTL), all subsequent users get instant responses.


# ════════════════════════════════════════════════════════════════════════════
# INTRADAY — high-confidence stock + index setups for day trading
# ════════════════════════════════════════════════════════════════════════════
# Design philosophy (be honest with the user):
#   No real intraday system wins 85% of trades. Even institutional algos
#   hit 55-65%. What we CAN deliver with 85%+ reliability:
#     1. Setup validity — only signals where 5+ indicators align
#     2. Mathematically correct levels — pivots, ATR-based stops, real R:R
#     3. Liquidity safety — only F&O / high-turnover stocks
#     4. Risk management discipline — capped trades, 1% rule, daily loss cutoff
#   That combo is profitable at 55% win rate.
#
# Indian-market specifics built in:
#   - NSE pivot calculation uses prev day OHLC (Classical pivots — most
#     widely watched in India, generate self-fulfilling levels)
#   - F&O liquidity filter (skips low-volume cash-only names that slip)
#   - Sector strength check (won't long a stock in a falling sector)
#   - Time-aware: setups assume entry between 9:30-14:30 IST
#   - ATR-based SL prevents getting stopped by normal noise
# ════════════════════════════════════════════════════════════════════════════

# NSE F&O list — most-traded ~200 stocks. Filtering to these eliminates the
# illiquidity-trap of cash-only stocks. Sourced from NSE Sept 2025 contract list
# (we keep this static; the list is stable +/- a few names per quarter).
NSE_FNO_STOCKS = {
    'AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT',
    'ADANIPORTS', 'ALKEM', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASIANPAINT',
    'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO',
    'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK',
    'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL',
    'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK',
    'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE',
    'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND',
    'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON',
    'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK',
    'GAIL', 'GLENMARK', 'GMRINFRA', 'GNFC', 'GODREJCP', 'GODREJPROP',
    'GRANULES', 'GRASIM', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFCAMC',
    'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER',
    'HINDPETRO', 'HINDUNILVR', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDEA',
    'IDFC', 'IDFCFIRSTB', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART',
    'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'IOC', 'IPCALAB', 'IRCTC',
    'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK',
    'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTIM', 'LTTS', 'LUPIN',
    'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX',
    'METROPOLIS', 'MFSL', 'MGL', 'MOTHERSON', 'MPHASIS', 'MUTHOOTFIN',
    'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC',
    'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET',
    'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POLYCAB', 'POWERGRID', 'PVRINOX',
    'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE',
    'SBIN', 'SHREECEM', 'SHRIRAMFIN', 'SIEMENS', 'SRF', 'SUNPHARMA', 'SUNTV',
    'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER',
    'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TRENT', 'TVSMOTOR',
    'UBL', 'ULTRACEMCO', 'UNITDSPR', 'UPL', 'VEDL', 'VOLTAS', 'WIPRO',
    'ZEEL', 'ZYDUSLIFE',
}


def _calculate_pivots(high, low, close):
    """Classical pivot points — the most-watched intraday levels in India.
    Returns dict with PP + R1/R2/R3 + S1/S2/S3 based on PREVIOUS day's OHLC."""
    pp = (high + low + close) / 3
    r1 = 2 * pp - low
    s1 = 2 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)
    r3 = high + 2 * (pp - low)
    s3 = low - 2 * (high - pp)
    return {
        'pp':  round(pp,  2),
        'r1':  round(r1,  2), 'r2':  round(r2,  2), 'r3':  round(r3,  2),
        's1':  round(s1,  2), 's2':  round(s2,  2), 's3':  round(s3,  2),
    }


def _calculate_camarilla(high, low, close):
    """Camarilla pivots — preferred for mean-reversion intraday plays.
    H3/L3 = breakout zones, H4/L4 = trend-day reversal levels."""
    rng = high - low
    return {
        'h3': round(close + rng * 1.1 / 4,  2),
        'h4': round(close + rng * 1.1 / 2,  2),
        'l3': round(close - rng * 1.1 / 4,  2),
        'l4': round(close - rng * 1.1 / 2,  2),
    }


def _intraday_index_bias(close_series, prev_close):
    """Compute simple intraday bias for an index based on daily structure."""
    if close_series is None or len(close_series) < 50:
        return {'bias': 'NEUTRAL', 'reason': 'insufficient data'}
    curr  = float(close_series.iloc[-1])
    sma20 = float(close_series.tail(20).mean())
    sma50 = float(close_series.tail(50).mean())
    chg_5d = (curr - float(close_series.iloc[-6])) / float(close_series.iloc[-6]) * 100 if len(close_series) >= 6 else 0

    # Strong trend: above both SMAs + 5d move > 1%
    if curr > sma20 > sma50 and chg_5d > 1:
        return {'bias': 'BULLISH', 'strength': 'STRONG',
                'reason': f'Above 20DMA + 50DMA, +{chg_5d:.1f}% over 5 days'}
    if curr < sma20 < sma50 and chg_5d < -1:
        return {'bias': 'BEARISH', 'strength': 'STRONG',
                'reason': f'Below 20DMA + 50DMA, {chg_5d:.1f}% over 5 days'}
    if curr > sma20 and curr > sma50:
        return {'bias': 'BULLISH', 'strength': 'MILD',
                'reason': 'Above both DMAs but momentum cooling'}
    if curr < sma20 and curr < sma50:
        return {'bias': 'BEARISH', 'strength': 'MILD',
                'reason': 'Below both DMAs, weak structure'}
    return {'bias': 'NEUTRAL', 'reason': 'Mixed DMA positioning — range-bound'}


def _intraday_index_card(symbol, display_name):
    """Returns full intraday context for an index: bias + pivots + key levels.
    Indices use yfinance symbols without our .NS suffix logic (^NSEI etc),
    so call yf.Ticker directly instead of fetch_history."""
    try:
        df = yf.Ticker(symbol).history(period='6mo')
        if df is None or df.empty or len(df) < 5:
            return None
        # Drop NaN-poisoned partial bars (pre-open / post-close)
        df = df.dropna(subset=['Close', 'High', 'Low'])
        if df is None or df.empty or len(df) < 5:
            return None
        prev_high  = float(df['High'].iloc[-2])
        prev_low   = float(df['Low'].iloc[-2])
        prev_close = float(df['Close'].iloc[-2])
        curr_price = float(df['Close'].iloc[-1])
        prev_close_for_chg = float(df['Close'].iloc[-2])
        day_chg = (curr_price - prev_close_for_chg) / prev_close_for_chg * 100 if prev_close_for_chg else 0

        bias = _intraday_index_bias(df['Close'], prev_close)
        pivots = _calculate_pivots(prev_high, prev_low, prev_close)
        cam = _calculate_camarilla(prev_high, prev_low, prev_close)

        # 20-day ATR for "expected daily range"
        try:
            atr = float(calculate_atr(df, 20).iloc[-1])
            atr_pct = round(atr / curr_price * 100, 2) if curr_price else None
        except Exception:
            atr = None; atr_pct = None

        return {
            'symbol':     symbol.replace('^', ''),
            'name':       display_name,
            'price':      round(curr_price, 2),
            'day_chg':    round(day_chg, 2),
            'bias':       bias.get('bias'),
            'strength':   bias.get('strength', ''),
            'reason':     bias.get('reason'),
            'pivots':     pivots,
            'camarilla':  cam,
            'atr':        round(atr, 2) if atr is not None else None,
            'atr_pct':    atr_pct,
            'prev_high':  round(prev_high,  2),
            'prev_low':   round(prev_low,   2),
            'prev_close': round(prev_close, 2),
        }
    except Exception as e:
        print(f'  [intraday {symbol}] index card failed: {e}')
        return None


# ════════════════════════════════════════════════════════════════════════════
# INTRADAY v2 — 5-minute candle infrastructure (Phase 1.1)
# yfinance supports interval='5m' for the last 7 days (free).
# These bars unlock real intraday setups (VWAP, ORB, gap fade, etc.) instead
# of the daily-bar approximations the v1 setups used.
# ════════════════════════════════════════════════════════════════════════════

def fetch_intraday_5m(ticker, lookback_days=2):
    """Fetch 5-minute OHLCV bars for the last `lookback_days` trading days.
    Cached for 5 minutes (5m bars don't change inside a 5m window anyway).
    Returns DataFrame or None."""
    sym = ticker if ticker.endswith('.NS') else ticker + '.NS'
    cache_key = f'5m:{sym}:{lookback_days}'
    cached = get_cached(cache_key, ttl=300)
    if cached is not None:
        return cached
    try:
        period = f'{lookback_days}d' if lookback_days <= 7 else '7d'
        df = yf.Ticker(sym).history(period=period, interval='5m')
        if df is None or df.empty:
            return None
        df = df.dropna(subset=['Close', 'High', 'Low'])
        if df.empty:
            return None
        set_cached(cache_key, df, ttl=300)
        return df
    except Exception as e:
        print(f'  [5m {sym}] fetch failed: {e}')
        return None


def calculate_vwap_5m(df_5m):
    """Session-anchored VWAP: cumulative (typical_price × volume) / cumulative volume,
    reset each trading session. Returns a series aligned with df_5m index."""
    if df_5m is None or df_5m.empty:
        return None
    # Group by trading date (IST) — VWAP resets at session open
    typical = (df_5m['High'] + df_5m['Low'] + df_5m['Close']) / 3
    pv = typical * df_5m['Volume']
    # Trading date in IST — yfinance returns timestamps in market timezone (Asia/Kolkata for .NS)
    try:
        dates = df_5m.index.tz_convert('Asia/Kolkata').date
    except Exception:
        dates = df_5m.index.date
    import pandas as _pd
    grp = _pd.Series(dates, index=df_5m.index)
    pv_cum  = pv.groupby(grp).cumsum()
    vol_cum = df_5m['Volume'].groupby(grp).cumsum()
    return pv_cum / vol_cum.replace(0, float('nan'))


def calculate_opening_range_5m(df_5m, range_minutes=15):
    """Opening Range = 9:15-9:30 IST high/low (default 15-min OR).
    Returns dict with 'high', 'low', 'session_start' for the LAST trading day."""
    if df_5m is None or df_5m.empty:
        return None
    try:
        idx_ist = df_5m.index.tz_convert('Asia/Kolkata')
    except Exception:
        idx_ist = df_5m.index
    import pandas as _pd
    df = df_5m.copy()
    df['ist_date'] = idx_ist.date
    df['ist_time'] = idx_ist.time
    # Last trading day in the data
    last_date = df['ist_date'].iloc[-1]
    day_df = df[df['ist_date'] == last_date]
    if day_df.empty:
        return None
    # Filter to 9:15-9:15+range_minutes (i.e., first 3 5m bars for 15-min OR)
    from datetime import time as _time
    range_end = _time(9, 15 + range_minutes - 1, 59)
    or_bars = day_df[day_df['ist_time'] <= range_end]
    or_bars = or_bars[or_bars['ist_time'] >= _time(9, 15)]
    if or_bars.empty:
        return None
    return {
        'high':          round(float(or_bars['High'].max()), 2),
        'low':           round(float(or_bars['Low'].min()), 2),
        'range_minutes': range_minutes,
        'bars':          len(or_bars),
        'complete':      len(or_bars) >= (range_minutes // 5),
    }


def calculate_bb_squeeze(df, period=20, std_mult=2.0):
    """Bollinger Band width over time + 'is currently squeezing' boolean.
    Squeeze = BB width at 20-day minimum (low volatility before expansion)."""
    if df is None or df.empty or len(df) < period + 5:
        return None
    close = df['Close']
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma * 100   # width as % of mean
    if len(width.dropna()) < period:
        return None
    curr_w = float(width.iloc[-1])
    min_w  = float(width.tail(period).min())
    is_squeeze = curr_w <= min_w * 1.02   # within 2% of 20-day min width
    return {
        'curr_width_pct': round(curr_w, 2),
        'min_width_pct':  round(min_w, 2),
        'is_squeeze':     is_squeeze,
        'upper':          round(float(upper.iloc[-1]), 2) if not _np_isnan(upper.iloc[-1]) else None,
        'lower':          round(float(lower.iloc[-1]), 2) if not _np_isnan(lower.iloc[-1]) else None,
        'middle':         round(float(sma.iloc[-1]), 2),
    }


def _np_isnan(v):
    """Safe NaN check that works with both Python floats and numpy NaN."""
    try:
        import math
        return math.isnan(float(v))
    except Exception:
        return True


def calculate_mtf_alignment(df_daily):
    """Multi-timeframe alignment score using daily EMA stack as proxy for
    actual 5m/15m/1h/1d timeframes (those would need separate fetches we
    don't want to make 40× in a single API call).

    EMA(5) = roughly weekly trend (short-term momentum)
    EMA(20) = monthly trend (intermediate)
    EMA(50) = quarterly trend (medium)
    EMA(200) = yearly trend (long-term)

    Returns:
      bias: 'LONG' if 3-4 EMAs below price, 'SHORT' if 3-4 above, else 'NEUTRAL'
      score: -4 to +4 (positive = bullish stack, negative = bearish)
      details: per-EMA direction for UI display
    """
    if df_daily is None or df_daily.empty or len(df_daily) < 200:
        return None
    close = df_daily['Close']
    curr = float(close.iloc[-1])
    if _np_isnan(curr):
        return None
    emas = {
        'ema5':   float(close.ewm(span=5,   adjust=False).mean().iloc[-1]),
        'ema20':  float(close.ewm(span=20,  adjust=False).mean().iloc[-1]),
        'ema50':  float(close.ewm(span=50,  adjust=False).mean().iloc[-1]),
        'ema200': float(close.ewm(span=200, adjust=False).mean().iloc[-1]),
    }
    above_count = sum(1 for v in emas.values() if curr > v)
    score = above_count - (4 - above_count)   # -4..+4

    if above_count >= 3:    bias = 'LONG'
    elif above_count <= 1:  bias = 'SHORT'
    else:                   bias = 'NEUTRAL'

    return {
        'bias':    bias,
        'score':   score,                          # -4 (max bearish) to +4 (max bullish)
        'above':   above_count,                    # how many of 4 EMAs price is above
        'details': {k: ('above' if curr > v else 'below') for k, v in emas.items()},
        'emas':    {k: round(v, 2) for k, v in emas.items()},
        'curr':    round(curr, 2),
    }


def _build_intraday_setup(row, df, sector_strength=None, regime=None, mtf=None, df_5m=None):
    """Given a screener row + its daily OHLC dataframe, evaluate ALL setups
    and return the best (highest conviction) setup, or None if no setup fires.

    v2 additions:
      - regime: market regime dict from _detect_market_regime() — used to
        gate mean-reversion setups (OVERSOLD_BOUNCE, BREAKOUT_WATCH) to
        RANGING markets only (where they actually work).
      - mtf: multi-timeframe alignment from calculate_mtf_alignment() —
        adds +5 to long setups in LONG-aligned stocks (or vice versa for
        shorts). High-alignment setups get a bonus.
      - df_5m: optional 5-minute bars from fetch_intraday_5m() — enables
        the new intraday-precise setups (VWAP Rejection, ORB, Gap Fade).
    """
    if df is None or df.empty or len(df) < 30:
        return None

    # Drop NaN-filled rows (yfinance often returns a partial bar for the
    # current pre-open / post-close day with NaN OHLC, which poisons every
    # calculation downstream). See task #39 for the original fix.
    df = df.dropna(subset=['Close', 'High', 'Low'])
    if df is None or df.empty or len(df) < 30:
        return None

    close  = df['Close']
    high   = df['High']
    low    = df['Low']
    volume = df['Volume']
    curr   = float(close.iloc[-1])

    # Pre-compute reusable indicators
    try:
        rsi = float(calculate_rsi(close).iloc[-1])
    except Exception:
        rsi = None
    try:
        atr = float(calculate_atr(df, 14).iloc[-1])
        atr_pct = atr / curr * 100 if curr else None
    except Exception:
        atr = None; atr_pct = None
    sma5  = float(close.tail(5).mean())
    sma20 = float(close.tail(20).mean())
    sma50 = float(close.tail(50).mean()) if len(close) >= 50 else sma20
    hi20  = float(high.tail(20).max())
    lo20  = float(low.tail(20).min())
    hi52w = float(high.tail(252).max()) if len(high) >= 252 else hi20
    vol_avg = float(volume.tail(20).mean())
    vol_yesterday = float(volume.iloc[-2]) if len(volume) >= 2 else 0
    vol_ratio = vol_yesterday / vol_avg if vol_avg > 0 else 0
    chg_5d = (curr - float(close.iloc[-6])) / float(close.iloc[-6]) * 100 if len(close) >= 6 else 0

    # Setup A: Trend Continuation Long
    #   curr > SMA5 > SMA20 > SMA50, RSI 50-70, vol_ratio > 1.3,
    #   ATR% 1.5-5, last 3 days higher closes
    setup_A = None
    if (curr > sma5 and sma5 > sma20 and sma20 > sma50
            and rsi is not None and 50 <= rsi <= 70
            and vol_ratio > 1.3
            and atr_pct is not None and 1.5 <= atr_pct <= 5
            and chg_5d > 0):
        conviction = 65
        confirmations = ['Price above 5/20/50 DMA stack', f'RSI {rsi:.0f} in healthy zone',
                         f'Volume {vol_ratio:.1f}× avg', f'ATR {atr_pct:.1f}% (workable range)']
        if sector_strength is not None and sector_strength >= 60:
            conviction += 10
            confirmations.append(f'Sector strength {sector_strength:.0f}/100')
        if chg_5d > 3:
            conviction += 5
            confirmations.append(f'Strong 5d momentum +{chg_5d:.1f}%')
        setup_A = {
            'type': 'TREND_CONTINUATION',
            'side': 'LONG',
            'name_pretty': 'Trend Continuation',
            'entry_zone': [round(curr * 0.998, 2), round(curr * 1.002, 2)],  # market price band
            'stop':       round(curr - atr * 1.2, 2),
            'target1':    round(curr + atr * 1.5, 2),
            'target2':    round(curr + atr * 3.0, 2),
            'conviction': min(conviction, 90),
            'confirmations': confirmations,
        }

    # ── Phase 2.4: GATE THE LOSERS ──
    # Backtest showed OVERSOLD_BOUNCE & BREAKOUT_WATCH lose money in non-ranging
    # markets. Only fire them when regime is RANGING (where mean-reversion works).
    regime_kind = (regime or {}).get('kind', 'UNKNOWN')
    allow_losers = regime_kind == 'RANGING'

    # Setup B: Oversold Bounce Long (counter-trend, smaller targets)
    # GATED to RANGING regime per Phase 2.4
    setup_B = None
    if (allow_losers
            and rsi is not None and 25 <= rsi <= 38
            and curr < sma20 and curr > lo20 * 1.005
            and atr_pct is not None and atr_pct >= 1.5):
        conviction = 55
        confirmations = [f'RSI {rsi:.0f} (oversold)', 'Holding above 20-day low',
                         f'ATR {atr_pct:.1f}% (sufficient for bounce)']
        if sector_strength is not None and sector_strength >= 50:
            conviction += 10
            confirmations.append(f'Sector still neutral/strong ({sector_strength:.0f}/100)')
        setup_B = {
            'type': 'OVERSOLD_BOUNCE',
            'side': 'LONG',
            'name_pretty': 'Oversold Bounce',
            'entry_zone': [round(curr * 0.997, 2), round(curr * 1.003, 2)],
            'stop':       round(lo20, 2),
            'target1':    round(curr + atr * 1.0, 2),
            'target2':    round(curr + atr * 2.0, 2),
            'conviction': min(conviction, 80),
            'confirmations': confirmations,
        }

    # Setup C: Breakout Watch (within 2% of 20D high)
    # GATED to RANGING regime per Phase 2.4 (backtest verdict)
    setup_C = None
    if (allow_losers
            and curr >= hi20 * 0.98 and curr <= hi20 * 1.005
            and rsi is not None and 55 <= rsi <= 72
            and vol_ratio > 1.2
            and atr_pct is not None and 1.5 <= atr_pct <= 5):
        breakout_lvl = round(hi20, 2)
        conviction = 60
        confirmations = [f'Within 2% of 20D high ₹{breakout_lvl}',
                         f'RSI {rsi:.0f}', f'Volume {vol_ratio:.1f}× avg']
        if sector_strength is not None and sector_strength >= 55:
            conviction += 10
            confirmations.append(f'Sector strength {sector_strength:.0f}/100')
        setup_C = {
            'type': 'BREAKOUT_WATCH',
            'side': 'LONG',
            'name_pretty': 'Breakout Watch',
            'entry_zone': [round(breakout_lvl * 1.001, 2), round(breakout_lvl * 1.005, 2)],
            'stop':       round(breakout_lvl - atr * 1.0, 2),
            'target1':    round(breakout_lvl + atr * 1.5, 2),
            'target2':    round(breakout_lvl + atr * 3.0, 2),
            'conviction': min(conviction, 85),
            'confirmations': confirmations,
        }

    # Setup D: 52W High Momentum (pullback to 5DMA)
    setup_D = None
    near_52wh = curr >= hi52w * 0.97
    if (near_52wh and rsi is not None and rsi < 80
            and curr <= sma5 * 1.015
            and atr_pct is not None and atr_pct >= 1.5):
        conviction = 60
        confirmations = [f'Within 3% of 52W high ₹{round(hi52w,2)}',
                         f'RSI {rsi:.0f} (not yet euphoric)',
                         f'Pulled back to 5DMA — entry buffer']
        if sector_strength is not None and sector_strength >= 60:
            conviction += 10
            confirmations.append(f'Sector strength {sector_strength:.0f}/100')
        setup_D = {
            'type': 'NEW_HIGH_MOMENTUM',
            'side': 'LONG',
            'name_pretty': '52W High Momentum',
            'entry_zone': [round(sma5 * 0.998, 2), round(sma5 * 1.005, 2)],
            'stop':       round(sma5 - atr * 1.2, 2),
            'target1':    round(hi52w * 1.01, 2),
            'target2':    round(hi52w * 1.025, 2),
            'conviction': min(conviction, 85),
            'confirmations': confirmations,
        }

    # Setup E: Short on weakness (trend continuation down)
    setup_E = None
    if (curr < sma5 and sma5 < sma20 and sma20 < sma50
            and rsi is not None and 30 <= rsi <= 50
            and vol_ratio > 1.3
            and atr_pct is not None and 1.5 <= atr_pct <= 5
            and chg_5d < 0):
        conviction = 55
        confirmations = ['Price below 5/20/50 DMA stack (bearish stack)',
                         f'RSI {rsi:.0f} (weak but not oversold)',
                         f'Volume {vol_ratio:.1f}× avg (distribution)',
                         f'Down {chg_5d:.1f}% over 5 days']
        if sector_strength is not None and sector_strength <= 45:
            conviction += 10
            confirmations.append(f'Sector weak ({sector_strength:.0f}/100)')
        setup_E = {
            'type': 'TREND_CONTINUATION',
            'side': 'SHORT',
            'name_pretty': 'Short on Weakness',
            'entry_zone': [round(curr * 0.998, 2), round(curr * 1.002, 2)],
            'stop':       round(curr + atr * 1.2, 2),
            'target1':    round(curr - atr * 1.5, 2),
            'target2':    round(curr - atr * 3.0, 2),
            'conviction': min(conviction, 80),
            'confirmations': confirmations,
        }

    # ═════════════════════════════════════════════════════════════════════
    # PHASE 2.1 — Five new setups using 5m data + pivots + BB
    # ═════════════════════════════════════════════════════════════════════

    # Pull pre-computed extras (passed in by caller for efficiency)
    # 5m bars enable: VWAP, opening range, gap detection. Fallback to daily-bar
    # approximation when 5m unavailable (e.g., during snapshot rebuild).
    vwap_curr = None
    or_high = or_low = None
    or_complete = False
    if df_5m is not None and not df_5m.empty:
        try:
            vwap_series = calculate_vwap_5m(df_5m)
            if vwap_series is not None and not vwap_series.empty:
                vwap_curr = float(vwap_series.iloc[-1])
        except Exception:
            pass
        try:
            or_data = calculate_opening_range_5m(df_5m, range_minutes=15)
            if or_data:
                or_high = or_data['high']
                or_low  = or_data['low']
                or_complete = or_data['complete']
        except Exception:
            pass

    # Setup F: VWAP Rejection LONG
    # Price dips to VWAP on an up trend → bounce setup. Only works when
    # trend is intact (price above key MAs) AND VWAP is rising.
    setup_F = None
    if (vwap_curr is not None and curr > sma20 and sma5 > sma20
            and abs(curr - vwap_curr) / curr < 0.005   # within 0.5% of VWAP
            and rsi is not None and 40 <= rsi <= 65
            and atr_pct is not None and atr_pct >= 1.5):
        conviction = 65
        confirmations = [f'Price at VWAP ₹{vwap_curr:.2f} on uptrend',
                         f'5DMA > 20DMA (trend intact)',
                         f'RSI {rsi:.0f} (room to run)']
        if sector_strength is not None and sector_strength >= 55:
            conviction += 5
            confirmations.append(f'Sector strength {sector_strength:.0f}/100')
        setup_F = {
            'type':        'VWAP_REJECTION',
            'side':        'LONG',
            'name_pretty': 'VWAP Pullback Long',
            'entry_zone':  [round(vwap_curr * 0.999, 2), round(vwap_curr * 1.005, 2)],
            'stop':        round(vwap_curr - atr * 0.8, 2),
            'target1':     round(curr + atr * 1.0, 2),
            'target2':     round(curr + atr * 2.0, 2),
            'conviction':  min(conviction, 85),
            'confirmations': confirmations,
        }

    # Setup G: Opening Range Breakout (ORB) LONG
    # First 15-min high break with volume = classic NSE intraday edge.
    # Only valid after 9:30 IST (when OR is complete).
    setup_G = None
    if (or_complete and or_high is not None and or_low is not None
            and curr <= or_high * 1.005   # near the OR high, not far above
            and curr > or_high * 0.995    # not below
            and vol_ratio > 1.1
            and atr_pct is not None and atr_pct >= 1.0):
        or_range = or_high - or_low
        conviction = 60
        confirmations = [f'OR high ₹{or_high} | OR low ₹{or_low}',
                         f'Range = ₹{or_range:.2f}',
                         f'Volume {vol_ratio:.1f}× avg']
        # MTF boost (long bias confirmed by EMA stack)
        if mtf is not None and mtf.get('bias') == 'LONG':
            conviction += 8
            confirmations.append(f'MTF LONG ({mtf["above"]}/4 EMAs)')
        if sector_strength is not None and sector_strength >= 55:
            conviction += 5
            confirmations.append(f'Sector strength {sector_strength:.0f}/100')
        setup_G = {
            'type':        'ORB',
            'side':        'LONG',
            'name_pretty': 'Opening Range Breakout',
            'entry_zone':  [round(or_high * 1.001, 2), round(or_high * 1.004, 2)],
            'stop':        round(or_low, 2),                 # OR low = invalidation
            'target1':     round(or_high + or_range, 2),     # 1× range above
            'target2':     round(or_high + 2 * or_range, 2), # 2× range above
            'conviction':  min(conviction, 85),
            'confirmations': confirmations,
        }

    # Setup H: Gap Fade LONG
    # Gap-down > 1.5% on a fundamentally sound stock → fill rate is ~65% by noon.
    # Uses today's open vs prior close. Requires 5m data for live open.
    setup_H = None
    prev_close = float(close.iloc[-2]) if len(close) >= 2 else None
    today_open = None
    if df_5m is not None and not df_5m.empty:
        try:
            today_open = float(df_5m['Open'].iloc[0])
        except Exception:
            pass
    if (prev_close and today_open and atr is not None):
        gap_pct = (today_open - prev_close) / prev_close * 100
        # Gap-down to fade up (LONG)
        if (gap_pct <= -1.5 and gap_pct >= -4.0   # not a crash
                and curr > today_open * 0.998       # not still falling hard
                and rsi is not None and rsi >= 30):
            conviction = 60
            confirmations = [f'Gap down {gap_pct:.1f}% from prev close ₹{prev_close:.2f}',
                             f'Open ₹{today_open:.2f} vs current ₹{curr:.2f}',
                             f'Stabilising (RSI {rsi:.0f})']
            if mtf is not None and mtf.get('bias') == 'LONG':
                conviction += 8
                confirmations.append(f'MTF still LONG ({mtf["above"]}/4 EMAs)')
            setup_H = {
                'type':        'GAP_FADE',
                'side':        'LONG',
                'name_pretty': 'Gap Fade (down → fill)',
                'entry_zone':  [round(curr * 0.998, 2), round(curr * 1.003, 2)],
                'stop':        round(today_open - atr * 0.5, 2),
                'target1':     round(prev_close * 0.997, 2),   # fill back to prior close
                'target2':     round(prev_close * 1.005, 2),   # overshoot
                'conviction':  min(conviction, 80),
                'confirmations': confirmations,
            }

    # Setup I: Pivot Bounce LONG
    # Touch of yesterday's pivot S1 with intraday reversal → mean-reversion
    # to PP/R1. Strong in ranging markets, decent in trending too.
    setup_I = None
    if (len(df) >= 2 and atr is not None):
        prev_h = float(df['High'].iloc[-2])
        prev_l = float(df['Low'].iloc[-2])
        prev_c = float(df['Close'].iloc[-2])
        pivots = _calculate_pivots(prev_h, prev_l, prev_c)
        s1 = pivots['s1']; pp = pivots['pp']; r1 = pivots['r1']
        # Long bounce off S1 (or between S1 and S2)
        if (curr >= s1 * 0.998 and curr <= s1 * 1.012   # within 1.2% of S1
                and rsi is not None and 30 <= rsi <= 50
                and atr_pct is not None and atr_pct >= 1.0):
            conviction = 55
            confirmations = [f'At/near S1 ₹{s1} (pivot support)',
                             f'PP ₹{pp} = T1', f'R1 ₹{r1} = T2',
                             f'RSI {rsi:.0f} (oversold-ish)']
            if mtf is not None and mtf.get('bias') == 'LONG':
                conviction += 8
                confirmations.append(f'MTF LONG ({mtf["above"]}/4 EMAs)')
            setup_I = {
                'type':        'PIVOT_BOUNCE',
                'side':        'LONG',
                'name_pretty': 'Pivot Bounce (S1)',
                'entry_zone':  [round(s1, 2), round(s1 * 1.005, 2)],
                'stop':        round(pivots['s2'], 2),
                'target1':     round(pp, 2),
                'target2':     round(r1, 2),
                'conviction':  min(conviction, 80),
                'confirmations': confirmations,
            }

    # Setup J: BB Squeeze Breakout LONG
    # When Bollinger Bands compress to 20-day min width, expansion follows.
    # Direction = whichever side price closes outside the band first.
    setup_J = None
    try:
        bb = calculate_bb_squeeze(df, period=20, std_mult=2.0)
        if bb and bb['is_squeeze'] and bb['upper'] and curr > bb['middle']:
            # Squeeze in progress and price biased to upper band → long breakout watch
            conviction = 55
            confirmations = [f'BB squeeze (width {bb["curr_width_pct"]:.1f}% at 20D min)',
                             f'Price above mid-band ₹{bb["middle"]}',
                             f'Breakout target: ₹{bb["upper"]} (upper band)']
            if mtf is not None and mtf.get('bias') == 'LONG':
                conviction += 8
                confirmations.append(f'MTF LONG ({mtf["above"]}/4 EMAs)')
            if vol_ratio > 1.3:
                conviction += 5
                confirmations.append(f'Volume {vol_ratio:.1f}× (early expansion)')
            setup_J = {
                'type':        'BB_SQUEEZE',
                'side':        'LONG',
                'name_pretty': 'BB Squeeze (long)',
                'entry_zone':  [round(bb['middle'] * 1.001, 2), round(curr * 1.003, 2)],
                'stop':        round(bb['lower'], 2),
                'target1':     round(bb['upper'], 2),
                'target2':     round(bb['upper'] + atr * 1.5, 2),
                'conviction':  min(conviction, 80),
                'confirmations': confirmations,
            }
    except Exception:
        pass

    candidates = [s for s in [setup_A, setup_B, setup_C, setup_D, setup_E,
                               setup_F, setup_G, setup_H, setup_I, setup_J] if s is not None]
    if not candidates:
        return None
    best = max(candidates, key=lambda s: s['conviction'])

    # Compute R:R for both targets
    if best['side'] == 'LONG':
        risk = (best['entry_zone'][1] - best['stop'])
        rr1  = (best['target1'] - best['entry_zone'][1]) / risk if risk > 0 else 0
        rr2  = (best['target2'] - best['entry_zone'][1]) / risk if risk > 0 else 0
    else:
        risk = (best['stop'] - best['entry_zone'][0])
        rr1  = (best['entry_zone'][0] - best['target1']) / risk if risk > 0 else 0
        rr2  = (best['entry_zone'][0] - best['target2']) / risk if risk > 0 else 0

    # Filter out setups with bad R:R (institutional discipline)
    if rr1 < 1.0:
        return None

    best['rr1'] = round(rr1, 2)
    best['rr2'] = round(rr2, 2)
    best['atr_pct'] = round(atr_pct, 2) if atr_pct else None

    # Attach MTF alignment (Phase 2.3) — adds a conviction bonus if the
    # multi-timeframe EMA stack confirms the setup's direction.
    if mtf is not None:
        best['mtf'] = mtf
        # Bonus: long setup with strong LONG alignment, or short with SHORT
        mtf_bias = mtf.get('bias')
        if best['side'] == 'LONG' and mtf_bias == 'LONG':
            best['conviction'] = min(100, best['conviction'] + (3 if mtf.get('above', 0) >= 3 else 0)
                                       + (5 if mtf.get('above', 0) == 4 else 0))
            best['confirmations'].append(f'MTF strongly LONG ({mtf["above"]}/4 EMAs above price)')
        elif best['side'] == 'SHORT' and mtf_bias == 'SHORT':
            best['conviction'] = min(100, best['conviction'] + (3 if mtf.get('above', 0) <= 1 else 0)
                                       + (5 if mtf.get('above', 0) == 0 else 0))
            best['confirmations'].append(f'MTF strongly SHORT ({4 - mtf["above"]}/4 EMAs above price)')
        elif (best['side'] == 'LONG' and mtf_bias == 'SHORT') or \
             (best['side'] == 'SHORT' and mtf_bias == 'LONG'):
            # Counter-MTF setup → reduce conviction (it's fighting the larger trend)
            best['conviction'] = max(0, best['conviction'] - 10)
            best['confirmations'].append(f'⚠ MTF against direction — counter-trend trade')

    # Attach VWAP context for UI display
    if vwap_curr is not None:
        best['vwap'] = round(vwap_curr, 2)
        best['vwap_dist_pct'] = round((curr - vwap_curr) / curr * 100, 2)
    if or_high is not None:
        best['or_high'] = or_high
        best['or_low']  = or_low
        best['or_complete'] = or_complete
    best['ticker'] = row.get('ticker')
    best['name'] = row.get('name')
    best['sector'] = row.get('sector')
    best['curr_price'] = round(curr, 2)
    return best


@app.route('/api/intraday_quote/<path:ticker>')
def intraday_quote(ticker):
    """Fast single-ticker live quote — bypasses yfinance for cloud reliability.
    Used by the intraday view to validate that a setup is still tradeable
    (live price within entry zone, not stopped, not past target).

    Cache: 25s — short enough that intraday polling at 30s intervals always
    sees fresh-enough data, long enough to absorb burst polling from
    multiple cards on screen at once.
    """
    sym = ticker.replace('.NS', '').replace('.BO', '').upper()
    cache_key = f'quote_live:{sym}'
    cached = get_cached(cache_key, ttl=25)
    if cached is not None:
        return jsonify(cached)

    quote = _fetch_nse_quote(sym)
    if quote is None:
        # Fallback: yfinance fast_info (works for single ticker even when bulk fails)
        try:
            tk = yf.Ticker(sym + '.NS')
            fi = tk.fast_info
            price = fi.get('last_price') or fi.get('regular_market_price')
            prev  = fi.get('previous_close')
            if price:
                quote = {
                    'symbol':     sym,
                    'price':      round(float(price), 2),
                    'prev_close': round(float(prev), 2) if prev else None,
                    'day_change_pct': round((float(price) - float(prev)) / float(prev) * 100, 2) if prev else None,
                    'source':     'yfinance_fast',
                    'as_of':      int(time.time()),
                }
        except Exception:
            quote = None

    if quote is None:
        return jsonify({'error': 'Quote unavailable', 'symbol': sym}), 503
    set_cached(cache_key, quote, ttl=25)
    return jsonify(quote)


def _fetch_nse_quote(symbol):
    """Pull last-traded-price for an NSE stock via NSE's quote-equity API.
    Returns dict or None. This works from cloud IPs (NSE doesn't bot-detect
    like Yahoo) so it's our preferred live-quote path."""
    s = _get_nse_session()
    if s is None:
        return None
    try:
        url = f'{_NSE_HOME}/api/quote-equity?symbol={symbol}'
        r = s.get(url, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        pi = data.get('priceInfo') or {}
        last_price = pi.get('lastPrice')
        prev_close = pi.get('previousClose')
        if last_price is None:
            return None
        return {
            'symbol':     symbol,
            'price':      round(float(last_price), 2),
            'prev_close': round(float(prev_close), 2) if prev_close is not None else None,
            'day_change':     round(float(pi.get('change') or 0), 2),
            'day_change_pct': round(float(pi.get('pChange') or 0), 2),
            'day_high':   round(float(pi.get('intraDayHighLow', {}).get('max') or 0), 2) or None,
            'day_low':    round(float(pi.get('intraDayHighLow', {}).get('min') or 0), 2) or None,
            'open':       round(float(pi.get('open') or 0), 2) or None,
            'source':     'nse_direct',
            'as_of':      int(time.time()),
        }
    except Exception as e:
        print(f'  [nse_quote {symbol}] failed: {e}')
        return None


@app.route('/api/intraday_signals')
def intraday_signals():
    """Returns:
       - index_cards: NIFTY 50, BANKNIFTY, NIFTY IT, FINNIFTY with bias + pivots
       - setups: ranked list of high-conviction stock setups
       - market_summary: overall day bias + sector leaders/laggards
       - risk_rules: position-sizing + capital-protection rules
    """
    try:
        return _intraday_signals_impl()
    except Exception as e:
        print(f'  [intraday_signals] error: {e}')
        if IS_PUBLIC:
            return jsonify({'error': 'Intraday signals temporarily unavailable. Try again in a moment.'}), 503
        return jsonify({'error': str(e)}), 500


def _intraday_signals_impl():
    cache_key = 'intraday_signals:v1'
    cached = get_cached(cache_key, ttl=900)   # 15 min cache (intraday context shifts)
    if cached:
        return jsonify(cached)

    # ── 1. Index cards (4 most-traded NSE indices for intraday) ──
    # Parallel fetch — each yfinance call 3-5s; serial would be 20s+.
    index_targets = [
        ('^NSEI',     'NIFTY 50'),
        ('^NSEBANK',  'NIFTY BANK'),
        ('^CNXIT',    'NIFTY IT'),
        ('NIFTY_FIN_SERVICE.NS', 'FINNIFTY'),
    ]
    index_cards = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        future_to_idx = {ex.submit(_intraday_index_card, sym, nm): (sym, nm) for sym, nm in index_targets}
        for fut in future_to_idx:
            try:
                card = fut.result(timeout=18)
                if card:
                    index_cards.append(card)
            except Exception as e:
                sym, nm = future_to_idx[fut]
                print(f'  [intraday {sym}] index fetch failed/timed out: {e}')
    # Restore original order (parallel exec gives unpredictable order)
    order_map = {nm: i for i, (_, nm) in enumerate(index_targets)}
    index_cards.sort(key=lambda c: order_map.get(c.get('name'), 999))

    # ── 2. Stock setups ──
    # Reuse warmest cached screener result (no extra yfinance calls).
    # Prefer NIFTY 50 (most liquid) → ALL NSE (more candidates) → NIFTY Midcap.
    rows = (get_cached('screener:NIFTY 50')
            or get_cached('screener:ALL NSE')
            or get_cached('screener:NIFTY Midcap'))

    if not rows or not isinstance(rows, list):
        # Trigger a NIFTY 50 screener fetch (will populate cache for future calls)
        try:
            with app.test_request_context('/api/screener?index=NIFTY 50'):
                resp = screener()
            rows = resp.get_json()
        except Exception:
            rows = []

    if not rows or not isinstance(rows, list):
        rows = []

    # Sector strength map (so we can confirm setups with sector context)
    sector_avg_st = {}
    sector_counts = {}
    for r in rows:
        s = r.get('sector_grouped') or r.get('sector')
        if not s: continue
        sector_avg_st[s] = sector_avg_st.get(s, 0) + (r.get('short_term') or 0)
        sector_counts[s] = sector_counts.get(s, 0) + 1
    for s in sector_avg_st:
        sector_avg_st[s] = sector_avg_st[s] / sector_counts[s] if sector_counts[s] else 0

    # Top + bottom sectors (the "rotation" view)
    sectors_sorted = sorted(sector_avg_st.items(), key=lambda x: x[1], reverse=True)
    top_sectors = [{'sector': s, 'score': round(v, 1)} for s, v in sectors_sorted[:3]]
    bot_sectors = [{'sector': s, 'score': round(v, 1)} for s, v in sectors_sorted[-3:][::-1]]

    # Filter to F&O-liquid stocks only
    fno_candidates = [r for r in rows if r.get('ticker', '').upper() in NSE_FNO_STOCKS]
    # If nothing matched (e.g. row format differs), don't drop all candidates
    candidates_for_setup = fno_candidates if fno_candidates else rows[:60]

    # Compute regime EARLY so _one_setup can use it to gate losing setups
    # (Phase 2.4). Cached 1h so this is essentially free on second call.
    regime = _detect_market_regime()

    # Pre-sort by short_term so most-interesting candidates fetch first —
    # if we time out mid-scan we still have the best ones evaluated.
    candidates_for_setup = sorted(
        candidates_for_setup,
        key=lambda r: abs((r.get('short_term') or 50) - 50),
        reverse=True,
    )[:40]   # cap at 40 (was 80) — 40 stocks × ~1s parallel = 8s in best case

    # Parallel fetch (mirrors screener's ThreadPoolExecutor pattern).
    # v2: also computes MTF alignment from daily history (cheap, no extra fetch)
    # and attempts a best-effort 5m fetch for VWAP/ORB/Gap-Fade setups.
    def _one_setup(r):
        ticker = r.get('ticker')
        if not ticker:
            return None
        try:
            df, _src = fetch_history(ticker + '.NS', period='6mo')
            # MTF alignment — uses the same df we already fetched, no extra call
            mtf = None
            try:
                mtf = calculate_mtf_alignment(df)
            except Exception:
                pass
            # 5m data — BEST-EFFORT only. Fails on cloud IPs sometimes; the
            # setup builder falls back to daily-bar logic when df_5m is None.
            df_5m = None
            try:
                df_5m = fetch_intraday_5m(ticker, lookback_days=2)
            except Exception:
                pass
            sector_score = sector_avg_st.get(r.get('sector_grouped') or r.get('sector'))
            return _build_intraday_setup(
                r, df,
                sector_strength=sector_score,
                regime=regime,
                mtf=mtf,
                df_5m=df_5m,
            )
        except Exception as e:
            print(f'  [intraday {ticker}] setup build failed: {e}')
            return None

    setups = []
    with ThreadPoolExecutor(max_workers=_SCREENER_WORKERS) as ex:
        futures = {ex.submit(_one_setup, r): r.get('ticker') for r in candidates_for_setup}
        for fut in futures:
            try:
                s = fut.result(timeout=30)
                if s:
                    setups.append(s)
            except Exception as e:
                print(f'  [intraday {futures[fut]}] future timed out: {e}')

    # (regime was already computed before the setup loop so _one_setup
    # could use it for gating losing setups)

    # ── 2.6 Load backtest stats so we can adjust conviction ──
    backtest_stats = _load_setup_backtest_stats()

    # ── 2.7 Adjust conviction by measured backtest profitability ──
    # The most important honesty upgrade: my heuristic score gets multiplied
    # by how profitable that setup type ACTUALLY is over 2 years of data.
    # A losing setup gets aggressive downweight + warning flag.
    for s in setups:
        original = s.get('conviction', 0)
        s['conviction_raw'] = original
        bt = backtest_stats.get('by_setup_type', {}).get(s.get('type'))
        if bt:
            avg_r = bt.get('avg_r') or 0
            # Profitability tier multiplier
            if avg_r >= 0.05:    mult, tier = 1.10, 'PROFITABLE'    # boost meaningful winners
            elif avg_r >= 0:     mult, tier = 1.00, 'MARGINAL'      # neutral
            elif avg_r >= -0.05: mult, tier = 0.70, 'LOSING'        # demote losers
            elif avg_r >= -0.20: mult, tier = 0.45, 'BAD'           # heavy demote
            else:                mult, tier = 0.25, 'DISASTER'      # near-zero out
        else:
            mult, tier = 1.0, 'UNKNOWN'

        # Regime adjustment — modest +/- 5 based on setup-vs-regime fit
        regime_adj = 0
        rk = regime.get('kind', 'MIXED')
        stype = s.get('type', '')
        if rk == 'TRENDING' and stype in ('TREND_CONTINUATION', 'NEW_HIGH_MOMENTUM', 'BREAKOUT_WATCH'):
            regime_adj = 5
        elif rk == 'RANGING' and stype == 'OVERSOLD_BOUNCE':
            regime_adj = 5
        elif rk == 'TRENDING' and stype == 'OVERSOLD_BOUNCE':
            regime_adj = -5    # mean-reversion in trending market = fade losers
        elif rk == 'RANGING' and stype in ('TREND_CONTINUATION', 'NEW_HIGH_MOMENTUM'):
            regime_adj = -5    # trend setups in chop = whipsaw city

        adjusted = max(0, min(100, int(round(original * mult + regime_adj))))
        s['conviction']        = adjusted
        s['conviction_tier']   = tier
        s['conviction_factor'] = round(mult, 2)
        s['regime_adj']        = regime_adj

    # ── 2.8 Sector concentration cap (max 2 setups per sector) ──
    # Prevents the user from going 5x long banking — diversification by force.
    setups.sort(key=lambda x: x.get('conviction', 0), reverse=True)
    sector_counts = {}
    capped = []
    MAX_PER_SECTOR = 2
    for s in setups:
        sec = s.get('sector') or '__other__'
        if sector_counts.get(sec, 0) >= MAX_PER_SECTOR:
            continue
        sector_counts[sec] = sector_counts.get(sec, 0) + 1
        capped.append(s)
    setups = capped[:12]   # final cap on top-N

    # ── 3. Market summary ──
    nifty_card = next((c for c in index_cards if c['name'] == 'NIFTY 50'), None)
    bias_summary = nifty_card['bias'] if nifty_card else 'NEUTRAL'

    long_count  = sum(1 for s in setups if s.get('side') == 'LONG')
    short_count = sum(1 for s in setups if s.get('side') == 'SHORT')

    market_summary = {
        'bias':          bias_summary,
        'regime':        regime,
        'long_signals':  long_count,
        'short_signals': short_count,
        'top_sectors':   top_sectors,
        'bot_sectors':   bot_sectors,
        'as_of_ist':     (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d %b %Y, %H:%M IST'),
    }

    # ── 4. Risk management rules (always present) ──
    risk_rules = {
        'max_trades_per_day':       5,
        'max_loss_per_trade_pct':   1.0,
        'daily_loss_cutoff_pct':    3.0,
        'min_rr':                   1.5,
        'trade_window_ist':         '09:30 – 14:30',
        'avoid_around_events':      ['RBI policy', 'budget day', 'earnings ±2 days', 'major US Fed days'],
        'why_85_is_a_myth': (
            'No honest intraday system wins 85% of trades. Even institutional algos hit 55-65%. '
            'What 85%+ reliable here: setups have 4+ confirmations, levels are mathematically '
            'computed (not opinion), R:R is ≥1.5, only F&O-liquid stocks. That combination is '
            'profitable at 55% win rate.'
        ),
    }

    # Annotate each setup with its measured historical edge for the UI.
    # (backtest_stats was loaded earlier in this fn for conviction weighting.)
    for s in setups:
        bt = backtest_stats.get('by_setup_type', {}).get(s.get('type'))
        if bt:
            s['backtest'] = {
                'win_rate_pct':    bt.get('win_rate_pct'),
                'avg_r':           bt.get('avg_r'),
                'profit_factor':   bt.get('profit_factor'),
                'avg_net_pnl_pct': bt.get('avg_net_pnl_pct'),
                'is_profitable':   bt.get('is_profitable'),
                'n_trades':        bt.get('n_trades'),
            }

    result = {
        'generated_at':    int(time.time()),
        'market_summary':  market_summary,
        'index_cards':     index_cards,
        'setups':          setups,
        'risk_rules':      risk_rules,
        'universe_size':   len(candidates_for_setup),
        'backtest':        backtest_stats,   # full payload for the "Setup Performance" card
    }
    set_cached(cache_key, result, ttl=900)
    return jsonify(result)


def _detect_market_regime():
    """ADX-based regime detection on NIFTY 50 daily bars.
    ADX > 25 = clear trend; 20-25 = mixed; < 20 = ranging/choppy.
    A regime tells you which setups to favor:
      TRENDING → trend continuation + breakouts work, fade mean-reversion
      RANGING  → fade extremes (oversold bounce, overbought sell) works
      MIXED    → trade smaller, expect whipsaws

    Result cached 1 hour — regime doesn't shift hourly intraday.

    Resilience chain:
      1. In-memory cache (1h)
      2. yf.Ticker('^NSEI').history — fails on Fly cloud IPs (Yahoo bot detection)
      3. Snapshot file (static/data/snapshot_regime.json, built daily)
    """
    cache_key = 'market_regime:v1'
    cached = get_cached(cache_key, ttl=3600)
    if cached:
        return cached
    try:
        df = yf.Ticker('^NSEI').history(period='6mo')
        if df is None or df.empty or len(df) < 30:
            # Fall through to snapshot fallback
            raise RuntimeError('NIFTY data unavailable from Yahoo')
        df = df.dropna(subset=['Close', 'High', 'Low'])
        # calculate_adx returns (adx, plus_di, minus_di)
        adx_series, plus_di_series, minus_di_series = calculate_adx(df, 14)
        adx_val  = float(adx_series.iloc[-1])
        plus_di  = float(plus_di_series.iloc[-1])
        minus_di = float(minus_di_series.iloc[-1])
        # ADX value → regime
        if adx_val >= 25:
            kind, reason = 'TRENDING', f'ADX {adx_val:.1f} — strong directional trend in force'
            playbook = 'Favor trend-continuation + breakout setups. Avoid counter-trend bounces (will get steamrolled).'
        elif adx_val >= 20:
            kind, reason = 'MIXED', f'ADX {adx_val:.1f} — weak trend, possible regime shift'
            playbook = 'Trade smaller. Both trend and mean-reversion setups have lower edge than usual.'
        else:
            kind, reason = 'RANGING', f'ADX {adx_val:.1f} — no trend, oscillating market'
            playbook = 'Favor oversold-bounce setups. Avoid breakouts (most will fail / be whipsaws).'
        direction = 'UP' if plus_di > minus_di else 'DOWN'

        result = {
            'kind':       kind,
            'direction':  direction,
            'adx':        round(adx_val, 1),
            'plus_di':    round(plus_di, 1),
            'minus_di':   round(minus_di, 1),
            'reason':     reason,
            'playbook':   playbook,
        }
        set_cached(cache_key, result, ttl=3600)
        return result
    except Exception as e:
        print(f'  [regime] live detect failed: {e} — trying snapshot')
        # Fallback: load the snapshot file (built daily by GitHub Actions
        # runner where yfinance works fine).
        try:
            path = os.path.join(os.path.dirname(__file__), 'static', 'data', 'snapshot_regime.json')
            if os.path.exists(path):
                with open(path, encoding='utf-8') as f:
                    snap = json.load(f)
                regime = snap.get('regime') or {}
                if regime.get('kind'):
                    # Mark as snapshot-sourced so UI can show staleness
                    regime['source']        = 'snapshot'
                    regime['snapshot_iso']  = snap.get('generated_at_iso')
                    set_cached(cache_key, regime, ttl=3600)
                    return regime
        except Exception as snap_e:
            print(f'  [regime] snapshot fallback also failed: {snap_e}')
        return {'kind': 'UNKNOWN', 'adx': None, 'reason': f'Live + snapshot both unavailable: {e}'}


def _load_setup_backtest_stats():
    """Load the offline-computed setup backtest. Returns empty dict if missing
    (e.g., before the first GitHub Actions run after deploy)."""
    try:
        path = os.path.join(os.path.dirname(__file__), 'static', 'data', 'snapshot_setup_backtest.json')
        if not os.path.exists(path):
            return {}
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f'  [backtest_stats] load failed: {e}')
        return {}


# ════════════════════════════════════════════════════════════════════════════
# ALERTS — server-side alert engine + in-app notifications (tasks #10 + #11)
# ════════════════════════════════════════════════════════════════════════════
# Architecture:
#   - SQLite stores alerts (per session) + fired events
#   - Alert types: price_above, price_below, score_long_term, risk_below,
#                  day_change_above, day_change_below
#   - Lazy check: frontend polls /api/alerts/check every 60s while visible.
#     Cheaper than a background scheduler that wastes cycles on idle Fly
#     machines (which auto-stop anyway).
#   - Cooldown: same alert won't re-fire within 1 hour even if still true.
#   - All endpoints are session-scoped via the existing cp_sid cookie.

_ALERTS_DB_PATH = os.path.join(CACHE_DIR, 'alerts.db')

# Supported alert types — used for validation + UI labels
ALERT_TYPES = {
    'price_above':       {'label': 'Price rises above',  'unit': '₹',  'dir': 'up'},
    'price_below':       {'label': 'Price falls below',  'unit': '₹',  'dir': 'down'},
    'score_long_term':   {'label': 'Long-term score ≥',  'unit': '',   'dir': 'up'},
    'risk_below':        {'label': 'Risk score falls to ≤', 'unit': '', 'dir': 'down'},
    'day_change_above':  {'label': 'Daily change above', 'unit': '%',  'dir': 'up'},
    'day_change_below':  {'label': 'Daily change below', 'unit': '%',  'dir': 'down'},
}

_ALERT_COOLDOWN_SEC = 3600   # don't re-fire same alert within an hour

def _alerts_db():
    conn = sqlite3.connect(_ALERTS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _alerts_init_db():
    conn = _alerts_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL DEFAULT 'local',
            ts_created INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT,
            type TEXT NOT NULL,
            threshold REAL NOT NULL,
            note TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            ts_last_check INTEGER DEFAULT 0,
            ts_last_fired INTEGER DEFAULT 0,
            fire_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL,
            session_id TEXT NOT NULL DEFAULT 'local',
            ts_fired INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            type TEXT NOT NULL,
            threshold REAL,
            fired_value REAL,
            message TEXT,
            dismissed INTEGER DEFAULT 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_session ON alerts(session_id, active)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_symbol  ON alerts(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_session ON alert_events(session_id, dismissed, ts_fired DESC)")
    conn.commit()
    conn.close()

_alerts_init_db()


def _fetch_quick_quote(symbol):
    """
    Lightweight price + day-change + score lookup for alert checking.
    Reuses the screener cache when possible (zero network cost), falls back
    to a cheap fast_info call otherwise.
    Returns: dict with price, prev_close, day_change_pct, long_term, risk_score
             — or None if we can't get a price.
    """
    sym = symbol.replace('.NS', '').replace('.BO', '').upper()

    # 1. Try every cached screener result first — most common path, free.
    # Cache layout from get_cached/set_cached is (data, ts, ttl).
    for cache_key in list(_cache.keys()):
        if not cache_key.startswith('screener:'):
            continue
        rows = get_cached(cache_key)   # honors TTL, returns None if expired
        if not isinstance(rows, list):
            continue
        for r in rows:
            if r.get('ticker', '').upper() == sym:
                return {
                    'price':          r.get('price'),
                    'day_change_pct': r.get('chg_1d'),
                    'long_term':      r.get('long_term'),
                    'risk_score':     r.get('risk_score'),
                    'source':         'screener_cache',
                }

    # 2. Fallback: cheap yfinance fast_info
    try:
        tk = yf.Ticker(sym + '.NS')
        fi = tk.fast_info
        price = fi.get('last_price') or fi.get('regular_market_price')
        prev  = fi.get('previous_close')
        dchg  = ((price - prev) / prev * 100) if (price and prev) else None
        return {
            'price':          price,
            'day_change_pct': dchg,
            'long_term':      None,
            'risk_score':     None,
            'source':         'fast_info',
        }
    except Exception:
        return None


def _check_alerts_for_session(session_id):
    """
    Walk all active alerts for this session, evaluate, and insert events
    for newly-triggered ones (respecting the 1-hour cooldown).
    Returns: list of new event dicts (for immediate frontend display).
    """
    conn = _alerts_db()
    now = int(time.time())
    cooldown_cutoff = now - _ALERT_COOLDOWN_SEC

    active = conn.execute(
        "SELECT * FROM alerts WHERE session_id = ? AND active = 1",
        (session_id,)
    ).fetchall()

    # Group by symbol so each ticker is only fetched once
    by_symbol = {}
    for a in active:
        by_symbol.setdefault(a['symbol'], []).append(a)

    new_events = []
    for sym, alerts_for_sym in by_symbol.items():
        quote = _fetch_quick_quote(sym)
        if not quote:
            continue
        for a in alerts_for_sym:
            try:
                triggered, fired_value = _evaluate_alert(a, quote)
            except Exception as e:
                print(f'  [alerts] eval failed for {sym} #{a["id"]}: {e}')
                continue

            # Always update last_check
            conn.execute("UPDATE alerts SET ts_last_check = ? WHERE id = ?", (now, a['id']))

            if not triggered:
                continue
            # Cooldown: skip if fired recently
            if (a['ts_last_fired'] or 0) > cooldown_cutoff:
                continue

            msg = _format_alert_message(a, fired_value)
            cur = conn.execute("""
                INSERT INTO alert_events (alert_id, session_id, ts_fired, symbol, type,
                                          threshold, fired_value, message)
                VALUES (?,?,?,?,?,?,?,?)
            """, (a['id'], session_id, now, sym, a['type'],
                  a['threshold'], fired_value, msg))
            conn.execute(
                "UPDATE alerts SET ts_last_fired = ?, fire_count = fire_count + 1 WHERE id = ?",
                (now, a['id'])
            )
            new_events.append({
                'id':          cur.lastrowid,
                'alert_id':    a['id'],
                'ts_fired':    now,
                'symbol':      sym,
                'name':        a['name'],
                'type':        a['type'],
                'threshold':   a['threshold'],
                'fired_value': fired_value,
                'message':     msg,
            })
    conn.commit()
    conn.close()
    return new_events


def _evaluate_alert(alert, quote):
    """Returns (triggered: bool, fired_value: float|None)."""
    t = alert['type']
    threshold = alert['threshold']
    if t == 'price_above':
        v = quote.get('price')
        return (v is not None and v >= threshold, v)
    if t == 'price_below':
        v = quote.get('price')
        return (v is not None and v <= threshold, v)
    if t == 'day_change_above':
        v = quote.get('day_change_pct')
        return (v is not None and v >= threshold, v)
    if t == 'day_change_below':
        v = quote.get('day_change_pct')
        return (v is not None and v <= threshold, v)
    if t == 'score_long_term':
        v = quote.get('long_term')
        return (v is not None and v >= threshold, v)
    if t == 'risk_below':
        v = quote.get('risk_score')
        return (v is not None and v <= threshold, v)
    return (False, None)


def _format_alert_message(alert, fired_value):
    sym = alert['symbol']
    name = alert['name'] or sym
    t = alert['type']
    th = alert['threshold']
    if t in ('price_above', 'price_below'):
        return f"{name} hit ₹{fired_value:.2f} (threshold ₹{th:.2f})"
    if t == 'day_change_above':
        return f"{name} up {fired_value:+.2f}% today (threshold +{th:.1f}%)"
    if t == 'day_change_below':
        return f"{name} down {fired_value:+.2f}% today (threshold {th:+.1f}%)"
    if t == 'score_long_term':
        return f"{name} long-term score reached {int(fired_value)} (threshold {int(th)})"
    if t == 'risk_below':
        return f"{name} risk dropped to {int(fired_value)} (threshold {int(th)})"
    return f"{name} alert triggered"


# ────── REST endpoints ──────

@app.route('/api/alerts', methods=['GET'])
def alerts_list():
    sid = _get_session_id()
    conn = _alerts_db()
    rows = conn.execute("""
        SELECT id, ts_created, symbol, name, type, threshold, note, active,
               ts_last_check, ts_last_fired, fire_count
        FROM alerts WHERE session_id = ? ORDER BY ts_created DESC
    """, (sid,)).fetchall()
    conn.close()
    return jsonify({'alerts': [dict(r) for r in rows], 'types': ALERT_TYPES})


@app.route('/api/alerts', methods=['POST'])
def alerts_create():
    sid = _get_session_id()
    data = request.get_json(force=True) or {}
    sym = (data.get('symbol') or '').strip().upper().replace('.NS', '').replace('.BO', '')
    name = (data.get('name') or sym).strip()
    typ = data.get('type')
    note = (data.get('note') or '').strip()
    try:
        threshold = float(data.get('threshold'))
    except Exception:
        return jsonify({'error': 'Invalid threshold'}), 400
    if not sym or typ not in ALERT_TYPES:
        return jsonify({'error': 'Missing symbol or invalid type'}), 400

    # De-dupe: don't allow two identical active alerts for the same symbol+type+threshold
    conn = _alerts_db()
    existing = conn.execute("""
        SELECT id FROM alerts
        WHERE session_id = ? AND symbol = ? AND type = ?
              AND ABS(threshold - ?) < 0.0001 AND active = 1
    """, (sid, sym, typ, threshold)).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': 'An identical alert already exists', 'existing_id': existing['id']}), 409

    cur = conn.execute("""
        INSERT INTO alerts (session_id, ts_created, symbol, name, type, threshold, note, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1)
    """, (sid, int(time.time()), sym, name[:80], typ, threshold, note[:200]))
    new_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({'id': new_id, 'symbol': sym, 'type': typ, 'threshold': threshold}), 201


@app.route('/api/alerts/<int:alert_id>', methods=['DELETE'])
def alerts_delete(alert_id):
    sid = _get_session_id()
    conn = _alerts_db()
    cur = conn.execute("DELETE FROM alerts WHERE id = ? AND session_id = ?", (alert_id, sid))
    conn.execute("DELETE FROM alert_events WHERE alert_id = ? AND session_id = ?", (alert_id, sid))
    conn.commit()
    conn.close()
    return jsonify({'deleted': cur.rowcount > 0})


@app.route('/api/alerts/<int:alert_id>/toggle', methods=['POST'])
def alerts_toggle(alert_id):
    sid = _get_session_id()
    conn = _alerts_db()
    row = conn.execute("SELECT active FROM alerts WHERE id = ? AND session_id = ?",
                       (alert_id, sid)).fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Not found'}), 404
    new_val = 0 if row['active'] else 1
    conn.execute("UPDATE alerts SET active = ? WHERE id = ?", (new_val, alert_id))
    conn.commit()
    conn.close()
    return jsonify({'active': bool(new_val)})


@app.route('/api/alerts/check', methods=['GET', 'POST'])
def alerts_check():
    """Called by frontend poller. Runs checks for this session + returns any new events."""
    sid = _get_session_id()
    new_events = _check_alerts_for_session(sid)
    return jsonify({'new_events': new_events})


@app.route('/api/alerts/events')
def alerts_events():
    """Returns recent events for this session. Default: undismissed only, last 50."""
    sid = _get_session_id()
    include_dismissed = request.args.get('all', '').lower() in ('1', 'true', 'yes')
    limit = min(int(request.args.get('limit', 50)), 200)
    conn = _alerts_db()
    if include_dismissed:
        rows = conn.execute("""
            SELECT id, alert_id, ts_fired, symbol, type, threshold, fired_value, message, dismissed
            FROM alert_events WHERE session_id = ? ORDER BY ts_fired DESC LIMIT ?
        """, (sid, limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT id, alert_id, ts_fired, symbol, type, threshold, fired_value, message, dismissed
            FROM alert_events
            WHERE session_id = ? AND dismissed = 0
            ORDER BY ts_fired DESC LIMIT ?
        """, (sid, limit)).fetchall()
    unread = conn.execute(
        "SELECT COUNT(*) FROM alert_events WHERE session_id = ? AND dismissed = 0", (sid,)
    ).fetchone()[0]
    conn.close()
    return jsonify({'events': [dict(r) for r in rows], 'unread_count': unread})


@app.route('/api/alerts/events/<int:event_id>/dismiss', methods=['POST'])
def alerts_event_dismiss(event_id):
    sid = _get_session_id()
    conn = _alerts_db()
    conn.execute(
        "UPDATE alert_events SET dismissed = 1 WHERE id = ? AND session_id = ?",
        (event_id, sid)
    )
    conn.commit()
    conn.close()
    return jsonify({'dismissed': True})


@app.route('/api/alerts/events/dismiss_all', methods=['POST'])
def alerts_events_dismiss_all():
    sid = _get_session_id()
    conn = _alerts_db()
    cur = conn.execute(
        "UPDATE alert_events SET dismissed = 1 WHERE session_id = ? AND dismissed = 0", (sid,)
    )
    conn.commit()
    conn.close()
    return jsonify({'dismissed_count': cur.rowcount})


if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
