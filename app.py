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
# Public deploys triple the TTL (15 min) — yfinance gets cranky from cloud IPs
# so we hit it less aggressively per user.
_cache = {}
CACHE_TTL = 900 if IS_PUBLIC else 300

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


def get_cached(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None


def set_cached(key, data):
    _cache[key] = (data, time.time())


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

    # Short-term targets: prefer real resistance zones if they sit within reach
    st_t1_method = 'R:R 2.0×'
    st_t2_method = 'R:R 3.0×'
    st_t1_info = None
    st_t2_info = None
    st_t1 = curr + st_risk * 2.0
    st_t2 = curr + st_risk * 3.0
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

    # ── Mid-term: stop at max(swing low, 3×ATR, EMA50 × 0.95), capped at -12%
    mt_stop_raw = max(swing_low_mt * 0.97, curr - 3 * atr, ema50 * 0.95 if ema50 > 0 else 0)
    mt_stop = min(mt_stop_raw, curr * 0.92)
    mt_stop = max(mt_stop, curr * 0.85)
    mt_risk = curr - mt_stop

    mt_t1_method = 'R:R 2.0×'
    mt_t2_method = 'R:R 3.5×'
    mt_t1_info = None
    mt_t2_info = None
    mt_t1 = curr + mt_risk * 2.0
    mt_t2 = curr + mt_risk * 3.5
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
    # Final cap on T2: don't promise crazy stretches
    if mt_t2 > high52 * 1.20 and 'Resistance' not in mt_t2_method:
        mt_t2 = high52 * 1.10

    # ── Long-term: EMA200-anchored stop (up to -22%), targets driven by fair value
    ema200 = float(close.ewm(span=200, adjust=False).mean().iloc[-1]) if len(close) >= 50 else ema50
    lt_stop_raw = max(low52 * 1.02, ema200 * 0.92 if ema200 > 0 else 0, curr - 5 * atr)
    lt_stop = min(lt_stop_raw, curr * 0.85)   # at least -15% room
    lt_stop = max(lt_stop, curr * 0.78)        # cap at -22%
    lt_risk = curr - lt_stop

    lt_t1_method = 'R:R 3.0×'
    lt_t2_method = 'R:R 5.0×'
    lt_t1 = curr + lt_risk * 3.0
    lt_t2 = curr + lt_risk * 5.0

    fair_value = compute_fair_value(info, curr) if info else None
    if fair_value and fair_value.get('fair_value') and fair_value['fair_value'] > curr * 1.05:
        # Stock is undervalued — use fair value as the anchor
        fv = fair_value['fair_value']
        lt_t1 = (curr + fv) / 2.0    # midpoint of journey
        lt_t2 = fv * 1.05            # slight overshoot of fair value
        lt_t1_method = 'Midpoint to Fair Value'
        lt_t2_method = f"Fair Value × 1.05 ({fair_value['verdict']})"
    elif fair_value and fair_value.get('fair_value') and fair_value['fair_value'] >= curr * 0.95:
        # Near fair value — conservative targets
        fv = fair_value['fair_value']
        # Target a modest 15-25% above CMP (organic earnings growth over 1-2y)
        lt_t1 = max(curr * 1.15, fv)
        lt_t2 = curr * 1.30
        lt_t1_method = 'Earnings Growth Anchor'
        lt_t2_method = '12-24m Conservative Target'
    else:
        # Overvalued or no fair value — fall back to R:R math with caps
        near_high = high52 > 0 and curr >= high52 * 0.95
        if near_high:
            lt_t1 = max(lt_t1, curr + 10 * atr)
            lt_t2 = max(lt_t2, curr + 20 * atr)
            lt_t1 = min(lt_t1, curr * 1.60)
            lt_t2 = min(lt_t2, curr * 2.20)
        else:
            if lt_t2 > high52 * 1.30:
                lt_t2 = high52 * 1.25
            if lt_t1 > high52 * 1.10 and high52 > curr:
                lt_t1 = max(curr * 1.20, high52 * 1.05)

    # Final sanity guards
    if lt_t1 <= curr:
        lt_t1 = curr * 1.20
    if lt_t2 <= lt_t1:
        lt_t2 = lt_t1 * 1.20

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
        # If sector/industry are missing, at least set a generic label so the
        # screener heat-map doesn't break on grouping
        if not info.get('sector'):
            info['sector'] = info.get('sector') or 'Unknown'
        if not info.get('industry'):
            info['industry'] = info.get('industry') or 'Unknown'

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

        # News (yfinance only — NSE doesn't provide structured news)
        news = []
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
                    news.append({'title': title, 'publisher': pub, 'url': url, 'published': ts})
        except Exception:
            pass

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
            'name':     info.get('longName') or info.get('shortName') or ticker,
            'sector':   info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'exchange': info.get('exchange', 'NSE'),
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
            return {
                'ticker':    ticker.replace('.NS', '').replace('.BO', ''),
                'name':      info.get('shortName') or info.get('longName') or ticker,
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
                'mktcap':    info.get('marketCap'),
                'sector':    info.get('sector', 'N/A'),
                'sector_grouped': standardize_sector(info.get('sector'), info.get('industry')),
                'industry':  info.get('industry'),
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
        if not rows:
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
    picks.sort(key=lambda x: (x.get('long_term', 0) - 0.5 * (x.get('risk_score') or 50)), reverse=True)

    # Group by sector
    by_sector = {}
    for p in picks:
        s = p.get('sector') or 'Other'
        by_sector.setdefault(s, []).append(p)
    sector_groups = sorted(
        [{'sector': k, 'count': len(v), 'avg_lt': round(sum(x['long_term'] for x in v) / len(v), 1),
          'avg_risk': round(sum((x['risk_score'] or 50) for x in v) / len(v), 1), 'stocks': v}
         for k, v in by_sector.items()],
        key=lambda x: (-x['avg_lt'], x['avg_risk'])
    )

    # Group by index too
    by_index = {}
    for p in picks:
        for idx in (p.get('indices') or ['Other']):
            by_index.setdefault(idx, []).append(p)
    index_groups = sorted(
        [{'index': k, 'count': len(v), 'avg_lt': round(sum(x['long_term'] for x in v) / len(v), 1),
          'avg_risk': round(sum((x['risk_score'] or 50) for x in v) / len(v), 1), 'stocks': v}
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


if __name__ == '__main__':
    app.run(debug=True, port=5001, host='0.0.0.0')
