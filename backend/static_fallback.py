"""
Static data fallback — reads pre-fetched OHLCV and fundamentals from the
GitHub repo (refreshed twice daily by a GitHub Actions cron job).

When Yahoo/Stooq/yfinance all fail due to Render IP blocks, this fallback
serves yesterday's data so the app keeps working.
"""
import io
import time
import requests as _requests
import pandas as pd


# Public GitHub raw URLs — no auth, CDN-cached, never rate-limited
GITHUB_USER   = 'saikb-deloitte'
GITHUB_REPO   = 'stock-analyzer'
GITHUB_BRANCH = 'main'
RAW_BASE = f'https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}/data'

# In-memory cache (10 min — static data changes only 2x daily anyway)
_CACHE: dict = {}
_TTL = 600


def _get(key):
    e = _CACHE.get(key)
    return e['data'] if e and (time.time() - e['ts']) < _TTL else None


def _put(key, data):
    _CACHE[key] = {'ts': time.time(), 'data': data}


def static_fetch_ohlcv(ticker: str):
    """Return a yfinance-shaped DataFrame from the static GitHub cache, or None."""
    ticker = ticker.upper()
    cache_key = f'ohlcv:{ticker}'
    cached = _get(cache_key)
    if cached is not None:
        return cached.copy()

    try:
        url = f'{RAW_BASE}/ohlcv/{ticker.replace("-", "_")}.json'
        r = _requests.get(url, timeout=10)
        if r.status_code != 200:
            return None
        j = r.json()
        if not j.get('timestamps'):
            return None
        df = pd.DataFrame({
            'Open':   j.get('open',   []),
            'High':   j.get('high',   []),
            'Low':    j.get('low',    []),
            'Close':  j.get('close',  []),
            'Volume': j.get('volume', []),
        }, index=pd.to_datetime(j['timestamps'], unit='s'))
        df = df.dropna(subset=['Close'])
        if df.empty:
            return None
        _put(cache_key, df.copy())
        return df
    except Exception:
        return None


def static_fetch_info(ticker: str) -> dict:
    """Return info dict (P/E, margins, sector, etc.) from static cache, or {}."""
    ticker = ticker.upper()
    cached = _get('info_all')
    if cached is None:
        try:
            r = _requests.get(f'{RAW_BASE}/info.json', timeout=10)
            if r.status_code == 200:
                cached = r.json().get('tickers', {}) or {}
                _put('info_all', cached)
            else:
                return {}
        except Exception:
            return {}
    return cached.get(ticker, {}) if cached else {}


def static_fetch_screen(universe: str):
    """Return pre-computed screener results for a universe from GitHub, or None."""
    universe = universe.strip().lower()
    cache_key = f'screen:{universe}'
    cached = _get(cache_key)
    if cached is not None:
        return cached
    try:
        r = _requests.get(f'{RAW_BASE}/screens/{universe}.json', timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        _put(cache_key, data)
        return data
    except Exception:
        return None


def static_screen_age() -> str | None:
    """Returns 'X ago' for the screens manifest, or None."""
    cached = _get('screens_manifest')
    if cached is None:
        try:
            r = _requests.get(f'{RAW_BASE}/screens/manifest.json', timeout=5)
            if r.status_code == 200:
                cached = r.json()
                _put('screens_manifest', cached)
        except Exception:
            return None
    if not cached or not cached.get('updated'):
        return None
    try:
        from datetime import datetime, timezone
        updated = datetime.fromisoformat(cached['updated'].replace('Z', '+00:00'))
        delta = datetime.now(timezone.utc) - updated
        h = delta.total_seconds() / 3600
        if h < 1: return f'{int(delta.total_seconds()/60)}m ago'
        if h < 24: return f'{int(h)}h ago'
        return f'{int(h/24)}d ago'
    except Exception:
        return None


def static_data_available() -> bool:
    """Quick check — is the manifest reachable?"""
    cached = _get('manifest')
    if cached is not None:
        return True
    try:
        r = _requests.get(f'{RAW_BASE}/manifest.json', timeout=5)
        if r.status_code == 200:
            _put('manifest', r.json())
            return True
    except Exception:
        pass
    return False


def static_data_age() -> str | None:
    """Returns 'X hours ago' string from manifest, or None."""
    cached = _get('manifest')
    if cached is None:
        if not static_data_available():
            return None
        cached = _get('manifest')
    if not cached or not cached.get('updated'):
        return None
    try:
        from datetime import datetime, timezone
        updated = datetime.fromisoformat(cached['updated'].replace('Z', '+00:00'))
        delta = datetime.now(timezone.utc) - updated
        hours = delta.total_seconds() / 3600
        if hours < 1: return f'{int(delta.total_seconds()/60)}m ago'
        if hours < 24: return f'{int(hours)}h ago'
        return f'{int(hours/24)}d ago'
    except Exception:
        return None
