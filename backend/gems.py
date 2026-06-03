"""
Gems — Fundamentally strong + undervalued stock finder.
FA-weighted scoring (FA 65% / TA 10% / News 15% / Social 5% / +Value bonus 5%).
"""
import os
import sys
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(__file__))

import yfinance as yf
import pandas as pd
import numpy as np

from analyzer import _yf_ticker, _yahoo_chart_direct


# ── Universe: ~150 quality US stocks for value scanning ──────────────────────
# Curated mix of large/mid-cap quality names where fundamentals data is reliable.
GEMS_UNIVERSE = [
    # Tech
    'AAPL','MSFT','GOOGL','META','NVDA','AMD','INTC','CSCO','ORCL','IBM','ADBE','CRM',
    'NOW','TXN','QCOM','AVGO','MU','AMAT','LRCX','ASML','TSM','UBER','LYFT','SHOP',
    'PYPL','SQ','HOOD','SOFI','PLTR','SNAP','PINS','SPOT','NFLX',
    # Financials
    'JPM','BAC','WFC','C','GS','MS','BLK','SCHW','AXP','V','MA','BRK-B','USB','PNC',
    'TFC','COF','DFS','SYF','ALL','TRV','CB','AIG','MET','PRU','AFL','HIG',
    # Healthcare
    'JNJ','PFE','MRK','ABBV','LLY','BMY','TMO','DHR','UNH','CVS','CI','HUM','GILD',
    'AMGN','REGN','VRTX','BIIB','MRNA','ZTS','MDT','SYK','BSX','BDX','ABT','ISRG',
    # Consumer
    'WMT','COST','TGT','HD','LOW','MCD','SBUX','NKE','TJX','ROST','DG','DLTR','BBY',
    'KR','SYY','CL','PG','KO','PEP','MDLZ','MO','PM','EL','CHWY','ETSY','EBAY',
    # Energy
    'XOM','CVX','COP','EOG','SLB','OXY','MPC','PSX','VLO','PXD','DVN','HES','APA',
    # Industrials
    'BA','LMT','RTX','GE','HON','CAT','DE','UPS','FDX','UNP','CSX','NSC','EMR','ETN',
    'PH','ITW','MMM','GD','NOC','LHX','GWW','FAST',
    # Communication
    'DIS','VZ','T','TMUS','CMCSA','CHTR','WBD','PARA','EA','TTWO',
    # Materials / Utilities / Real Estate
    'LIN','APD','FCX','NEM','SHW','DD','NUE','STLD','SO','DUK','NEE','D','AEP','SRE',
    'AMT','CCI','EQIX','PSA','SPG','O','VICI','WELL','DLR','PLD',
]


def _has(d, k):
    v = d.get(k)
    if v is None: return False
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f): return False
        return True
    except Exception:
        return False


def _gnum(d, k, default=None):
    if not _has(d, k): return default
    try: return float(d.get(k))
    except: return default


def _classify_tags(m):
    """Determine quality tags based on fundamental snapshot."""
    tags = []
    pe = m.get('pe'); pb = m.get('pb'); ps = m.get('ps')
    div = m.get('div_yield') or 0
    rev_g = m.get('revenue_growth') or 0
    margin = m.get('profit_margin') or 0
    debt_eq = m.get('debt_equity')

    # Undervalued: low multiples
    undervalued_signals = 0
    if pe and pe < 15: undervalued_signals += 1
    if pb and pb < 2: undervalued_signals += 1
    if ps and ps < 2: undervalued_signals += 1
    if undervalued_signals >= 2:
        tags.append('undervalued')

    # Quality: strong margins + low debt
    if margin > 0.15 and (debt_eq is None or debt_eq < 1.0):
        tags.append('quality')

    # Growth: revenue growth > 15%
    if rev_g > 0.15:
        tags.append('grower')

    # Dividend: yield > 3%
    if div > 0.03:
        tags.append('divi')

    # Low risk proxy: large cap + low debt
    mcap = m.get('market_cap') or 0
    if mcap > 50e9 and (debt_eq is None or debt_eq < 0.8):
        tags.append('lowrisk')

    return tags


def _score_gem(m):
    """FA-weighted composite: FA 65% + TA 10% + News 15% + Social 5% + Value +5%."""
    # FA component (0-100)
    fa = 0
    signals = 0
    # Margin scoring
    margin = m.get('profit_margin')
    if margin is not None:
        signals += 1
        if margin > 0.20: fa += 20
        elif margin > 0.10: fa += 12
        elif margin > 0.05: fa += 6
        elif margin > 0: fa += 0
        else: fa -= 10
    # Revenue growth
    rev_g = m.get('revenue_growth')
    if rev_g is not None:
        signals += 1
        if rev_g > 0.30: fa += 20
        elif rev_g > 0.15: fa += 14
        elif rev_g > 0.05: fa += 8
        elif rev_g > 0: fa += 2
        else: fa -= 8
    # P/E
    pe = m.get('pe')
    if pe is not None and pe > 0:
        signals += 1
        if pe < 10: fa += 15
        elif pe < 15: fa += 10
        elif pe < 20: fa += 5
        elif pe < 30: fa += 0
        else: fa -= 8
    # ROE
    roe = m.get('roe')
    if roe is not None:
        signals += 1
        if roe > 0.20: fa += 15
        elif roe > 0.12: fa += 10
        elif roe > 0.05: fa += 4
        else: fa -= 4
    # Debt/Equity (lower better)
    de = m.get('debt_equity')
    if de is not None:
        signals += 1
        if de < 0.3: fa += 10
        elif de < 0.7: fa += 5
        elif de < 1.5: fa += 0
        else: fa -= 8
    # Dividend (bonus)
    div = m.get('div_yield')
    if div and div > 0.03: fa += 5

    fa_max = signals * 20
    fa_norm = (fa / fa_max * 100) if fa_max > 0 else 0
    fa_norm = max(-100, min(100, fa_norm))

    # TA component (use 50-day SMA position as proxy — small weight)
    ta = 0
    pct_above_sma = m.get('pct_above_sma50')
    if pct_above_sma is not None:
        if pct_above_sma > 5: ta = 30
        elif pct_above_sma > 0: ta = 15
        elif pct_above_sma > -5: ta = 0
        else: ta = -20

    # Value bonus
    value_bonus = 0
    if pe and pe < 12 and m.get('pb') and m.get('pb') < 1.5: value_bonus += 50
    if div and div > 0.04: value_bonus += 30
    value_bonus = min(100, value_bonus)

    # News & Social — unavailable without per-ticker fetches; default 0
    news = 0
    social = 0

    composite = (fa_norm * 0.65 + ta * 0.10 + news * 0.15 + social * 0.05 + value_bonus * 0.05)
    composite = round(max(-100, min(100, composite)))

    # Low-confidence guard: a "gem" must have enough fundamental data to judge.
    # With <3 FA signals the FA score is unreliable — cap so it can't rank as
    # a strong buy off a single metric (e.g. only a P/E).
    if signals < 3:
        composite = min(composite, 15)

    return composite


def _fetch_one(ticker):
    """Fetch fundamentals + sparkline for one ticker."""
    try:
        # Get info via direct Yahoo; fall back to static GitHub info when Yahoo
        # is blocked / returns a stub (common on Render's shared IP).
        info = {}
        try:
            info = _yf_ticker(ticker).info or {}
        except Exception:
            info = {}
        if not info or len(info) < 5 or info.get('trailingPE') is None and info.get('sector') is None:
            try:
                from static_fallback import static_fetch_info
                sinfo = static_fetch_info(ticker)
                if sinfo:
                    # Merge: prefer live values, fill gaps from static
                    merged = dict(sinfo)
                    merged.update({k: v for k, v in info.items() if v is not None})
                    info = merged
            except Exception:
                pass

        # OHLCV for sparkline + 52w + current price
        df = _yahoo_chart_direct(ticker, period='3mo')
        if df is None or df.empty:
            try:
                from static_fallback import static_fetch_ohlcv
                df = static_fetch_ohlcv(ticker)
            except Exception:
                df = None
        if df is None or df.empty:
            return None

        current = float(df['Close'].iloc[-1])
        prev = float(df['Close'].iloc[-2]) if len(df) >= 2 else current
        change_pct = (current - prev) / prev * 100 if prev > 0 else 0
        spark = [float(x) for x in df['Close'].tail(30).values]
        high_52w = float(df['High'].max()) if 'High' in df.columns else current
        low_52w  = float(df['Low'].min())  if 'Low'  in df.columns else current

        # SMA 50 position
        sma50 = float(df['Close'].rolling(50).mean().iloc[-1]) if len(df) >= 50 else current
        pct_above_sma50 = ((current - sma50) / sma50 * 100) if sma50 else 0

        metrics = {
            'ticker': ticker,
            'company': info.get('longName') or info.get('shortName') or ticker,
            'sector':  info.get('sector', 'Unknown'),
            'industry': info.get('industry', ''),
            'market_cap': _gnum(info, 'marketCap'),
            'price': round(current, 2),
            'change_pct': round(change_pct, 2),
            'spark': spark,
            'high_52w': round(high_52w, 2),
            'low_52w':  round(low_52w, 2),
            'pe':  _gnum(info, 'trailingPE'),
            'pb':  _gnum(info, 'priceToBook'),
            'ps':  _gnum(info, 'priceToSalesTrailing12Months'),
            'peg': _gnum(info, 'pegRatio'),
            'profit_margin': _gnum(info, 'profitMargins'),
            'roe': _gnum(info, 'returnOnEquity'),
            'revenue_growth': _gnum(info, 'revenueGrowth'),
            'div_yield': _gnum(info, 'dividendYield'),
            'debt_equity': _gnum(info, 'debtToEquity'),
            'pct_above_sma50': pct_above_sma50,
        }

        # Normalize debt/equity (yfinance returns as percent like 50.0 = 0.5)
        if metrics['debt_equity'] and metrics['debt_equity'] > 5:
            metrics['debt_equity'] = metrics['debt_equity'] / 100

        # Normalize dividend yield — yfinance 0.2.5x+ returns it as a percent
        # number (e.g. 2.25 meaning 2.25%) instead of a fraction (0.0225).
        # Anything > 1 is clearly already a percent → convert to fraction.
        if metrics['div_yield'] and metrics['div_yield'] > 1:
            metrics['div_yield'] = metrics['div_yield'] / 100

        metrics['tags'] = _classify_tags(metrics)
        metrics['score'] = _score_gem(metrics)
        return metrics
    except Exception:
        return None


def _apply_filters(g, f):
    """Return True if gem passes all filters."""
    if g['price'] > f['max_price']: return False
    if g['pe'] is not None and g['pe'] > f['max_pe']: return False
    if f['min_growth'] > -20 and g.get('revenue_growth') is not None and g['revenue_growth'] * 100 < f['min_growth']: return False
    if f['min_margin'] > -10 and g.get('profit_margin') is not None and g['profit_margin'] * 100 < f['min_margin']: return False
    # Sector filter
    if f['sectors'] and 'ALL' not in f['sectors']:
        if g.get('sector') not in f['sectors']: return False
    # Tags — must have at least one selected tag
    if f['tags']:
        if not any(t in g['tags'] for t in f['tags']): return False
    return True


# ── Module-level cache to avoid rescanning the universe constantly ───
_GEMS_CACHE = {'ts': 0, 'data': []}
_GEMS_TTL = 3600  # 1 hour


def scan_gems(filters: dict):
    """Returns dict with gems list + metadata."""
    start = time.time()

    # Use cached universe data if fresh
    if _GEMS_CACHE['data'] and (time.time() - _GEMS_CACHE['ts']) < _GEMS_TTL:
        all_gems = _GEMS_CACHE['data']
    else:
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_fetch_one, GEMS_UNIVERSE))
        all_gems = [r for r in results if r is not None]
        _GEMS_CACHE['data'] = all_gems
        _GEMS_CACHE['ts'] = time.time()

    # Apply filters
    filtered = [g for g in all_gems if _apply_filters(g, filters)]
    # Sort by score desc
    filtered.sort(key=lambda x: x['score'], reverse=True)
    # Cap at 50 results to keep payload reasonable
    filtered = filtered[:50]

    return {
        'gems': filtered,
        'universe_scanned': len(GEMS_UNIVERSE),
        'duration_s': round(time.time() - start, 1),
        'cached': (time.time() - _GEMS_CACHE['ts']) < _GEMS_TTL and len(all_gems) == len(_GEMS_CACHE['data']),
    }
