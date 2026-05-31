import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import yfinance as yf
import pandas as pd
import numpy as np
import pytz
import time
import random
import io
import requests as _requests
from datetime import datetime, time as dtime

from concurrent.futures import ThreadPoolExecutor


# ── curl_cffi browser-impersonating session ──────────────────────────────────
_yf_session = None
def _get_yf_session():
    global _yf_session
    if _yf_session is not None:
        return _yf_session
    try:
        from curl_cffi import requests as curl_requests
        _yf_session = curl_requests.Session(impersonate="chrome131")
    except Exception:
        _yf_session = None
    return _yf_session


# ── Stooq: free, no-auth daily OHLCV source ──────────────────────────────────
_STOOQ_LAST_ERROR = None  # exposed by /diag

def _stooq_download(ticker, period_days=365):
    """Download daily OHLCV from Stooq. Returns DataFrame or None."""
    global _STOOQ_LAST_ERROR
    sym = ticker.lower() + '.us'
    # Try multiple URL formats — Stooq sometimes blocks cloud IPs on www, but
    # the bare domain endpoint often works.
    urls = [
        f'https://stooq.com/q/d/l/?s={sym}&i=d',
        f'https://stooq.com/q/d/l/?s={sym}&i=d&o=1111111&c=0',
    ]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/csv,text/plain,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    for url in urls:
        try:
            r = _requests.get(url, timeout=15, headers=headers)
            text = r.text.strip()
            if r.status_code != 200:
                _STOOQ_LAST_ERROR = f'HTTP {r.status_code} from {url}'
                continue
            if not text or text.lower().startswith('no data') or '<html' in text.lower()[:200]:
                _STOOQ_LAST_ERROR = f'Bad body (status={r.status_code}, first 80 chars): {text[:80]!r}'
                continue
            df = pd.read_csv(io.StringIO(text))
            if df.empty or 'Close' not in df.columns:
                _STOOQ_LAST_ERROR = f'Parsed CSV missing Close col. Cols: {list(df.columns)}'
                continue
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date').sort_index()
            if period_days:
                cutoff = df.index[-1] - pd.Timedelta(days=period_days + 14)
                df = df[df.index >= cutoff]
            _STOOQ_LAST_ERROR = None
            return df
        except Exception as e:
            _STOOQ_LAST_ERROR = f'Exception on {url}: {type(e).__name__}: {e}'
    return None


_DIRECT_YAHOO_LAST_ERROR = None

# Rotate through different browser fingerprints — Yahoo blocks individual profiles
# but rotating diversifies enough to often slip through.
_IMPERSONATE_POOL = ['chrome131', 'chrome124', 'chrome120', 'safari17_2_ios', 'edge101']

def _yahoo_chart_direct(ticker, period='1y'):
    """Call Yahoo's chart API DIRECTLY, rotating through 5 browser fingerprints."""
    global _DIRECT_YAHOO_LAST_ERROR
    try:
        from curl_cffi import requests as curl_requests
    except Exception as e:
        _DIRECT_YAHOO_LAST_ERROR = f'curl_cffi missing: {e}'
        return None

    url = f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}'
    params = {'range': period, 'interval': '1d', 'includePrePost': 'false', 'events': 'div,split'}

    for impersonate in _IMPERSONATE_POOL:
        try:
            s = curl_requests.Session(impersonate=impersonate)
            r = s.get(url, params=params, timeout=12)
            if r.status_code != 200:
                _DIRECT_YAHOO_LAST_ERROR = f'{impersonate}: HTTP {r.status_code}'
                continue
            j = r.json()
            result = (j.get('chart', {}).get('result') or [None])[0]
            if not result:
                _DIRECT_YAHOO_LAST_ERROR = f'{impersonate}: no result in JSON'
                continue
            timestamps = result.get('timestamp', [])
            ind = result.get('indicators', {})
            quote = (ind.get('quote') or [{}])[0]
            adj = ((ind.get('adjclose') or [{}])[0]).get('adjclose', [])
            if not timestamps or not quote.get('close'):
                _DIRECT_YAHOO_LAST_ERROR = f'{impersonate}: empty timestamps/close'
                continue
            df = pd.DataFrame({
                'Open':   quote.get('open', []),
                'High':   quote.get('high', []),
                'Low':    quote.get('low', []),
                'Close':  adj if adj else quote.get('close', []),
                'Volume': quote.get('volume', []),
            }, index=pd.to_datetime(timestamps, unit='s'))
            df = df.dropna(subset=['Close'])
            if df.empty:
                _DIRECT_YAHOO_LAST_ERROR = f'{impersonate}: empty df after dropna'
                continue
            _DIRECT_YAHOO_LAST_ERROR = None
            return df
        except Exception as e:
            _DIRECT_YAHOO_LAST_ERROR = f'{impersonate}: {type(e).__name__}: {e}'
            continue
    return None


_TWELVE_DATA_LAST_ERROR = None

def _twelve_data_download(ticker, period_days=365):
    """Twelve Data API — requires TWELVE_DATA_API_KEY env var. 800 req/day free."""
    global _TWELVE_DATA_LAST_ERROR
    key = os.environ.get('TWELVE_DATA_API_KEY', '').strip()
    if not key:
        _TWELVE_DATA_LAST_ERROR = 'no API key set'
        return None
    # Twelve Data: outputsize is bars to return; ~252 bars per year
    outputsize = min(int(period_days * 0.7), 5000)
    url = 'https://api.twelvedata.com/time_series'
    params = {'symbol': ticker, 'interval': '1day', 'outputsize': outputsize, 'apikey': key, 'order': 'asc'}
    try:
        r = _requests.get(url, params=params, timeout=15)
        j = r.json()
        if j.get('status') == 'error' or j.get('code'):
            _TWELVE_DATA_LAST_ERROR = j.get('message', 'error') + f' (code {j.get("code")})'
            return None
        values = j.get('values', [])
        if not values:
            _TWELVE_DATA_LAST_ERROR = 'no values in response'
            return None
        df = pd.DataFrame(values)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime').sort_index()
        # Cast string columns to float
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.rename(columns={'open':'Open','high':'High','low':'Low','close':'Close','volume':'Volume'})
        _TWELVE_DATA_LAST_ERROR = None
        return df
    except Exception as e:
        _TWELVE_DATA_LAST_ERROR = f'{type(e).__name__}: {e}'
        return None


def _yf_download_with_retry(ticker, **kwargs):
    """Multi-source OHLCV cascade: tries fastest/most-reliable first."""
    period = kwargs.get('period', '1y')
    period_days = {'5d': 7, '1mo': 31, '3mo': 93, '6mo': 186, '1y': 365, '2y': 730, '5y': 1825}.get(period, 365)
    is_daily = kwargs.get('interval', '1d') == '1d'

    if is_daily:
        # 1️⃣ Twelve Data (if key is configured) — no IP blocks
        df = _twelve_data_download(ticker, period_days=period_days)
        if df is not None and not df.empty and len(df) >= 60:
            return df

        # 2️⃣ Direct Yahoo chart API via curl_cffi (bypasses yfinance lib wrapper)
        df = _yahoo_chart_direct(ticker, period=period)
        if df is not None and not df.empty and len(df) >= 60:
            return df

        # 3️⃣ Stooq (often blocked from Render but try anyway — might work on retry)
        df = _stooq_download(ticker, period_days=period_days)
        if df is not None and not df.empty and len(df) >= 60:
            return df

        # 4️⃣ GitHub static fallback — pre-fetched twice daily via GitHub Actions
        try:
            from static_fallback import static_fetch_ohlcv
            df = static_fetch_ohlcv(ticker)
            if df is not None and not df.empty and len(df) >= 60:
                return df
        except Exception:
            pass

    # 5️⃣ Final fallback: yfinance library (with curl_cffi session)
    session = _get_yf_session()
    if session is not None:
        kwargs.setdefault('session', session)
    last_err = None
    for attempt in range(2):
        try:
            df = yf.download(ticker, **kwargs)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if 'rate limit' in err_str or 'too many' in err_str or '429' in err_str:
                time.sleep(2 ** attempt * 4 + random.uniform(0, 1))
            else:
                raise
        if attempt < 1:
            time.sleep(2 ** attempt * 4 + random.uniform(0, 1))
    if last_err:
        raise last_err
    raise ValueError(f'No price data found for {ticker} from any source')


def _yf_ticker(symbol):
    """Build a yf.Ticker with the browser-impersonating session attached."""
    session = _get_yf_session()
    return yf.Ticker(symbol, session=session) if session else yf.Ticker(symbol)

from technical import (compute_indicators, detect_patterns, compute_signals,
                       score_technical, find_support_resistance, compute_atr_percentile)
from fundamental import get_fundamentals, get_quarterly_trend
from news_analyzer import analyze_news, fetch_supplementary_news
from social_analyzer import get_social_sentiment
from risk_analyzer import compute_risk, compute_trade_setup, compute_atr_regime, compute_fibonacci_targets


def _get_market_status():
    """Returns (status, time_str) based on NYSE trading hours (ET)."""
    try:
        et = pytz.timezone('America/New_York')
        now = datetime.now(et)
        if now.weekday() >= 5:
            return 'closed', None
        t = now.time()
        if dtime(9, 30) <= t <= dtime(16, 0):
            return 'open', now.strftime('%I:%M %p ET').lstrip('0')
        elif dtime(4, 0) <= t < dtime(9, 30):
            return 'pre_market', now.strftime('%I:%M %p ET').lstrip('0')
        elif dtime(16, 0) < t <= dtime(20, 0):
            return 'after_hours', now.strftime('%I:%M %p ET').lstrip('0')
        return 'closed', None
    except Exception:
        return 'unknown', None


def get_verdict(score):
    if score >= 40:
        return 'STRONG BUY', '#10b981'
    elif score >= 20:
        return 'BUY', '#22c55e'
    elif score >= -20:
        return 'NEUTRAL', '#f59e0b'
    elif score >= -40:
        return 'AVOID', '#ef4444'
    else:
        return 'STRONG AVOID', '#dc2626'


def safe_float(v):
    try:
        f = float(v)
        return None if np.isnan(f) or np.isinf(f) else f
    except Exception:
        return None


def safe_list(series):
    return [safe_float(v) for v in series]


class StockAnalyzer:
    def __init__(self, ticker):
        self.ticker = ticker.upper().strip()

    def analyze(self):
        df = _yf_download_with_retry(self.ticker, period='1y', interval='1d',
                                     progress=False, auto_adjust=True)

        if df.empty:
            raise ValueError(f'No price data found for {self.ticker}')

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.copy()
        df = compute_indicators(df)
        atr_pct_rank = compute_atr_percentile(df)

        patterns = detect_patterns(df)
        signals = compute_signals(df)
        ta_score = score_technical(patterns, signals)

        try:
            fa_score, fa_metrics, fa_signals, fa_available = get_fundamentals(self.ticker)
        except Exception as e:
            fa_score = 0
            fa_metrics = {}
            fa_signals = [{'name': 'Fundamentals', 'value': f'Unavailable: {str(e)[:60]}', 'type': 'neutral', 'weight': 0}]
            fa_available = False

        # Fetch news, social, IV, and quarterly trend concurrently
        news_score = 0
        analyzed_news = []
        social_data = {"score": 0, "available": False, "total_mentions": 0, "sources": []}
        implied_vol = None
        quarterly_trend = None
        try:
            stock = _yf_ticker(self.ticker)
            raw_news = stock.news[:15] if stock.news else []

            def _fetch_iv():
                try:
                    exps = stock.options
                    if not exps:
                        return None
                    cur = safe_float(stock.fast_info.last_price) or 0
                    all_iv = []
                    for exp in exps[:2]:
                        chain = stock.option_chain(exp)
                        for opt in [chain.calls, chain.puts]:
                            atm = opt[abs(opt['strike'] - cur) / (cur + 1e-9) < 0.07]
                            all_iv.extend(atm['impliedVolatility'].dropna().tolist())
                    return round(float(np.mean(all_iv)) * 100, 1) if all_iv else None
                except Exception:
                    return None

            executor = ThreadPoolExecutor(max_workers=5)
            google_fut = executor.submit(fetch_supplementary_news, self.ticker)
            social_fut = executor.submit(get_social_sentiment, self.ticker)
            iv_fut     = executor.submit(_fetch_iv)
            qt_fut     = executor.submit(get_quarterly_trend, self.ticker)

            # Per-future timeouts — each call gets its own deadline
            try:
                google_articles = google_fut.result(timeout=8)
            except Exception:
                google_articles = []

            # Merge yfinance + Google News; deduplicate by first-5-word title match
            yf_titles = {' '.join(a.get('title', '').lower().split()[:5]) for a in raw_news}
            for a in google_articles:
                key = ' '.join(a.get('title', '').lower().split()[:5])
                if key and key not in yf_titles:
                    raw_news.append(a)
                    yf_titles.add(key)

            news_score, analyzed_news = analyze_news(self.ticker, raw_news)

            try:
                social_data = social_fut.result(timeout=12)
            except Exception:
                pass

            try:
                implied_vol = iv_fut.result(timeout=10)
            except Exception:
                pass

            try:
                quarterly_trend = qt_fut.result(timeout=10)
            except Exception:
                pass

            executor.shutdown(wait=False)
        except Exception:
            pass
        social_score = social_data.get("score", 0)

        # Update IV in risk after concurrent fetch completes (risk is computed below)
        try:
            risk = compute_risk(df, self.ticker)
        except Exception:
            atr_val = safe_float(df['ATR'].iloc[-1]) or 1.0
            risk = {
                'tier': 'Unknown', 'color': '#6b7280', 'score': 50,
                'beta': 1.0, 'hv_30d': 25.0, 'hv_1y': 25.0,
                'max_drawdown_1y': -15.0, 'atr': atr_val, 'atr_pct': 1.5,
                'sharpe_6m': None, 'high_52w': float(df['High'].max()),
                'low_52w': float(df['Low'].min()), 'pos_52w_pct': 50.0,
                'suggested_size_1pct': 0,
            }

        # ATR regime (Low Vol / Normal / High Vol)
        atr_regime_label, _ = compute_atr_regime(atr_pct_rank)
        risk['atr_regime'] = atr_regime_label
        risk['atr_pct_rank'] = round(atr_pct_rank, 1)
        risk['implied_vol'] = implied_vol  # filled in after concurrent fetch below

        # Fibonacci extension targets
        fibonacci = None
        try:
            fibonacci = compute_fibonacci_targets(df)
        except Exception:
            pass

        short_term, mid_term, long_term, st_dir, mt_dir, lt_dir, composite = compute_trade_setup(
            df, ta_score, fa_score, news_score, risk,
            atr_pct_rank=atr_pct_rank, fibonacci=fibonacci,
            social_score=social_score, fa_available=fa_available,
        )
        composite = round(composite)

        overall_v, overall_c = get_verdict(composite)
        st_v, st_c = get_verdict(int(ta_score * 0.65 + news_score * 0.25 + social_score * 0.10))
        if fa_available:
            mt_v, mt_c = get_verdict(int(fa_score * 0.55 + ta_score * 0.30 + news_score * 0.10 + social_score * 0.05))
            lt_v, lt_c = get_verdict(int(fa_score * 0.70 + ta_score * 0.15 + news_score * 0.10 + social_score * 0.05))
        else:
            mt_v, mt_c = get_verdict(int(ta_score * 0.65 + news_score * 0.25 + social_score * 0.10))
            lt_v, lt_c = get_verdict(int(ta_score * 0.65 + news_score * 0.25 + social_score * 0.10))

        sr_levels = find_support_resistance(df)

        stock_obj = _yf_ticker(self.ticker)
        try:
            info = stock_obj.info or {}
            if not info or len(info) < 5:
                raise ValueError('empty info')
        except Exception:
            # Fall back to static info from GitHub
            try:
                from static_fallback import static_fetch_info
                info = static_fetch_info(self.ticker) or {}
            except Exception:
                info = {}
        company_name = info.get('longName', self.ticker)
        sector = info.get('sector', fa_metrics.get('sector', 'Unknown'))
        industry = info.get('industry', 'Unknown')

        # Use real-time price from fast_info; fall back to last daily close
        current_price = None
        prev_close = None
        try:
            fi = stock_obj.fast_info
            current_price = safe_float(fi.last_price)
            prev_close = safe_float(fi.previous_close)
        except Exception:
            pass
        if not current_price:
            current_price = safe_float(info.get('currentPrice')) or safe_float(info.get('regularMarketPrice'))
        if not current_price:
            current_price = safe_float(df['Close'].iloc[-1]) or 0
        if not prev_close:
            prev_close = safe_float(df['Close'].iloc[-1]) if current_price != safe_float(df['Close'].iloc[-1]) \
                         else (safe_float(df['Close'].iloc[-2]) if len(df) >= 2 else current_price)
        change = current_price - (prev_close or current_price)
        change_pct = (change / prev_close * 100) if prev_close else 0
        volume = int(df['Volume'].iloc[-1])
        avg_vol = int(df['Volume'].rolling(20).mean().iloc[-1]) if len(df) >= 20 else volume

        # Earnings warning (≤14 days)
        earnings_warning = None
        try:
            cal = stock_obj.calendar
            earnings_date = None
            if isinstance(cal, dict):
                dates = cal.get('Earnings Date', [])
                if dates:
                    earnings_date = pd.to_datetime(dates[0])
            elif hasattr(cal, 'empty') and not cal.empty:
                if 'Earnings Date' in cal.index:
                    row = cal.loc['Earnings Date']
                    earnings_date = pd.to_datetime(row.iloc[0] if hasattr(row, 'iloc') else row)
            if earnings_date is not None:
                if hasattr(earnings_date, 'tzinfo') and earnings_date.tzinfo is not None:
                    earnings_date = earnings_date.tz_localize(None)
                days_to = (earnings_date - pd.Timestamp.now()).days
                if 0 <= days_to <= 14:
                    earnings_warning = {
                        'level': 'critical' if days_to <= 5 else 'warning',
                        'days': days_to,
                        'date': earnings_date.strftime('%b %d, %Y'),
                    }
        except Exception:
            pass

        # Short squeeze flag
        short_float_pct = fa_metrics.get('short_float_pct') or 0
        short_squeeze = bool(short_float_pct and short_float_pct > 0.20)

        chart_df = df.tail(180)
        timestamps = [int(ts.timestamp() * 1000) for ts in chart_df.index]

        market_status, market_time = _get_market_status()

        return {
            'ticker': self.ticker,
            'company': company_name,
            'sector': sector,
            'industry': industry,
            'current_price': round(current_price, 2),
            'change': round(change, 2),
            'change_pct': round(change_pct, 2),
            'volume': volume,
            'avg_volume': avg_vol,
            'market_status': market_status,
            'market_time': market_time,
            'scores': {
                'technical': ta_score,
                'fundamental': fa_score,
                'news': news_score,
                'social': social_score,
                'composite': composite,
                'fa_available': fa_available,
            },
            'verdict': {
                'overall': overall_v,
                'overall_color': overall_c,
                'short_term': st_v,
                'short_term_color': st_c,
                'mid_term': mt_v,
                'mid_term_color': mt_c,
                'short_direction': st_dir,
                'mid_direction': mt_dir,
                'long_direction': lt_dir,
                'long_term': lt_v,
                'long_term_color': lt_c,
                'short_squeeze': short_squeeze,
                'short_float_pct': round(short_float_pct * 100, 1) if short_float_pct else 0,
            },
            'trade_setup': {
                'short_term': short_term,
                'mid_term': mid_term,
                'long_term': long_term,
            },
            'risk': risk,
            'earnings_warning': earnings_warning,
            'fibonacci': fibonacci,
            'technical': {
                'patterns': patterns,
                'signals': [{'name': s['name'], 'value': s['value'], 'type': s['type']} for s in signals],
            },
            'fundamental': {
                'metrics': fa_metrics,
                'signals': [{'name': s['name'], 'value': s['value'], 'type': s['type']} for s in fa_signals],
            },
            'news': {
                'score': news_score,
                'articles': analyzed_news,
            },
            'social': social_data,
            'quarterly_trend': quarterly_trend,
            'support_resistance': sr_levels,
            'chart_data': {
                'timestamps': timestamps,
                'opens': safe_list(chart_df['Open']),
                'highs': safe_list(chart_df['High']),
                'lows': safe_list(chart_df['Low']),
                'closes': safe_list(chart_df['Close']),
                'volumes': [int(v) for v in chart_df['Volume']],
                'sma20': safe_list(chart_df['SMA20']),
                'sma50': safe_list(chart_df['SMA50']),
                'sma200': safe_list(chart_df['SMA200']),
                'ema9': safe_list(chart_df['EMA9']),
                'bb_upper': safe_list(chart_df['BB_Upper']),
                'bb_lower': safe_list(chart_df['BB_Lower']),
                'bb_mid': safe_list(chart_df['BB_Mid']),
                'rsi': safe_list(chart_df['RSI']),
                'macd': safe_list(chart_df['MACD']),
                'macd_signal': safe_list(chart_df['MACD_Signal']),
                'macd_hist': safe_list(chart_df['MACD_Hist']),
            },
        }
