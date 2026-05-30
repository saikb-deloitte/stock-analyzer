import sys
import os
import time
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn
from typing import List, Optional

from analyzer import StockAnalyzer
from screener import run_screener

app = FastAPI(title='SK Stock Analyzer API', version='1.0')

# ── In-memory TTL cache (aggressive — personal-use, not real-time trading) ───
_CACHE: dict = {}
_CACHE_TTL_MARKET_OPEN = 1800   # 30 min during market hours
_CACHE_TTL_MARKET_CLOSED = 14400 # 4 hours after hours (data is nearly static)
_STALE_FALLBACK_TTL = 86400 * 3  # serve up to 3-day-old data on Yahoo errors

def _current_ttl():
    """Returns cache TTL based on US market hours (rough NYSE check)."""
    try:
        import pytz
        from datetime import datetime, time as dtime
        now = datetime.now(pytz.timezone('America/New_York'))
        if now.weekday() >= 5:
            return _CACHE_TTL_MARKET_CLOSED
        t = now.time()
        if dtime(9, 30) <= t <= dtime(16, 0):
            return _CACHE_TTL_MARKET_OPEN
        return _CACHE_TTL_MARKET_CLOSED
    except Exception:
        return _CACHE_TTL_MARKET_OPEN

def _get_cached(key: str):
    entry = _CACHE.get(key)
    if entry and (time.time() - entry['ts']) < _current_ttl():
        return entry['data']
    return None

def _set_cached(key: str, data):
    _CACHE[key] = {'ts': time.time(), 'data': data}

def _cache_age(key: str):
    entry = _CACHE.get(key)
    return round(time.time() - entry['ts']) if entry else None

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.get('/analyze/{ticker}')
def analyze(ticker: str, refresh: bool = Query(default=False)):
    ticker = ticker.upper().strip()
    if not refresh:
        cached = _get_cached(ticker)
        if cached:
            cached['_cached'] = True
            cached['_cache_age_s'] = _cache_age(ticker)
            return cached
    try:
        result = StockAnalyzer(ticker).analyze()
        result['_cached'] = False
        _set_cached(ticker, result)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        err_str = str(e).lower()
        is_rate_limit = ('too many' in err_str or 'rate limit' in err_str or '429' in err_str)
        is_no_data    = ('no price data' in err_str or 'no data' in err_str)

        # Return stale cache (up to 3 days old) as last-resort on Yahoo errors
        if is_rate_limit or is_no_data:
            stale = _CACHE.get(ticker)
            if stale and (time.time() - stale['ts']) < _STALE_FALLBACK_TTL:
                d = stale['data']
                d['_cached'] = True
                d['_cache_age_s'] = round(time.time() - stale['ts'])
                d['_stale'] = True
                return d
            if is_rate_limit:
                raise HTTPException(
                    status_code=429,
                    detail='Yahoo Finance rate-limited this server. Try again in 60 seconds.',
                    headers={'Retry-After': '60'},
                )
        raise HTTPException(status_code=500, detail=f'Analysis error: {str(e)}')


@app.get('/screen/stream')
def screen_stream(
    max_price: float = Query(default=5.0, ge=0.01, le=50.0),
    min_score: int = Query(default=35, ge=0, le=100),
    extra: Optional[str] = Query(default=None, description='Comma-separated extra tickers'),
):
    extra_tickers = [t.strip().upper() for t in extra.split(',')] if extra else []

    def generate():
        for chunk in run_screener(
            extra_tickers=extra_tickers,
            max_price=max_price,
            min_score=min_score,
        ):
            yield chunk

    return StreamingResponse(
        generate(),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
        },
    )


@app.get('/chart/{ticker}')
def chart(
    ticker: str,
    interval: str = Query(default='1d', regex='^(15m|60m|1d|1wk)$'),
    period: str = Query(default='6mo', regex='^(5d|1mo|3mo|6mo|1y|2y)$'),
):
    """Return OHLCV + indicators for any timeframe — used by the frontend chart toggle."""
    import yfinance as yf
    import pandas as pd
    import numpy as np

    def _safe(v):
        try:
            f = float(v)
            return None if (np.isnan(f) or np.isinf(f)) else f
        except Exception:
            return None

    def _sl(series):
        return [_safe(v) for v in series]

    try:
        ticker = ticker.upper()
        df = yf.download(ticker, period=period, interval=interval,
                         progress=False, auto_adjust=True)
        if df.empty:
            raise HTTPException(status_code=404, detail=f'No data for {ticker}')
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close = df['Close']
        df['SMA20'] = close.rolling(20).mean()
        df['SMA50'] = close.rolling(50).mean()
        df['EMA9'] = close.ewm(span=9, adjust=False).mean()
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        df['BB_Upper'] = bb_mid + 2 * bb_std
        df['BB_Lower'] = bb_mid - 2 * bb_std
        df['BB_Mid'] = bb_mid

        # RSI (14)
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))

        # MACD (12, 26, 9)
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        df['MACD'] = ema12 - ema26
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

        timestamps = [int(ts.timestamp() * 1000) for ts in df.index]
        return {
            'ticker': ticker, 'interval': interval, 'period': period,
            'timestamps': timestamps,
            'opens':      _sl(df['Open']),
            'highs':      _sl(df['High']),
            'lows':       _sl(df['Low']),
            'closes':     _sl(df['Close']),
            'volumes':    [int(v) for v in df['Volume']],
            'sma20':      _sl(df['SMA20']),
            'sma50':      _sl(df['SMA50']),
            'ema9':       _sl(df['EMA9']),
            'bb_upper':   _sl(df['BB_Upper']),
            'bb_lower':   _sl(df['BB_Lower']),
            'bb_mid':     _sl(df['BB_Mid']),
            'rsi':        _sl(df['RSI']),
            'macd':       _sl(df['MACD']),
            'macd_signal': _sl(df['MACD_Signal']),
            'macd_hist':  _sl(df['MACD_Hist']),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/health')
def health():
    return {'status': 'ok'}


@app.get('/diag')
def diag():
    """Diagnostic endpoint — checks data-source health."""
    out = {'cache_size': len(_CACHE), 'cache_ttl_now_s': _current_ttl()}
    # Check curl_cffi
    try:
        from curl_cffi import requests as curl_requests
        out['curl_cffi'] = 'installed'
    except Exception as e:
        out['curl_cffi'] = f'NOT installed: {e}'
    # Check Stooq
    try:
        from analyzer import _stooq_download, _STOOQ_LAST_ERROR
        df = _stooq_download('AAPL', period_days=30)
        from analyzer import _STOOQ_LAST_ERROR as _err_after
        out['stooq'] = 'OK' if (df is not None and not df.empty) else 'returned empty'
        out['stooq_last_error'] = _err_after
        if df is not None and not df.empty:
            out['stooq_rows'] = len(df)
            out['stooq_last_close'] = float(df['Close'].iloc[-1])
    except Exception as e:
        out['stooq'] = f'ERROR: {type(e).__name__}: {e}'
    # Test direct Yahoo chart API via curl_cffi
    try:
        from analyzer import _yahoo_chart_direct, _DIRECT_YAHOO_LAST_ERROR
        df = _yahoo_chart_direct('AAPL', period='1mo')
        from analyzer import _DIRECT_YAHOO_LAST_ERROR as _err_yh
        out['yahoo_direct'] = 'OK' if (df is not None and not df.empty) else 'returned empty'
        out['yahoo_direct_error'] = _err_yh
        if df is not None and not df.empty:
            out['yahoo_direct_rows'] = len(df)
    except Exception as e:
        out['yahoo_direct'] = f'ERROR: {type(e).__name__}: {str(e)[:120]}'

    # Test Twelve Data (if API key set)
    try:
        from analyzer import _twelve_data_download
        df = _twelve_data_download('AAPL', period_days=30)
        from analyzer import _TWELVE_DATA_LAST_ERROR as _err_td
        out['twelve_data'] = 'OK' if (df is not None and not df.empty) else 'returned empty / no key'
        out['twelve_data_error'] = _err_td
        if df is not None and not df.empty:
            out['twelve_data_rows'] = len(df)
    except Exception as e:
        out['twelve_data'] = f'ERROR: {type(e).__name__}: {str(e)[:120]}'

    # Test yfinance library fallback
    try:
        import yfinance as yf
        from analyzer import _get_yf_session
        session = _get_yf_session()
        kwargs = {'period': '5d', 'interval': '1d', 'progress': False, 'auto_adjust': True}
        if session:
            kwargs['session'] = session
        df = yf.download('AAPL', **kwargs)
        out['yfinance_lib'] = 'OK' if (df is not None and not df.empty) else 'returned empty'
        if df is not None and not df.empty:
            out['yfinance_rows'] = len(df)
    except Exception as e:
        out['yfinance_lib'] = f'ERROR: {type(e).__name__}: {str(e)[:120]}'

    return out


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8001, reload=False)
