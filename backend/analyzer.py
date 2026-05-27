import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import yfinance as yf
import pandas as pd
import numpy as np
import pytz
import time
from datetime import datetime, time as dtime

from concurrent.futures import ThreadPoolExecutor


def _yf_download_with_retry(ticker, **kwargs):
    """yfinance download with exponential backoff on rate-limit (429) errors."""
    last_err = None
    for attempt in range(3):
        try:
            df = yf.download(ticker, **kwargs)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if 'rate limit' in err_str or 'too many' in err_str or '429' in err_str:
                time.sleep(2 ** attempt * 3)   # 3s, 6s, 12s
            else:
                raise
        if attempt < 2:
            time.sleep(2 ** attempt * 3)
    if last_err:
        raise last_err
    raise ValueError(f'No data returned for {ticker} after retries')

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
            stock = yf.Ticker(self.ticker)
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

        stock_obj = yf.Ticker(self.ticker)
        info = stock_obj.info
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
