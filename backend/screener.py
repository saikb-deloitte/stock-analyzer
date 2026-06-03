import yfinance as yf
import pandas as pd
import numpy as np
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from technical import compute_indicators, detect_patterns, compute_signals, score_technical
from fundamental import get_fundamentals
from news_analyzer import analyze_news

# ~70 commonly traded stocks that frequently price under $5
PENNY_UNIVERSE = [
    # AI / Tech
    'SOUN', 'BBAI', 'GFAI', 'MVIS', 'VERB', 'MULN', 'IDEX', 'DPRO',
    # EV / Clean Mobility
    'NKLA', 'GOEV', 'WKHS', 'ZEV', 'SOLO', 'AYRO', 'RIDE', 'PTRA',
    # Clean Energy
    'FCEL', 'TELL', 'AMMO',
    # Biotech / Pharma
    'MNKD', 'NVAX', 'SELB', 'ZYNE', 'IDRA', 'KPTI', 'OCGN', 'VBIV',
    'NRXP', 'INVO', 'ATNF', 'CRIS', 'AGRX', 'BFRI',
    # Cannabis
    'SNDL', 'ACB', 'CRON', 'OGI', 'HEXO', 'YCBD',
    # Retail / Consumer
    'EXPR', 'GPRO', 'BBIG', 'WISH',
    # Finance / Insurance
    'CLOV', 'UWMC', 'MFIN',
    # Space / Aerospace
    'SPCE',
    # Telecom / Media
    'NOK', 'SIRI',
    # Other liquid small-caps
    'INDO', 'MMAT', 'TTOO', 'VISL', 'LKCO', 'GREE',
    'NCTY', 'PHUN', 'ILUS', 'TRXC', 'NAKD',
]


# ── Named index universes (US equities) — scanned in full, no price cap ──────
INDEX_UNIVERSES = {
    'large_cap': [
        'AAPL','MSFT','GOOGL','AMZN','META','NVDA','BRK-B','LLY','AVGO','TSLA',
        'JPM','V','UNH','XOM','MA','PG','JNJ','HD','COST','MRK','ABBV','CVX',
        'CRM','BAC','KO','PEP','WMT','NFLX','ADBE','TMO','AMD','CSCO','ACN',
        'MCD','ABT','LIN','DHR','WFC','TXN','DIS','INTC','VZ','PM','CAT','IBM',
        'GE','QCOM','NOW','INTU','AMGN','SPGI','UBER','GS','HON','BKNG','RTX',
        'NEE','PFE','LOW','UNP','T',
    ],
    'tech': [
        'AAPL','MSFT','GOOGL','META','NVDA','AVGO','CRM','ADBE','AMD','CSCO',
        'ACN','TXN','NOW','INTU','QCOM','IBM','ORCL','INTC','MU','AMAT','LRCX',
        'PLTR','SHOP','UBER','SNOW','PANW','ANET','ADI','KLAC','SNPS','CDNS',
    ],
    'financials': [
        'JPM','V','MA','BAC','WFC','GS','MS','SPGI','BLK','AXP','C','SCHW',
        'CB','PGR','MMC','USB','PNC','TFC','COF','AON','MET','AIG','PRU','AFL','TRV',
    ],
    'healthcare': [
        'LLY','UNH','JNJ','MRK','ABBV','TMO','ABT','DHR','PFE','AMGN','BMY',
        'MDT','GILD','CVS','ISRG','VRTX','REGN','CI','ZTS','BSX','SYK','HUM','BDX','MRNA',
    ],
    'energy': [
        'XOM','CVX','COP','SLB','EOG','MPC','PSX','VLO','OXY','WMB','KMI','HES','DVN','APA',
    ],
    'consumer': [
        'AMZN','TSLA','HD','COST','WMT','PG','KO','PEP','MCD','NKE','SBUX','LOW',
        'TJX','TGT','DG','EL','MDLZ','CL','MO','PM','BBY','ROST','DLTR','KR','CHWY',
    ],
    'dividend': [
        'JNJ','PG','KO','PEP','XOM','CVX','MCD','ABBV','MRK','VZ','T','IBM','MMM',
        'CAT','HD','LOW','TXN','PM','MO','CL','GD','LMT','SO','DUK','O','NEE',
    ],
}


_YF_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def get_dynamic_universe(max_price=5.0, max_results=250):
    """
    Fetch a dynamic list of liquid sub-$max_price US equities from Yahoo Finance screener.
    Tries three predefined screeners (most_actives, small_cap_gainers, day_gainers) plus
    a custom POST screener. Falls back to None if all fail.
    """
    tickers = set()

    # 1. Predefined screeners — no auth needed
    for scr_id in ['most_actives', 'small_cap_gainers', 'day_gainers', 'undervalued_small_caps']:
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/screener/predefined/saved"
            params = {"count": 100, "offset": 0, "scrIds": scr_id}
            r = requests.get(url, params=params, headers=_YF_HEADERS, timeout=10)
            if r.status_code == 200:
                quotes = (r.json().get("finance", {})
                           .get("result", [{}])[0].get("quotes", []))
                for q in quotes:
                    price = q.get("regularMarketPrice", 999)
                    vol = q.get("regularMarketVolume") or q.get("averageDailyVolume3Month") or 0
                    sym = q.get("symbol", "")
                    if price and 0.10 < price <= max_price and vol >= 100_000 and sym:
                        tickers.add(sym)
        except Exception:
            pass

    # 2. Custom POST screener — broader price-filtered query
    try:
        url = "https://query2.finance.yahoo.com/v1/finance/screener"
        payload = {
            "offset": 0, "size": max_results,
            "sortField": "intradaymarketcap", "sortType": "DESC",
            "quoteType": "EQUITY",
            "query": {
                "operator": "AND",
                "operands": [
                    {"operator": "LT", "operands": ["regularmarketprice", max_price]},
                    {"operator": "GT", "operands": ["regularmarketprice", 0.10]},
                    {"operator": "GT", "operands": ["averagedailyvol3month", 100_000]},
                    {"operator": "IN", "operands": ["exchange", "NMS", "NYQ", "NCM", "NIM"]},
                ],
            },
            "userId": "", "userIdType": "guid",
        }
        headers = {**_YF_HEADERS, "Content-Type": "application/json"}
        r = requests.post(url, json=payload, headers=headers, timeout=15)
        if r.status_code == 200:
            quotes = (r.json().get("finance", {})
                       .get("result", [{}])[0].get("quotes", []))
            for q in quotes:
                if q.get("symbol"):
                    tickers.add(q["symbol"])
    except Exception:
        pass

    return list(tickers) if tickers else None


def batch_get_prices(tickers):
    """Single yfinance call to get latest close prices for all tickers."""
    try:
        df = yf.download(tickers, period='5d', interval='1d',
                         progress=False, auto_adjust=True, group_by='column')
        if df.empty:
            return {}

        # yfinance returns MultiIndex (Close/Open/…, Ticker) for multi-ticker downloads.
        # For a single ticker, or when only one ticker has data, it may return flat columns.
        if isinstance(df.columns, pd.MultiIndex):
            closes = df['Close']
        elif 'Close' in df.columns:
            # Flat columns — single ticker result
            if len(tickers) == 1:
                closes = pd.DataFrame({tickers[0]: df['Close']})
            else:
                # Multiple tickers requested but yfinance returned flat columns.
                # Fall back: return the one ticker whose data is present.
                closes = pd.DataFrame({tickers[0]: df['Close']})
        else:
            return {}

        result = {}
        for t in tickers:
            if t in closes.columns:
                series = closes[t].dropna()
                if not series.empty:
                    result[t] = float(series.iloc[-1])
        return result
    except Exception:
        return {}


def analyze_candidate(ticker, max_price=5.0):
    """Full analysis of a single candidate. Returns dict or None."""
    try:
        # Use the resilient multi-source cascade (Yahoo direct → Stooq → GitHub static)
        # so screener works even when Render's IP is blocked by Yahoo.
        try:
            from analyzer import _yf_download_with_retry
            df = _yf_download_with_retry(ticker, period='1y', interval='1d',
                                         progress=False, auto_adjust=True)
        except Exception:
            df = yf.download(ticker, period='1y', interval='1d',
                             progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 60:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        current_price = float(df['Close'].iloc[-1])
        if current_price <= 0 or current_price > max_price:
            return None

        avg_vol = float(df['Volume'].rolling(20).mean().iloc[-1])
        if avg_vol < 100_000:
            return None

        df = compute_indicators(df.copy())
        patterns = detect_patterns(df)
        signals = compute_signals(df)
        ta_score = score_technical(patterns, signals)

        try:
            fa_score, fa_metrics, _, fa_available = get_fundamentals(ticker)
        except Exception:
            fa_score = 0
            fa_metrics = {}
            fa_available = False

        try:
            raw_news = yf.Ticker(ticker).news[:10] or []
            news_score, _ = analyze_news(ticker, raw_news)
        except Exception:
            news_score = 0

        # Mirror risk_analyzer.py weights, renormalized to sum=1.0 since social_score
        # is skipped in the screener for speed (avoids Reddit/StockTwits API calls).
        # Original weights with social: ta0.45+fa0.30+news0.15+social0.10=1.0
        # Renormalized without social:  ta0.50+fa0.33+news0.17 = 1.00
        if fa_available:
            composite = round(ta_score * 0.50 + fa_score * 0.33 + news_score * 0.17)
            mt_score  = round(fa_score * 0.58 + ta_score * 0.32 + news_score * 0.10)
        else:
            # Without FA: ta0.65+news0.25+social0.10=1.0 → renorm: ta0.72+news0.28
            composite = round(ta_score * 0.72 + news_score * 0.28)
            mt_score  = round(ta_score * 0.72 + news_score * 0.28)

        atr = float(df['ATR'].iloc[-1]) if not pd.isna(df['ATR'].iloc[-1]) else current_price * 0.03
        n = len(df)
        high_arr = df['High'].values
        low_arr = df['Low'].values

        # Use entry_price (historical close) for all trade calculations.
        # current_price may be overwritten with real-time data later (display only).
        entry_price = current_price

        resistances = sorted([high_arr[i] for i in range(3, n - 3)
                               if high_arr[i] == max(high_arr[i-3:i+4])
                               and high_arr[i] > entry_price])
        supports = sorted([low_arr[i] for i in range(3, n - 3)
                           if low_arr[i] == min(low_arr[i-3:i+4])
                           and low_arr[i] < entry_price])

        stop_raw = entry_price - 3.0 * atr
        stop = round(max(stop_raw, entry_price * 0.01), 4)  # guard: never < 1% of price
        t1_atr = round(entry_price + 5.0 * atr, 4)
        t2_atr = round(entry_price + 8.0 * atr, 4)

        near_res = [r for r in resistances if r > entry_price * 1.02 and r < entry_price * 1.40]
        t1 = round(min(near_res[0], t1_atr), 4) if near_res else t1_atr
        t2 = round(min(near_res[1] if len(near_res) > 1 else t2_atr, t2_atr), 4)

        risk_amt = entry_price - stop
        rr1 = round((t1 - entry_price) / risk_amt, 1) if risk_amt > 0 else 0
        rr2 = round((t2 - entry_price) / risk_amt, 1) if risk_amt > 0 else 0

        # Use High/Low columns (not Close) for true 52-week range
        high_52w = float(df['High'].iloc[-min(252, n):].max())
        low_52w  = float(df['Low'].iloc[-min(252, n):].min())
        prev = float(df['Close'].iloc[-2]) if n >= 2 else current_price
        change_pct = (current_price - prev) / prev * 100 if prev > 0 else 0

        try:
            t_obj = yf.Ticker(ticker)
            info = t_obj.info
            company = info.get('longName', ticker)
            sector = info.get('sector', fa_metrics.get('sector', 'Unknown'))
            analyst_target = info.get('targetMeanPrice')
            short_float_pct = info.get('shortPercentOfFloat') or 0
            # Real-time price for display only — trade setup already computed above
            try:
                rt = t_obj.fast_info.last_price
                if rt and rt > 0:
                    current_price = float(rt)
            except Exception:
                rt_price = info.get('currentPrice') or info.get('regularMarketPrice')
                if rt_price and rt_price > 0:
                    current_price = float(rt_price)
        except Exception:
            # Yahoo info blocked — fall back to static GitHub info
            company = ticker
            sector = fa_metrics.get('sector', 'Unknown')
            analyst_target = None
            short_float_pct = 0
            try:
                from static_fallback import static_fetch_info
                sinfo = static_fetch_info(ticker)
                if sinfo:
                    company = sinfo.get('longName') or sinfo.get('shortName') or ticker
                    sector = sinfo.get('sector', sector)
                    analyst_target = sinfo.get('targetMeanPrice')
            except Exception:
                pass

        def v(col):
            val = df[col].iloc[-1]
            return None if pd.isna(val) else float(val)

        rsi = v('RSI')
        sma20 = v('SMA20')
        sma50 = v('SMA50')
        macd = v('MACD')
        macd_sig = v('MACD_Signal')

        bullish_patterns = [p['name'] for p in patterns if p['type'] == 'bullish']

        return {
            'ticker': ticker,
            'company': company,
            'sector': sector,
            'current_price': round(current_price, 4),
            'change_pct': round(change_pct, 2),
            'avg_volume': int(avg_vol),
            'scores': {
                'technical': ta_score,
                'fundamental': fa_score,
                'news': news_score,
                'composite': composite,
                'mid_term': mt_score,
            },
            'trade': {
                'entry': round(entry_price, 4),
                'stop_loss': round(stop, 4),
                'stop_pct': round((entry_price - stop) / entry_price * 100, 1),
                'target_1': t1,
                'target_1_pct': round((t1 - entry_price) / entry_price * 100, 1),
                'target_2': t2,
                'target_2_pct': round((t2 - entry_price) / entry_price * 100, 1),
                'risk_reward_t1': rr1,
                'risk_reward_t2': rr2,
            },
            'indicators': {
                'rsi': round(rsi, 1) if rsi is not None else None,
                'above_sma20': (sma20 is not None and current_price > sma20),
                'above_sma50': (sma50 is not None and current_price > sma50),
                'macd_bullish': (macd is not None and macd_sig is not None and macd > macd_sig),
                'atr': round(atr, 4),
                'atr_pct': round(atr / current_price * 100, 2),
            },
            'fundamentals': {
                'pe_ratio': fa_metrics.get('pe_ratio'),
                'revenue_growth': fa_metrics.get('revenue_growth'),
                'profit_margin': fa_metrics.get('profit_margin'),
                'analyst_target': round(analyst_target, 4) if analyst_target else None,
                'analyst_consensus': fa_metrics.get('analyst_consensus'),
            },
            'patterns': bullish_patterns,
            '52w_high': round(high_52w, 4),
            '52w_low': round(low_52w, 4),
            'short_squeeze': short_float_pct > 0.20,
            'short_float_pct': round(short_float_pct * 100, 1),
        }
    except Exception:
        return None


def run_screener(extra_tickers=None, max_price=5.0, min_score=35, workers=6, universe='penny'):
    """Generator — yields SSE-ready JSON strings for progress and results."""

    # ── Index universe mode: scan a fixed list in full, no price cap, show all ──
    if universe and universe != 'penny' and universe in INDEX_UNIVERSES:
        tickers = list(dict.fromkeys(INDEX_UNIVERSES[universe] + (extra_tickers or [])))
        label = universe.replace('_', ' ').title()
        yield _sse({'type': 'status',
                    'message': f'Scanning {len(tickers)} {label} stocks…',
                    'total': len(tickers)})

        done_count = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # huge max_price → no price rejection for index scans
            futures = {executor.submit(analyze_candidate, t, 1e12): t for t in tickers}
            for future in as_completed(futures):
                done_count += 1
                ticker = futures[future]
                result = future.result()
                yield _sse({'type': 'progress', 'done': done_count,
                            'total': len(tickers), 'ticker': ticker})
                # Index mode: emit ALL analyzed stocks (Centaur-style table); user sorts
                if result:
                    yield _sse({'type': 'result', 'data': result})
        yield _sse({'type': 'done', 'count': done_count})
        return

    # ── Penny-stock mode (original behaviour) ──
    yield _sse({'type': 'status', 'message': 'Building dynamic ticker universe…', 'total': 0})

    dynamic = get_dynamic_universe(max_price=max_price)
    if dynamic:
        base = list(dict.fromkeys(dynamic + PENNY_UNIVERSE))
        source_label = f'Dynamic ({len(dynamic)} found) + {len(PENNY_UNIVERSE)} curated'
    else:
        base = PENNY_UNIVERSE
        source_label = f'{len(PENNY_UNIVERSE)} curated (live screener unavailable)'

    universe = list(dict.fromkeys(base + (extra_tickers or [])))

    yield _sse({'type': 'status', 'message': f'Fetching prices for {len(universe)} stocks ({source_label})…', 'total': len(universe)})

    prices = batch_get_prices(universe)
    candidates = [(t, p) for t, p in prices.items() if 0 < p <= max_price]
    candidates.sort(key=lambda x: x[0])

    yield _sse({'type': 'status',
                'message': f'Found {len(candidates)} stocks under ${max_price}. Running deep analysis…',
                'candidates': len(candidates)})

    if not candidates:
        yield _sse({'type': 'done', 'count': 0})
        return

    tickers_to_scan = [t for t, _ in candidates]
    done_count = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(analyze_candidate, t, max_price): t for t in tickers_to_scan}
        for future in as_completed(futures):
            done_count += 1
            ticker = futures[future]
            result = future.result()

            yield _sse({'type': 'progress', 'done': done_count,
                        'total': len(tickers_to_scan), 'ticker': ticker})

            if result and result['scores']['mid_term'] >= min_score:
                yield _sse({'type': 'result', 'data': result})

    yield _sse({'type': 'done', 'count': done_count})


def _sse(obj):
    return f"data: {json.dumps(obj)}\n\n"
