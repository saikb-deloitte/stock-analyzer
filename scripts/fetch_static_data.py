#!/usr/bin/env python3
"""
Fetch OHLCV + fundamentals for the entire app universe and save as static
JSON files in data/.  Runs in GitHub Actions twice daily (9:30 AM and
5:30 PM ET on weekdays) — GitHub's IPs aren't blocked by Yahoo.

Backend reads these static files when live data sources fail.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yfinance as yf
import pandas as pd


# ── Universe: combined from Gems + Penny + Indices ───────────────────────────
INDICES = ['SPY', 'QQQ', 'DIA', 'IWM', 'GLD', 'TLT', 'BTC-USD']

GEMS_UNIVERSE = [
    'AAPL','MSFT','GOOGL','META','NVDA','AMD','INTC','CSCO','ORCL','IBM','ADBE','CRM',
    'NOW','TXN','QCOM','AVGO','MU','AMAT','LRCX','ASML','TSM','UBER','LYFT','SHOP',
    'PYPL','SQ','HOOD','SOFI','PLTR','SNAP','PINS','SPOT','NFLX',
    'JPM','BAC','WFC','C','GS','MS','BLK','SCHW','AXP','V','MA','BRK-B','USB','PNC',
    'TFC','COF','DFS','SYF','ALL','TRV','CB','AIG','MET','PRU','AFL','HIG',
    'JNJ','PFE','MRK','ABBV','LLY','BMY','TMO','DHR','UNH','CVS','CI','HUM','GILD',
    'AMGN','REGN','VRTX','BIIB','MRNA','ZTS','MDT','SYK','BSX','BDX','ABT','ISRG',
    'WMT','COST','TGT','HD','LOW','MCD','SBUX','NKE','TJX','ROST','DG','DLTR','BBY',
    'KR','SYY','CL','PG','KO','PEP','MDLZ','MO','PM','EL','CHWY','ETSY','EBAY',
    'XOM','CVX','COP','EOG','SLB','OXY','MPC','PSX','VLO','PXD','DVN','HES','APA',
    'BA','LMT','RTX','GE','HON','CAT','DE','UPS','FDX','UNP','CSX','NSC','EMR','ETN',
    'PH','ITW','MMM','GD','NOC','LHX','GWW','FAST',
    'DIS','VZ','T','TMUS','CMCSA','CHTR','WBD','PARA','EA','TTWO',
    'LIN','APD','FCX','NEM','SHW','DD','NUE','STLD','SO','DUK','NEE','D','AEP','SRE',
    'AMT','CCI','EQIX','PSA','SPG','O','VICI','WELL','DLR','PLD',
]

PENNY_UNIVERSE = [
    'SOUN','BBAI','GFAI','MVIS','VERB','MULN','IDEX','DPRO',
    'NKLA','GOEV','WKHS','ZEV','SOLO','AYRO','RIDE','PTRA',
    'FCEL','TELL','AMMO',
    'MNKD','NVAX','SELB','ZYNE','IDRA','KPTI','OCGN','VBIV',
    'NRXP','INVO','ATNF','CRIS','AGRX','BFRI',
    'SNDL','ACB','CRON','OGI','HEXO','YCBD',
    'EXPR','GPRO','BBIG','WISH',
    'CLOV','UWMC','MFIN',
    'SPCE','NOK','SIRI',
    'INDO','MMAT','TTOO','VISL','LKCO','GREE',
    'NCTY','PHUN','ILUS','TRXC','NAKD',
]

UNIVERSE = sorted(set(GEMS_UNIVERSE + PENNY_UNIVERSE + INDICES))

# Output paths
DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
OHLCV_DIR = DATA_DIR / 'ohlcv'
DATA_DIR.mkdir(parents=True, exist_ok=True)
OHLCV_DIR.mkdir(parents=True, exist_ok=True)


def fetch_one(ticker: str) -> dict | None:
    """Fetch OHLCV (1y daily) + info dict for one ticker."""
    try:
        t = yf.Ticker(ticker)
        df = t.history(period='1y', interval='1d', auto_adjust=True)
        if df.empty or len(df) < 30:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        ohlcv = {
            'ticker': ticker,
            'updated': datetime.now(timezone.utc).isoformat(),
            'timestamps': [int(ts.timestamp()) for ts in df.index],
            'open':   [round(float(x), 4) if not pd.isna(x) else None for x in df['Open']],
            'high':   [round(float(x), 4) if not pd.isna(x) else None for x in df['High']],
            'low':    [round(float(x), 4) if not pd.isna(x) else None for x in df['Low']],
            'close':  [round(float(x), 4) if not pd.isna(x) else None for x in df['Close']],
            'volume': [int(x) if not pd.isna(x) else 0 for x in df['Volume']],
        }

        # Save OHLCV per-ticker
        out_path = OHLCV_DIR / f'{ticker.replace("-", "_")}.json'
        with open(out_path, 'w') as f:
            json.dump(ohlcv, f, separators=(',', ':'))

        # Info — pick only the fields we use
        try:
            info = t.info or {}
        except Exception:
            info = {}

        info_subset = {k: info.get(k) for k in [
            'longName', 'shortName', 'sector', 'industry', 'marketCap',
            'trailingPE', 'forwardPE', 'priceToBook', 'priceToSalesTrailing12Months', 'pegRatio',
            'profitMargins', 'operatingMargins', 'grossMargins',
            'returnOnEquity', 'returnOnAssets',
            'revenueGrowth', 'earningsGrowth', 'earningsQuarterlyGrowth',
            'dividendYield', 'payoutRatio',
            'debtToEquity', 'currentRatio', 'quickRatio',
            'beta', 'sharesOutstanding', 'floatShares', 'shortPercentOfFloat',
            'targetMeanPrice', 'recommendationKey', 'numberOfAnalystOpinions',
            'currentPrice', 'regularMarketPrice', 'regularMarketPreviousClose',
            'fiftyTwoWeekHigh', 'fiftyTwoWeekLow',
            'fullTimeEmployees', 'website',
        ]}
        # Drop None values to save space
        info_subset = {k: v for k, v in info_subset.items() if v is not None}

        return {'ticker': ticker, 'info': info_subset, 'success': True}
    except Exception as e:
        return {'ticker': ticker, 'error': str(e)[:200], 'success': False}


def main() -> int:
    print(f'Fetching data for {len(UNIVERSE)} tickers...')
    start = time.time()

    info_all: dict = {}
    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    # Parallel fetch with bounded concurrency (Yahoo dislikes too many parallel)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_one, t): t for t in UNIVERSE}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                result = fut.result(timeout=60)
            except Exception as e:
                result = {'ticker': t, 'error': f'timeout/{type(e).__name__}: {e}', 'success': False}
            if result and result.get('success'):
                info_all[t] = result['info']
                successes.append(t)
                print(f'[{i}/{len(UNIVERSE)}] {t} ✓')
            else:
                failures.append((t, result.get('error', 'unknown') if result else 'no result'))
                print(f'[{i}/{len(UNIVERSE)}] {t} ✗  {result.get("error","")[:60] if result else ""}')

    # Write consolidated info
    info_path = DATA_DIR / 'info.json'
    with open(info_path, 'w') as f:
        json.dump({
            'updated': datetime.now(timezone.utc).isoformat(),
            'count': len(info_all),
            'tickers': info_all,
        }, f, separators=(',', ':'))

    # Manifest
    manifest_path = DATA_DIR / 'manifest.json'
    with open(manifest_path, 'w') as f:
        json.dump({
            'updated': datetime.now(timezone.utc).isoformat(),
            'universe_size': len(UNIVERSE),
            'success_count': len(successes),
            'failure_count': len(failures),
            'tickers': sorted(successes),
            'failures': dict(failures[:20]),  # first 20 failure reasons
            'duration_s': round(time.time() - start, 1),
        }, f, indent=2)

    print(f'\n{"="*60}')
    print(f'Done: {len(successes)}/{len(UNIVERSE)} succeeded in {round(time.time()-start,1)}s')
    if failures:
        print(f'Failed: {[t for t,_ in failures[:10]]}{"..." if len(failures)>10 else ""}')
    return 0 if len(successes) > len(UNIVERSE) * 0.5 else 1  # need >50% success


if __name__ == '__main__':
    sys.exit(main())
