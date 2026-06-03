#!/usr/bin/env python3
"""
Pre-compute screener results for every index/sector universe and save them as
static JSON in data/screens/.  Runs in GitHub Actions (clean IPs, no Yahoo block)
twice daily, right after fetch_static_data.py.

The backend's /screen/snapshot endpoint serves these instantly — the Screen tab
loads in <1s instead of running a 60-stock live scan on Render's blocked IP.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Make backend modules importable
BACKEND = Path(__file__).resolve().parent.parent / 'backend'
sys.path.insert(0, str(BACKEND))

from screener import analyze_candidate, INDEX_UNIVERSES  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent.parent / 'data' / 'screens'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# All unique tickers across every universe (analyze each once, reuse everywhere)
ALL_TICKERS = sorted({t for tickers in INDEX_UNIVERSES.values() for t in tickers})


def main() -> int:
    print(f'Pre-computing screens for {len(ALL_TICKERS)} unique tickers '
          f'across {len(INDEX_UNIVERSES)} universes...')
    start = time.time()

    # 1️⃣ Analyze every unique ticker once (no price cap → max_price huge)
    results: dict = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(analyze_candidate, t, 1e12): t for t in ALL_TICKERS}
        for i, fut in enumerate(as_completed(futures), 1):
            t = futures[fut]
            try:
                r = fut.result(timeout=90)
            except Exception as e:
                r = None
                print(f'[{i}/{len(ALL_TICKERS)}] {t} ERROR {str(e)[:50]}')
            if r:
                results[t] = r
                print(f'[{i}/{len(ALL_TICKERS)}] {t} OK  (mid={r["scores"]["mid_term"]})')
            else:
                print(f'[{i}/{len(ALL_TICKERS)}] {t} skipped (no data)')

    # 2️⃣ Assemble per-universe files
    now_iso = datetime.now(timezone.utc).isoformat()
    universe_meta = {}
    for universe, tickers in INDEX_UNIVERSES.items():
        rows = [results[t] for t in tickers if t in results]
        rows.sort(key=lambda x: x['scores']['mid_term'], reverse=True)
        out_path = OUT_DIR / f'{universe}.json'
        with open(out_path, 'w') as f:
            json.dump({
                'universe': universe,
                'updated': now_iso,
                'count': len(rows),
                'results': rows,
            }, f, separators=(',', ':'))
        universe_meta[universe] = len(rows)
        print(f'  → {universe}: {len(rows)} stocks saved')

    # 3️⃣ Manifest
    with open(OUT_DIR / 'manifest.json', 'w') as f:
        json.dump({
            'updated': now_iso,
            'universes': universe_meta,
            'unique_tickers': len(ALL_TICKERS),
            'analyzed_ok': len(results),
            'duration_s': round(time.time() - start, 1),
        }, f, indent=2)

    dur = round(time.time() - start, 1)
    print(f'\n{"="*60}')
    print(f'Done: {len(results)}/{len(ALL_TICKERS)} analyzed in {dur}s')
    # Succeed if we got at least half the universe
    return 0 if len(results) > len(ALL_TICKERS) * 0.5 else 1


if __name__ == '__main__':
    sys.exit(main())
