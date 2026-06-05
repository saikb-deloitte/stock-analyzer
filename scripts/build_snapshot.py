"""
Centaur Prism — daily static data snapshot builder.

Runs the screener + long-term-picks logic offline (typically via GitHub Actions
runners, which aren't rate-limited by Yahoo). Writes JSON files to
static/data/ for use as a stable data source independent of the live API.

Usage:
    python scripts/build_snapshot.py              # default: NIFTY 50 only
    python scripts/build_snapshot.py --all        # all common indices
    python scripts/build_snapshot.py --indices "NIFTY 50,NIFTY Bank,NIFTY IT"

These JSON files are reachable in production at:
    /static/data/snapshot_<index>.json

Or via GitHub directly:
    https://raw.githubusercontent.com/<user>/<repo>/<branch>/static/data/snapshot_<index>.json

The live Render/Fly app is NOT modified by this script — it's a separate,
parallel data path.
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Add project root so we can import app.py
PROJ_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJ_ROOT))

# Force local mode — avoid rate limits + session-scoped archive
os.environ.pop('PUBLIC_MODE', None)

# Now import the app (this loads everything including the screener logic)
print('Loading Centaur Prism app modules...')
from app import app, _screener_impl, _long_term_picks_impl, _intraday_signals_impl

OUT_DIR = PROJ_ROOT / 'static' / 'data'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def slug(name):
    return name.lower().replace(' ', '_').replace('/', '_')


def build_screener_snapshot(index_name):
    print(f'\n=== Building screener snapshot for "{index_name}" ===')
    t0 = time.time()
    with app.test_request_context(f'/api/screener?index={index_name}'):
        resp = _screener_impl()
        try:
            data = resp.get_json()
        except Exception as e:
            print(f'  ERROR: could not parse screener response: {e}')
            return False

    if not isinstance(data, list):
        print(f'  ERROR: screener returned {type(data).__name__} (not a list): {str(data)[:200]}')
        return False

    if not data:
        print(f'  WARN: empty screener for {index_name}')
        return False

    payload = {
        'generated_at':      int(time.time()),
        'generated_at_iso':  datetime.now(timezone.utc).isoformat(),
        'index':             index_name,
        'stock_count':       len(data),
        'source':            'build_snapshot.py',
        'app_version':       'centaur-prism',
        'stocks':            data,
    }

    out_path = OUT_DIR / f'snapshot_{slug(index_name)}.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1024
    elapsed = time.time() - t0
    print(f'  OK -> {out_path.name} | {len(data)} stocks | {size_kb:.1f} KB | {elapsed:.1f}s')
    return True


def build_picks_snapshot(index_filter='all'):
    print(f'\n=== Building long-term picks snapshot ({index_filter}) ===')
    t0 = time.time()
    with app.test_request_context(f'/api/long_term_picks?index={index_filter}'):
        resp = _long_term_picks_impl()
        try:
            data = resp.get_json()
        except Exception as e:
            print(f'  ERROR: could not parse picks response: {e}')
            return False

    if not isinstance(data, dict) or 'total_picks' not in data:
        print(f'  ERROR: picks returned unexpected shape: {str(data)[:200]}')
        return False

    payload = {
        'generated_at':      int(time.time()),
        'generated_at_iso':  datetime.now(timezone.utc).isoformat(),
        'index':             index_filter,
        'total_picks':       data.get('total_picks', 0),
        'total_scanned':     data.get('total_scanned', 0),
        'source':            'build_snapshot.py',
        'data':              data,
    }

    out_path = OUT_DIR / f'snapshot_picks_{slug(index_filter)}.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1024
    elapsed = time.time() - t0
    print(f'  OK -> {out_path.name} | {data.get("total_picks", 0)} picks | {size_kb:.1f} KB | {elapsed:.1f}s')
    return True


def build_intraday_snapshot():
    """Pre-computes the intraday signals payload (pivots, index bias, setups)
    so the live endpoint can fall back to a static file when yfinance is
    blocked on cloud IPs (the common Fly/Render case)."""
    print(f'\n=== Building intraday signals snapshot ===')
    t0 = time.time()
    with app.test_request_context('/api/intraday_signals'):
        resp = _intraday_signals_impl()
        try:
            data = resp.get_json()
        except Exception as e:
            print(f'  ERROR: could not parse intraday response: {e}')
            return False

    if not isinstance(data, dict) or 'setups' not in data:
        print(f'  ERROR: intraday returned unexpected shape: {str(data)[:200]}')
        return False

    payload = {
        'generated_at':      int(time.time()),
        'generated_at_iso':  datetime.now(timezone.utc).isoformat(),
        'source':            'build_snapshot.py',
        'data':              data,
    }
    out_path = OUT_DIR / 'snapshot_intraday.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)

    size_kb = out_path.stat().st_size / 1024
    elapsed = time.time() - t0
    print(f'  OK -> {out_path.name} | {len(data.get("setups",[]))} setups | {size_kb:.1f} KB | {elapsed:.1f}s')
    return True


def write_index_manifest(snapshots):
    """Single file listing every snapshot built — handy for the frontend."""
    manifest = {
        'generated_at':     int(time.time()),
        'generated_at_iso': datetime.now(timezone.utc).isoformat(),
        'snapshots':        snapshots,
    }
    out_path = OUT_DIR / 'manifest.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    print(f'\n=== Manifest written ===\n  {out_path.name}: {len(snapshots)} entries')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--all',     action='store_true', help='Build snapshots for all common indices')
    parser.add_argument('--indices', type=str, default='NIFTY 50', help='Comma-separated list')
    parser.add_argument('--picks',   action='store_true', default=True, help='Also build picks snapshot')
    args = parser.parse_args()

    if args.all:
        indices = [
            'NIFTY 50',
            'NIFTY Bank',
            'NIFTY IT',
            'NIFTY Pharma',
            'NIFTY Auto',
            'NIFTY Midcap',
            # ALL NSE = bhavcopy-derived top ~300 liquid stocks. Heaviest
            # build (~5-8 min via GitHub Actions runner where yfinance is
            # unblocked). Put last so a failure here doesn't kill the
            # index-specific snapshots.
            'ALL NSE',
        ]
    else:
        indices = [s.strip() for s in args.indices.split(',') if s.strip()]

    print(f'Centaur Prism Snapshot Builder')
    print(f'  Target indices: {indices}')
    print(f'  Output dir:     {OUT_DIR}')

    built = []
    for idx in indices:
        try:
            if build_screener_snapshot(idx):
                built.append({
                    'kind':  'screener',
                    'index': idx,
                    'file':  f'snapshot_{slug(idx)}.json',
                })
        except Exception as e:
            print(f'  FAIL "{idx}": {e}')

    if args.picks:
        try:
            if build_picks_snapshot('all'):
                built.append({
                    'kind':  'picks',
                    'index': 'all',
                    'file':  'snapshot_picks_all.json',
                })
        except Exception as e:
            print(f'  FAIL picks: {e}')

    # Intraday snapshot — pre-computes pivots + setups so the live endpoint
    # has a fallback when yfinance is blocked on cloud IPs.
    try:
        if build_intraday_snapshot():
            built.append({
                'kind':  'intraday',
                'index': 'all',
                'file':  'snapshot_intraday.json',
            })
    except Exception as e:
        print(f'  FAIL intraday: {e}')

    write_index_manifest(built)
    print(f'\nDone. {len(built)} snapshots built.')
    if not built:
        sys.exit(1)


if __name__ == '__main__':
    main()
