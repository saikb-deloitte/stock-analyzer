"""
Centaur Prism — Intraday setup backtest engine.

Runs each of the 5 intraday setup types over 2 years of historical NSE daily
data per F&O stock, then computes REAL win rates / avg R / drawdown /
profit factor per setup type. This replaces my heuristic conviction scores
with measured statistics.

Trade simulation:
  - Setup fires on day D (close)
  - Entry on day D+1 open IF price opens within entry zone, else SKIP
  - Track day D+1 intraday hi/lo:
      LONG : if low <= stop → STOPPED at stop
             elif high >= t2 → T2 hit
             elif high >= t1 → T1 hit (50% off, BE on rest)
             else            → close at day's close (mark-to-market)
      SHORT: mirrored
  - Cost: 0.05% slippage + ₹20 brokerage per round-trip
  - Position size = 1% of capital risked per trade (Kelly assumed = full)

Output: scripts/../static/data/snapshot_setup_backtest.json — a single
file with per-setup stats over the past 24 months. Frontend reads this
to show "actual" win rates next to my conviction scores.

Usage:
    python scripts/backtest_setups.py                 # default: top 50 F&O
    python scripts/backtest_setups.py --tickers 100   # broader scan
"""
import os, sys, json, time, argparse, math
from pathlib import Path
from datetime import datetime, timezone, timedelta

PROJ_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJ_ROOT))
os.environ.pop('PUBLIC_MODE', None)

print('Loading Centaur Prism app...')
from app import (
    app, fetch_history, calculate_rsi, calculate_atr,
    _build_intraday_setup, calculate_mtf_alignment, NSE_FNO_STOCKS,
)

OUT_DIR = PROJ_ROOT / 'static' / 'data'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Costs (realistic round-trip for intraday in NSE)
SLIPPAGE_PCT  = 0.05    # 0.05% per leg
BROKERAGE_RS  = 20      # flat ₹20 per round-trip (Zerodha-like)

# Position rules
RISK_PCT_PER_TRADE = 1.0    # 1% capital at risk
CAPITAL = 100_000           # notional capital for backtest

# Trade simulation parameters
LOOKBACK_DAYS = 504          # ~24 trading months
WARMUP_DAYS   = 60           # need history for indicators on day 1


def _simulate_trade(setup, intraday_high, intraday_low, intraday_close):
    """Simulate a single intraday trade given the setup + next-day's bar.
    Returns dict: {outcome, r_multiple, gross_pnl_pct, net_pnl_pct}"""
    side = setup['side']
    entry_lo, entry_hi = setup['entry_zone']
    stop = setup['stop']
    t1   = setup['target1']
    t2   = setup['target2']

    # Did price OPEN inside (or pierce through) the entry zone?
    # We assume entry at midpoint of zone (worst-case avg slippage).
    entry_price = (entry_lo + entry_hi) / 2

    # For LONG: must be able to enter (low <= entry_hi). If low > entry_hi
    # the stock gapped above our entry — skip (chasing breakouts loses).
    if side == 'LONG':
        if intraday_low > entry_hi:
            return {'outcome': 'SKIPPED_GAP_UP', 'r_multiple': 0, 'gross_pnl_pct': 0, 'net_pnl_pct': 0}
        # Hit logic: gap-down through stop = stopped at open low (worst case)
        if intraday_low <= stop:
            # Did it then bounce to T1/T2 before close? Be pessimistic: stopped.
            exit_price = stop
            r_mult = -1.0
            outcome = 'STOPPED'
        elif intraday_high >= t2:
            exit_price = t2
            r_mult = (t2 - entry_price) / (entry_price - stop)
            outcome = 'T2_HIT'
        elif intraday_high >= t1:
            # Hit T1 but not T2 — exit at T1
            exit_price = t1
            r_mult = (t1 - entry_price) / (entry_price - stop)
            outcome = 'T1_HIT'
        else:
            # MTM at close
            exit_price = intraday_close
            r_mult = (exit_price - entry_price) / (entry_price - stop) if (entry_price - stop) > 0 else 0
            outcome = 'CLOSED_MTM'
        gross = (exit_price - entry_price) / entry_price * 100
    else:  # SHORT
        if intraday_high < entry_lo:
            return {'outcome': 'SKIPPED_GAP_DOWN', 'r_multiple': 0, 'gross_pnl_pct': 0, 'net_pnl_pct': 0}
        if intraday_high >= stop:
            exit_price = stop
            r_mult = -1.0
            outcome = 'STOPPED'
        elif intraday_low <= t2:
            exit_price = t2
            r_mult = (entry_price - t2) / (stop - entry_price)
            outcome = 'T2_HIT'
        elif intraday_low <= t1:
            exit_price = t1
            r_mult = (entry_price - t1) / (stop - entry_price)
            outcome = 'T1_HIT'
        else:
            exit_price = intraday_close
            r_mult = (entry_price - exit_price) / (stop - entry_price) if (stop - entry_price) > 0 else 0
            outcome = 'CLOSED_MTM'
        gross = (entry_price - exit_price) / entry_price * 100

    # Net of costs (slippage both legs + brokerage as % of position)
    pos_size = CAPITAL * (RISK_PCT_PER_TRADE / 100) / abs(entry_price - stop) * entry_price
    brokerage_pct = (BROKERAGE_RS / pos_size * 100) if pos_size > 0 else 0
    net = gross - (SLIPPAGE_PCT * 2) - brokerage_pct
    return {
        'outcome':       outcome,
        'r_multiple':    round(r_mult, 3),
        'gross_pnl_pct': round(gross, 3),
        'net_pnl_pct':   round(net, 3),
    }


def backtest_one_stock(ticker, lookback=LOOKBACK_DAYS):
    """Walk 2-year history, identify every setup that fired, simulate result."""
    df, _src = fetch_history(ticker + '.NS', period='2y')
    if df is None or df.empty or len(df) < WARMUP_DAYS + 30:
        return None
    df = df.dropna(subset=['Close', 'High', 'Low'])
    if len(df) < WARMUP_DAYS + 30:
        return None

    trades = []
    # Walk from WARMUP_DAYS to end-1 (need at least 1 future bar to simulate)
    for i in range(WARMUP_DAYS, len(df) - 1):
        # Slice "as if" we're at day i — build setup, then check day i+1's bar
        df_slice = df.iloc[:i + 1]
        # Synthesize a minimal screener-style row (we don't have sector score here)
        row = {'ticker': ticker, 'name': ticker, 'sector': ''}
        try:
            # Compute MTF alignment from the historical slice (matches what
            # live code does). 5m data isn't available for old dates so we
            # pass None — new 5m setups (VWAP/ORB/GapFade) won't fire in
            # backtest, but daily-bar setups (Pivot Bounce, BB Squeeze) will.
            mtf = None
            try:
                mtf = calculate_mtf_alignment(df_slice)
            except Exception:
                pass
            setup = _build_intraday_setup(row, df_slice, sector_strength=None,
                                          regime=None, mtf=mtf, df_5m=None)
        except Exception:
            continue
        if setup is None:
            continue

        # Day i+1's intraday range
        next_bar = df.iloc[i + 1]
        try:
            hi = float(next_bar['High'])
            lo = float(next_bar['Low'])
            cl = float(next_bar['Close'])
        except Exception:
            continue
        if math.isnan(hi) or math.isnan(lo) or math.isnan(cl):
            continue

        result = _simulate_trade(setup, hi, lo, cl)
        trades.append({
            'date':           str(df.index[i + 1].date()),
            'setup_type':     setup['type'],
            'side':           setup['side'],
            'conviction':     setup['conviction'],
            **result,
        })
    return trades


def summarize_trades(trades, group_by='setup_type'):
    """Compute per-setup-type stats: win rate, avg R, profit factor, etc."""
    by_group = {}
    for t in trades:
        if t['outcome'].startswith('SKIPPED'):
            continue
        key = t.get(group_by) or 'UNKNOWN'
        by_group.setdefault(key, []).append(t)

    summary = {}
    for setup_type, ts in by_group.items():
        n = len(ts)
        if n == 0: continue
        wins = [t for t in ts if t['r_multiple'] > 0]
        losses = [t for t in ts if t['r_multiple'] <= 0]
        win_rate = len(wins) / n * 100
        avg_r   = sum(t['r_multiple'] for t in ts) / n
        avg_win_r  = sum(t['r_multiple'] for t in wins) / len(wins) if wins else 0
        avg_loss_r = sum(t['r_multiple'] for t in losses) / len(losses) if losses else 0
        # Profit factor: gross wins / gross losses (in R)
        sum_win  = sum(t['r_multiple'] for t in wins)
        sum_loss = abs(sum(t['r_multiple'] for t in losses)) or 0.0001
        profit_factor = sum_win / sum_loss
        # Net PnL (cost-adjusted)
        avg_net_pct = sum(t['net_pnl_pct'] for t in ts) / n
        # T1 / T2 / Stop / MTM breakdown
        outcomes = {}
        for t in ts:
            outcomes[t['outcome']] = outcomes.get(t['outcome'], 0) + 1
        # Max drawdown (cumulative R curve)
        cum = 0; peak = 0; max_dd = 0
        for t in ts:
            cum += t['r_multiple']
            peak = max(peak, cum)
            max_dd = min(max_dd, cum - peak)
        # Expectancy in ₹ per trade @ 1% capital risk
        expectancy_rs = avg_r * (CAPITAL * RISK_PCT_PER_TRADE / 100)

        summary[setup_type] = {
            'n_trades':         n,
            'win_rate_pct':     round(win_rate, 1),
            'avg_r':            round(avg_r, 3),
            'avg_win_r':        round(avg_win_r, 2),
            'avg_loss_r':       round(avg_loss_r, 2),
            'profit_factor':    round(profit_factor, 2),
            'avg_net_pnl_pct':  round(avg_net_pct, 3),
            'max_drawdown_r':   round(max_dd, 2),
            'expectancy_rs':    round(expectancy_rs, 0),
            'outcomes':         outcomes,
            'is_profitable':    avg_net_pct > 0,
        }
    return summary


def run_backtest(top_n=50):
    print(f'Backtesting top {top_n} F&O stocks across 5 setup types...')
    # Pick first N alphabetically from the F&O list (deterministic)
    tickers = sorted(NSE_FNO_STOCKS)[:top_n]

    all_trades = []
    completed = 0
    for ticker in tickers:
        completed += 1
        t0 = time.time()
        try:
            trades = backtest_one_stock(ticker)
        except Exception as e:
            print(f'  [{completed}/{len(tickers)}] {ticker:12s} FAILED: {e}')
            continue
        if trades:
            all_trades.extend(trades)
            print(f'  [{completed}/{len(tickers)}] {ticker:12s} {len(trades):>4d} signals · {time.time()-t0:.1f}s')
        else:
            print(f'  [{completed}/{len(tickers)}] {ticker:12s} no data')

    print(f'\nTotal trades simulated: {len(all_trades)}')
    print('Computing per-setup statistics...')

    by_setup = summarize_trades(all_trades, group_by='setup_type')
    by_side  = summarize_trades(all_trades, group_by='side')

    # Overall portfolio stats (treat ALL trades as one strategy)
    overall = summarize_trades([{**t, '__all__': 'ALL'} for t in all_trades], group_by='__all__')
    overall_stats = overall.get('ALL', {})

    payload = {
        'generated_at':     int(time.time()),
        'generated_at_iso': datetime.now(timezone.utc).isoformat(),
        'config': {
            'lookback_days':       LOOKBACK_DAYS,
            'warmup_days':         WARMUP_DAYS,
            'slippage_pct_leg':    SLIPPAGE_PCT,
            'brokerage_rs':        BROKERAGE_RS,
            'risk_pct_per_trade':  RISK_PCT_PER_TRADE,
            'notional_capital':    CAPITAL,
            'top_n_stocks':        top_n,
            'tickers_scanned':     completed,
        },
        'by_setup_type':    by_setup,
        'by_side':          by_side,
        'overall':          overall_stats,
        'total_trades':     len([t for t in all_trades if not t['outcome'].startswith('SKIPPED')]),
        'total_signals':    len(all_trades),
    }
    out_path = OUT_DIR / 'snapshot_setup_backtest.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    size_kb = out_path.stat().st_size / 1024
    print(f'\nWritten: {out_path.name} ({size_kb:.1f} KB)')
    print(f'\n=== Per-setup performance ===')
    for setup, stats in sorted(by_setup.items(), key=lambda x: -x[1]['avg_r']):
        flag = '[+]' if stats['is_profitable'] else '[-]'
        # Use ASCII-only output to survive Windows cp1252 console + Linux CI alike
        print(f'  {flag} {setup:22s} n={stats["n_trades"]:>4d}  win={stats["win_rate_pct"]:>5.1f}%  '
              f'avg_R={stats["avg_r"]:>+.2f}  PF={stats["profit_factor"]:>4.2f}  '
              f'net%={stats["avg_net_pnl_pct"]:>+.2f}  expectancy=Rs.{stats["expectancy_rs"]:>5.0f}/trade')
    return payload


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', type=int, default=50,
                        help='How many F&O stocks to backtest (default: 50)')
    args = parser.parse_args()
    run_backtest(top_n=args.tickers)
