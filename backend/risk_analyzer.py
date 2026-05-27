import pandas as pd
import numpy as np
import yfinance as yf


def compute_atr_regime(atr_pct_rank):
    """Maps ATR percentile rank to volatility regime and stop multiplier."""
    if atr_pct_rank < 33:
        return 'Low Vol', 1.0
    elif atr_pct_rank < 67:
        return 'Normal', 1.5
    else:
        return 'High Vol', 2.0


def compute_fibonacci_targets(df):
    """Fibonacci extension levels (127.2%, 161.8%, 261.8%) from 60-bar swing range."""
    close = df['Close'].values
    high = df['High'].values
    low = df['Low'].values
    n = len(close)
    current = float(close[-1])

    lookback = min(60, n)
    swing_low = float(min(low[-lookback:]))
    swing_high = float(max(high[-lookback:]))
    swing_range = swing_high - swing_low

    if swing_range <= 0 or current <= 0:
        return None

    fib_127 = round(swing_low + 1.272 * swing_range, 4)
    fib_162 = round(swing_low + 1.618 * swing_range, 4)
    fib_262 = round(swing_low + 2.618 * swing_range, 4)

    return {
        'swing_low': round(swing_low, 4),
        'swing_high': round(swing_high, 4),
        'fib_127': fib_127,
        'fib_162': fib_162,
        'fib_262': fib_262,
        'fib_127_pct': round((fib_127 - current) / current * 100, 1),
        'fib_162_pct': round((fib_162 - current) / current * 100, 1),
        'fib_262_pct': round((fib_262 - current) / current * 100, 1),
    }


# ── Multi-source Support / Resistance ────────────────────────────────────────

def _cluster_levels(levels, tolerance_pct=1.5):
    """Merge levels within tolerance_pct% of each other into a single average."""
    if not levels:
        return []
    sorted_lvls = sorted(levels)
    groups = [[sorted_lvls[0]]]
    for lvl in sorted_lvls[1:]:
        if (lvl - groups[-1][0]) / groups[-1][0] * 100 < tolerance_pct:
            groups[-1].append(lvl)
        else:
            groups.append([lvl])
    return [round(sum(g) / len(g), 3) for g in groups]


def compute_sr_levels(df, current):
    """
    Multi-source support / resistance:
      1. Swing pivots  — 3-bar, 5-bar, 10-bar windows
      2. SMAs          — 20, 50, 200
      3. Period highs/lows — 10d, 20d, 60d
      4. Psychological round numbers (step adapts to price magnitude)

    Nearby levels (within 1.5%) are merged into a single zone.
    Returns (supports_desc, resistances_asc) both within ±100% of current price.
    """
    close_arr = df['Close'].values.astype(float)
    high_arr  = df['High'].values.astype(float)
    low_arr   = df['Low'].values.astype(float)
    n = len(close_arr)
    raw: set = set()

    # 1. Multi-window swing pivots (3, 5, 10-bar)
    for w in (3, 5, 10):
        for i in range(w, n - w):
            window_h = high_arr[i - w: i + w + 1]
            window_l = low_arr[i - w: i + w + 1]
            if high_arr[i] == float(np.max(window_h)):
                raw.add(round(float(high_arr[i]), 3))
            if low_arr[i] == float(np.min(window_l)):
                raw.add(round(float(low_arr[i]), 3))

    # 2. SMA 20 / 50 / 200
    for win in (20, 50, 200):
        if n >= win:
            sma = float(np.mean(close_arr[-win:]))
            if not np.isnan(sma):
                raw.add(round(sma, 3))

    # 3. Recent period highs / lows (10d, 20d, 60d)
    for period in (10, 20, 60):
        if n >= period:
            raw.add(round(float(np.max(high_arr[-period:])), 3))
            raw.add(round(float(np.min(low_arr[-period:])), 3))

    # 4. Psychological round numbers
    if current < 1:
        step = 0.10
    elif current < 3:
        step = 0.25
    elif current < 10:
        step = 0.50
    elif current < 30:
        step = 1.0
    elif current < 100:
        step = 5.0
    else:
        step = 10.0

    v = round(int(current * 0.5 / step) * step, 3)
    while v <= current * 2.0 + step:
        raw.add(round(v, 3))
        v = round(v + step, 3)

    # Filter to ±100% of current price
    lo_bound = current * 0.50
    hi_bound = current * 2.00
    raw = {l for l in raw if lo_bound <= l <= hi_bound}

    # Cluster nearby levels and split into supports / resistances
    clustered = _cluster_levels(raw)
    resistances = sorted([l for l in clustered if l > current * 1.005])
    supports    = sorted([l for l in clustered if l < current * 0.995], reverse=True)
    return supports, resistances


def compute_risk(df, ticker):
    close = df['Close']
    high = df['High']
    low = df['Low']

    returns = close.pct_change().dropna()

    hv_30 = float(returns.iloc[-30:].std() * np.sqrt(252)) if len(returns) >= 30 else float(returns.std() * np.sqrt(252))
    hv_1y = float(returns.std() * np.sqrt(252))

    rolling_max = close.cummax()
    drawdown = (close - rolling_max) / rolling_max.replace(0, np.nan)
    max_drawdown = float(drawdown.min())

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr = float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1])
    atr_pct = atr / float(close.iloc[-1]) * 100

    # Beta vs SPY
    beta = 1.0
    try:
        spy_df = yf.download('SPY', period='1y', progress=False, auto_adjust=True)
        if isinstance(spy_df.columns, pd.MultiIndex):
            spy_df.columns = spy_df.columns.get_level_values(0)
        spy_ret = spy_df['Close'].pct_change().dropna()
        common = returns.index.intersection(spy_ret.index)
        if len(common) > 60:
            sr = returns[common].values
            spr = spy_ret[common].values
            cov = np.cov(sr, spr)[0][1]
            var = np.var(spr)
            beta = cov / var if var > 0 else 1.0
    except Exception:
        pass

    # Risk tier
    if hv_30 < 0.20:
        risk_tier, risk_color = 'Low', '#22c55e'
    elif hv_30 < 0.35:
        risk_tier, risk_color = 'Medium', '#f59e0b'
    elif hv_30 < 0.55:
        risk_tier, risk_color = 'High', '#ef4444'
    else:
        risk_tier, risk_color = 'Very High', '#dc2626'

    risk_score = min(100, int(hv_30 * 100 + abs(max_drawdown) * 40 + min(abs(beta - 1) * 15, 20)))

    # 6-month Sharpe-like ratio
    sharpe_6m = None
    if len(close) >= 126:
        ret_6m = (float(close.iloc[-1]) - float(close.iloc[-126])) / float(close.iloc[-126])
        vol_6m = float(returns.iloc[-126:].std() * np.sqrt(252))
        sharpe_6m = round(ret_6m / vol_6m, 2) if vol_6m > 0 else None

    n = len(close)
    high_52w = float(close.iloc[-min(252, n):].max())
    low_52w = float(close.iloc[-min(252, n):].min())
    cur = float(close.iloc[-1])
    pos_52w = (cur - low_52w) / (high_52w - low_52w) * 100 if high_52w != low_52w else 50.0

    # Position sizing suggestion (1% risk)
    risk_per_share = atr * 1.5
    suggested_size_1pct = round(1000 / risk_per_share, 0) if risk_per_share > 0 else 0

    return {
        'tier': risk_tier,
        'color': risk_color,
        'score': risk_score,
        'beta': round(beta, 2),
        'hv_30d': round(hv_30 * 100, 1),
        'hv_1y': round(hv_1y * 100, 1),
        'max_drawdown_1y': round(max_drawdown * 100, 1),
        'atr': round(atr, 2),
        'atr_pct': round(atr_pct, 2),
        'sharpe_6m': sharpe_6m,
        'high_52w': round(high_52w, 2),
        'low_52w': round(low_52w, 2),
        'pos_52w_pct': round(pos_52w, 1),
        'suggested_size_1pct': int(suggested_size_1pct),
    }


def compute_trade_setup(df, ta_score, fa_score, news_score, risk, atr_pct_rank=50, fibonacci=None, social_score=0, fa_available=True):
    close = df['Close'].values
    high = df['High'].values
    low = df['Low'].values
    n = len(close)
    current = float(close[-1])
    atr = risk['atr']

    if fa_available:
        composite = ta_score * 0.45 + fa_score * 0.30 + news_score * 0.15 + social_score * 0.10
        mt_score = fa_score * 0.55 + ta_score * 0.30 + news_score * 0.10 + social_score * 0.05
        lt_score = fa_score * 0.70 + ta_score * 0.15 + news_score * 0.10 + social_score * 0.05
    else:
        composite = ta_score * 0.60 + news_score * 0.25 + social_score * 0.15
        mt_score  = ta_score * 0.65 + news_score * 0.25 + social_score * 0.10
        lt_score  = ta_score * 0.65 + news_score * 0.25 + social_score * 0.10
    st_score = ta_score * 0.65 + news_score * 0.25 + social_score * 0.10

    def _dir(score):
        if score >= 15:  return 'bullish'
        if score <= -15: return 'bearish'
        return 'neutral'

    st_dir = _dir(st_score)
    mt_dir = _dir(mt_score)
    lt_dir = _dir(lt_score)

    # ATR regime — dynamic stop multipliers
    _, regime_mult = compute_atr_regime(atr_pct_rank)
    st_stop_mult = regime_mult          # Low=1.0, Normal=1.5, High=2.0
    mt_stop_mult = regime_mult * 2.0    # Low=2.0, Normal=3.0, High=4.0
    lt_stop_mult = regime_mult * 3.0    # Low=3.0, Normal=4.5, High=6.0

    # ── Multi-source support / resistance ─────────────────────────────────────
    supports, resistances = compute_sr_levels(df, current)

    def resistance_above(price_floor):
        """Nearest resistance strictly above price_floor × 1.005."""
        lvls = [r for r in resistances if r > price_floor * 1.005]
        return lvls[0] if lvls else None

    def support_below(price_ceil):
        """Nearest support strictly below price_ceil × 0.995 (supports sorted desc)."""
        lvls = [s for s in supports if s < price_ceil * 0.995]
        return lvls[0] if lvls else None

    # Expected daily move ≈ 50% of ATR; used for dynamic day estimates
    _atr_pct = atr / current * 100
    _daily_pct = max(0.10, _atr_pct * 0.50)

    def _days(dist_pct, cap, floor=1):
        return max(floor, min(cap, round(dist_pct / _daily_pct)))

    def _pick(candidates):
        """Return (price, label) for the candidate with the lowest price."""
        return min(candidates, key=lambda x: x[0])

    def _pick_max(candidates):
        """Return (price, label) for the candidate with the highest price (bearish targets)."""
        return max(candidates, key=lambda x: x[0])

    def build_setup(direction, stop_mult, atr_t1_mult, atr_t2_mult,
                    timeframe, day_cap_t1, day_cap_t2, use_fib=False,
                    floor_t1=1, floor_t2=2, res_min_pct=1.0):
        """
        res_min_pct: minimum % distance a resistance/support level must be
        from current price to qualify as a target candidate.
        Short-term uses 1% (tight), mid-term uses 4% (needs room to develop).
        """

        if direction == 'bullish':
            entry = current
            stop  = round(current - stop_mult * atr, 2)
            t1_atr_price = round(current + atr_t1_mult * atr, 2)
            t2_atr_price = round(current + atr_t2_mult * atr, 2)

            # ── Target 1 ─────────────────────────────────────────────────────
            t1_cands = [(t1_atr_price, f'ATR {atr_t1_mult}×')]
            nr = resistance_above(current * (1 + res_min_pct / 100))
            if nr:
                t1_cands.append((nr, 'Resistance'))
            if use_fib and fibonacci:
                f127 = fibonacci.get('fib_127')
                if f127 and f127 > current * 1.01:
                    t1_cands.append((f127, 'Fib 127.2%'))
            t1, t1_basis = _pick(t1_cands)
            t1 = round(t1, 2)

            # ── Target 2 (must be above T1) ───────────────────────────────────
            t2_cands = [(t2_atr_price, f'ATR {atr_t2_mult}×')]
            fr = resistance_above(t1)          # first resistance above T1
            if fr:
                t2_cands.append((fr, 'Resistance'))
            if use_fib and fibonacci:
                f162 = fibonacci.get('fib_162')
                if f162 and f162 > t1 * 1.01:
                    t2_cands.append((f162, 'Fib 161.8%'))
            t2, t2_basis = _pick(t2_cands)
            t2 = round(t2, 2)
            # Guarantee T2 is meaningfully above T1
            min_t2 = round(t1 + atr * 0.5, 2)
            if t2 <= t1:
                t2, t2_basis = min_t2, f'ATR {atr_t2_mult}×'
            elif t2 < min_t2:
                t2 = min_t2

            risk_amt = entry - stop
            rr1 = round((t1 - entry) / risk_amt, 1) if risk_amt > 0 else 0
            rr2 = round((t2 - entry) / risk_amt, 1) if risk_amt > 0 else 0
            t1_pct = round((t1 - entry) / entry * 100, 1)
            t2_pct = round((t2 - entry) / entry * 100, 1)
            return {
                'timeframe': timeframe, 'direction': 'LONG',
                'entry': round(entry, 2), 'stop_loss': stop,
                'stop_pct': round((entry - stop) / entry * 100, 1),
                'target_1': t1, 'target_1_pct': t1_pct, 'target_1_basis': t1_basis,
                'target_2': t2, 'target_2_pct': t2_pct, 'target_2_basis': t2_basis,
                'risk_reward_t1': rr1, 'risk_reward_t2': rr2,
                'days_to_t1': _days(t1_pct, day_cap_t1, floor_t1),
                'days_to_t2': _days(t2_pct, day_cap_t2, floor_t2),
            }

        elif direction == 'bearish':
            entry = current
            stop  = round(current + stop_mult * atr, 2)
            t1_atr_price = round(current - atr_t1_mult * atr, 2)
            t2_atr_price = round(current - atr_t2_mult * atr, 2)

            # ── Target 1 ─────────────────────────────────────────────────────
            t1_cands = [(t1_atr_price, f'ATR {atr_t1_mult}×')]
            ns = support_below(current * (1 - res_min_pct / 100))
            if ns:
                t1_cands.append((ns, 'Support'))
            t1, t1_basis = _pick_max(t1_cands)   # highest = closest to entry (conservative)
            t1 = round(t1, 2)

            # ── Target 2 (must be below T1) ───────────────────────────────────
            t2_cands = [(t2_atr_price, f'ATR {atr_t2_mult}×')]
            fs = support_below(t1)
            if fs:
                t2_cands.append((fs, 'Support'))
            t2, t2_basis = _pick_max(t2_cands)
            t2 = round(t2, 2)
            max_t2 = round(t1 - atr * 0.5, 2)
            if t2 >= t1:
                t2, t2_basis = max_t2, f'ATR {atr_t2_mult}×'
            elif t2 > max_t2:
                t2 = max_t2

            risk_amt = stop - entry
            rr1 = round((entry - t1) / risk_amt, 1) if risk_amt > 0 else 0
            rr2 = round((entry - t2) / risk_amt, 1) if risk_amt > 0 else 0
            t1_pct = round((entry - t1) / entry * 100, 1)
            t2_pct = round((entry - t2) / entry * 100, 1)
            return {
                'timeframe': timeframe, 'direction': 'SHORT',
                'entry': round(entry, 2), 'stop_loss': stop,
                'stop_pct': round((stop - entry) / entry * 100, 1),
                'target_1': t1, 'target_1_pct': t1_pct, 'target_1_basis': t1_basis,
                'target_2': t2, 'target_2_pct': t2_pct, 'target_2_basis': t2_basis,
                'risk_reward_t1': rr1, 'risk_reward_t2': rr2,
                'days_to_t1': _days(t1_pct, day_cap_t1, floor_t1),
                'days_to_t2': _days(t2_pct, day_cap_t2, floor_t2),
            }

        else:  # neutral / hold-watch — show potential upside levels
            entry = current
            stop  = round(current - stop_mult * atr, 2)
            t1_atr_price = round(current + atr_t1_mult * atr, 2)
            t2_atr_price = round(current + atr_t2_mult * atr, 2)

            nr = resistance_above(current * (1 + res_min_pct / 100))
            t1_cands = [(t1_atr_price, f'ATR {atr_t1_mult}×')]
            if nr:
                t1_cands.append((nr, 'Resistance'))
            t1, t1_basis = _pick(t1_cands)
            t1 = round(t1, 2)

            fr = resistance_above(t1)
            t2_cands = [(t2_atr_price, f'ATR {atr_t2_mult}×')]
            if fr:
                t2_cands.append((fr, 'Resistance'))
            t2, t2_basis = _pick(t2_cands)
            t2 = round(max(t2, round(t1 + atr * 0.5, 2)), 2)

            risk_amt = entry - stop
            rr1 = round((t1 - entry) / risk_amt, 1) if risk_amt > 0 else 0
            rr2 = round((t2 - entry) / risk_amt, 1) if risk_amt > 0 else 0
            t1_pct = round((t1 - entry) / entry * 100, 1)
            t2_pct = round((t2 - entry) / entry * 100, 1)
            return {
                'timeframe': timeframe, 'direction': 'HOLD/WATCH',
                'entry': round(entry, 2), 'stop_loss': stop,
                'stop_pct': round((entry - stop) / entry * 100, 1),
                'target_1': t1, 'target_1_pct': t1_pct, 'target_1_basis': t1_basis,
                'target_2': t2, 'target_2_pct': t2_pct, 'target_2_basis': t2_basis,
                'risk_reward_t1': rr1, 'risk_reward_t2': rr2,
                'days_to_t1': _days(t1_pct, day_cap_t1, floor_t1),
                'days_to_t2': _days(t2_pct, day_cap_t2, floor_t2),
            }

    short_term = build_setup(st_dir, st_stop_mult,  2.0,  3.5, '1–3 weeks',   21,  42, floor_t1=2,  floor_t2=4,  res_min_pct=1.0)
    mid_term   = build_setup(mt_dir, mt_stop_mult,  5.0,  8.0, '1–3 months',  90, 120, use_fib=True, floor_t1=14, floor_t2=30, res_min_pct=4.0)
    long_term  = build_setup(lt_dir, lt_stop_mult, 10.0, 16.0, '3–12 months', 180, 365, use_fib=True, floor_t1=45, floor_t2=90, res_min_pct=7.0)

    return short_term, mid_term, long_term, st_dir, mt_dir, lt_dir, composite
