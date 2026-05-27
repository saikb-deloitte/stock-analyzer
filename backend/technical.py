import pandas as pd
import numpy as np


def compute_indicators(df):
    close = df['Close']
    high = df['High']
    low = df['Low']
    volume = df['Volume']

    df['SMA20'] = close.rolling(20).mean()
    df['SMA50'] = close.rolling(50).mean()
    df['SMA200'] = close.rolling(200).mean()
    df['EMA9'] = close.ewm(span=9, adjust=False).mean()
    df['EMA21'] = close.ewm(span=21, adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df['RSI'] = 100 - (100 / (1 + rs))

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    df['BB_Upper'] = bb_mid + 2 * bb_std
    df['BB_Lower'] = bb_mid - 2 * bb_std
    df['BB_Mid'] = bb_mid
    bb_range = df['BB_Upper'] - df['BB_Lower']
    df['BB_Pct'] = (close - df['BB_Lower']) / bb_range.replace(0, np.nan)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    df['ATR'] = tr.ewm(alpha=1/14, adjust=False).mean()

    low14 = low.rolling(14).min()
    high14 = high.rolling(14).max()
    denom = (high14 - low14).replace(0, np.nan)
    df['Stoch_K'] = 100 * (close - low14) / denom
    df['Stoch_D'] = df['Stoch_K'].rolling(3).mean()

    df['Vol_MA20'] = volume.rolling(20).mean()

    # ADX — Wilder's original +DM / -DM definition (1978)
    # +DM = up-move if up-move > down-move AND up-move > 0, else 0
    # -DM = down-move if down-move > up-move AND down-move > 0, else 0
    up_move   = high.diff()           # today_high − yesterday_high
    down_move = (-low.diff())         # yesterday_low − today_low (positive when low falls)
    plus_dm  = pd.Series(
        np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
        index=high.index)
    minus_dm = pd.Series(
        np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
        index=low.index)
    atr14    = df['ATR'].replace(0, np.nan)
    di_plus  = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    di_minus = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14
    di_sum   = (di_plus + di_minus).replace(0, np.nan)
    dx       = 100 * (di_plus - di_minus).abs() / di_sum
    df['ADX'] = dx.ewm(alpha=1/14, adjust=False).mean()

    return df


def detect_patterns(df):
    patterns = []
    close = df['Close'].values
    high = df['High'].values
    low = df['Low'].values
    opens = df['Open'].values if 'Open' in df.columns else close
    volume = df['Volume'].values
    n = len(close)

    if n < 40:
        return patterns

    current = close[-1]

    # ── Candlestick Patterns (last 3 bars) ───────────────────────
    if n >= 11:  # need 10 bars back for trend context
        o1, h1, l1, c1 = opens[-1], high[-1], low[-1], close[-1]
        o2, h2, l2, c2 = opens[-2], high[-2], low[-2], close[-2]
        o3, h3, l3, c3 = opens[-3], high[-3], low[-3], close[-3]

        body1  = abs(c1 - o1)
        rng1   = h1 - l1
        upper1 = h1 - max(o1, c1)
        lower1 = min(o1, c1) - l1
        body2  = abs(c2 - o2)
        rng2   = h2 - l2
        upper2 = h2 - max(o2, c2)
        lower2 = min(o2, c2) - l2
        body3  = abs(c3 - o3)
        rng3   = h3 - l3
        upper3 = h3 - max(o3, c3)
        lower3 = min(o3, c3) - l3

        # Trend context: 10-bar lookback (requires at least 1% move to qualify)
        in_downtrend = close[-10] > c1 * 1.01
        in_uptrend   = close[-10] < c1 * 0.99

        # ── Single-bar patterns ──

        # Dragonfly Doji — bullish: tiny body at top, long lower shadow only
        if (rng1 > 0 and body1 < rng1 * 0.07 and upper1 < rng1 * 0.07
                and lower1 > rng1 * 0.70):
            patterns.append({'name': 'Dragonfly Doji', 'type': 'bullish', 'strength': 'moderate',
                             'description': 'Open ≈ Close ≈ High — buyers reclaimed lows; potential bullish reversal'})

        # Gravestone Doji — bearish: tiny body at bottom, long upper shadow only
        elif (rng1 > 0 and body1 < rng1 * 0.07 and lower1 < rng1 * 0.07
              and upper1 > rng1 * 0.70):
            patterns.append({'name': 'Gravestone Doji', 'type': 'bearish', 'strength': 'moderate',
                             'description': 'Open ≈ Close ≈ Low — sellers rejected the highs; potential bearish reversal'})

        # Generic Doji — indecision (catches everything not already classified above)
        elif rng1 > 0 and body1 / rng1 < 0.10:
            patterns.append({'name': 'Doji', 'type': 'neutral', 'strength': 'moderate',
                             'description': 'Open ≈ Close — market indecision, watch for breakout direction'})

        # Spinning Top — small body, notable shadows both sides (not doji)
        if (rng1 > 0 and 0.10 <= body1 / rng1 <= 0.35
                and upper1 > body1 * 0.5 and lower1 > body1 * 0.5):
            patterns.append({'name': 'Spinning Top', 'type': 'neutral', 'strength': 'weak',
                             'description': 'Small body with shadows on both sides — indecision, wait for next candle'})

        # Bullish Marubozu — full bull body, virtually no shadows
        if rng1 > 0 and body1 / rng1 > 0.92 and c1 > o1:
            patterns.append({'name': 'Bullish Marubozu', 'type': 'bullish', 'strength': 'strong',
                             'description': 'Full bull candle, minimal shadows — strong buying conviction'})

        # Bearish Marubozu — full bear body, virtually no shadows
        if rng1 > 0 and body1 / rng1 > 0.92 and c1 < o1:
            patterns.append({'name': 'Bearish Marubozu', 'type': 'bearish', 'strength': 'strong',
                             'description': 'Full bear candle, minimal shadows — strong selling conviction'})

        # Hammer — bullish reversal: long lower shadow, small body at top, DOWNTREND context
        if (rng1 > 0 and in_downtrend
                and body1 / rng1 < 0.35 and lower1 >= 2 * body1 and upper1 <= body1 * 1.1):
            patterns.append({'name': 'Hammer', 'type': 'bullish', 'strength': 'strong',
                             'description': f'Long lower shadow at ${c1:.2f} — buyers rejected the lows decisively'})

        # Inverted Hammer — bullish reversal: long upper shadow, small body at bottom, DOWNTREND context
        elif (rng1 > 0 and in_downtrend
              and body1 / rng1 < 0.35 and upper1 >= 2 * body1 and lower1 <= body1 * 0.5):
            patterns.append({'name': 'Inverted Hammer', 'type': 'bullish', 'strength': 'moderate',
                             'description': f'Long upper shadow after downtrend at ${c1:.2f} — potential reversal, confirm with next bar'})

        # Shooting Star — bearish reversal: long upper shadow, small body at bottom, UPTREND context
        if (rng1 > 0 and in_uptrend
                and body1 / rng1 < 0.35 and upper1 >= 2 * body1 and lower1 <= body1 * 1.1):
            patterns.append({'name': 'Shooting Star', 'type': 'bearish', 'strength': 'strong',
                             'description': f'Long upper shadow at ${c1:.2f} — sellers pushed back from the highs'})

        # Hanging Man — bearish reversal: long lower shadow, small body at top, UPTREND context
        elif (rng1 > 0 and in_uptrend
              and body1 / rng1 < 0.35 and lower1 >= 2 * body1 and upper1 <= body1 * 0.5):
            patterns.append({'name': 'Hanging Man', 'type': 'bearish', 'strength': 'moderate',
                             'description': f'Hammer shape in uptrend at ${c1:.2f} — bears tested lows; bearish if next bar confirms'})

        # ── Two-bar patterns ──

        # Bullish Engulfing
        if c1 > o1 and c2 < o2 and c1 >= o2 and o1 <= c2:
            patterns.append({'name': 'Bullish Engulfing', 'type': 'bullish', 'strength': 'strong',
                             'description': 'Green candle fully engulfs prior red — strong buying reversal'})

        # Bearish Engulfing
        if c1 < o1 and c2 > o2 and c1 <= o2 and o1 >= c2:
            patterns.append({'name': 'Bearish Engulfing', 'type': 'bearish', 'strength': 'strong',
                             'description': 'Red candle fully engulfs prior green — strong selling reversal'})

        # Bullish Harami — small green bar contained within large red bar's body
        if (c2 < o2 and body2 > 0 and c1 > o1
                and o1 >= c2 and c1 <= o2 and body1 < body2 * 0.60):
            patterns.append({'name': 'Bullish Harami', 'type': 'bullish', 'strength': 'moderate',
                             'description': 'Small green bar inside prior red body — selling momentum may be stalling'})

        # Bearish Harami — small red bar contained within large green bar's body
        if (c2 > o2 and body2 > 0 and c1 < o1
                and o1 <= c2 and c1 >= o2 and body1 < body2 * 0.60):
            patterns.append({'name': 'Bearish Harami', 'type': 'bearish', 'strength': 'moderate',
                             'description': 'Small red bar inside prior green body — buying momentum may be stalling'})

        # Piercing Pattern — opens below prior close, closes above prior bar's midpoint (bullish reversal)
        if (c2 < o2 and body2 > 0 and c1 > o1
                and o1 < c2 and c1 > (o2 + c2) / 2 and c1 < o2):
            patterns.append({'name': 'Piercing Pattern', 'type': 'bullish', 'strength': 'moderate',
                             'description': 'Opens below prior close, closes above prior midpoint — bulls recovering momentum'})

        # Dark Cloud Cover — opens above prior close, closes below prior bar's midpoint (bearish reversal)
        if (c2 > o2 and body2 > 0 and c1 < o1
                and o1 > c2 and c1 < (o2 + c2) / 2 and c1 > o2):
            patterns.append({'name': 'Dark Cloud Cover', 'type': 'bearish', 'strength': 'moderate',
                             'description': 'Opens above prior close, closes below prior midpoint — bears taking control'})

        # Tweezer Bottom — matching lows on two consecutive bars (bullish reversal)
        if (abs(l1 - l2) / (max(l1, l2) + 1e-9) < 0.003
                and close[-5] > c1 * 1.005):
            patterns.append({'name': 'Tweezer Bottom', 'type': 'bullish', 'strength': 'moderate',
                             'description': f'Matching lows at ${min(l1, l2):.2f} — double support tested; reversal likely'})

        # Tweezer Top — matching highs on two consecutive bars (bearish reversal)
        if (abs(h1 - h2) / (max(h1, h2) + 1e-9) < 0.003
                and close[-5] < c1 * 0.995):
            patterns.append({'name': 'Tweezer Top', 'type': 'bearish', 'strength': 'moderate',
                             'description': f'Matching highs at ${max(h1, h2):.2f} — double resistance tested; reversal likely'})

        # ── Three-bar patterns ──

        # Morning Star (3-bar bullish reversal)
        if (c3 < o3 and body3 > 0 and                     # bar 3: big red
                body2 < body3 * 0.35 and                  # bar 2: small star
                c1 > o1 and body1 >= body3 * 0.5 and      # bar 1: big green
                c1 > (o3 + c3) / 2):                      # closes above bar-3 midpoint
            patterns.append({'name': 'Morning Star', 'type': 'bullish', 'strength': 'strong',
                             'description': '3-bar reversal: large red → indecision star → large green'})

        # Evening Star (3-bar bearish reversal)
        if (c3 > o3 and body3 > 0 and                     # bar 3: big green
                body2 < body3 * 0.35 and                  # bar 2: small star
                c1 < o1 and body1 >= body3 * 0.5 and      # bar 1: big red
                c1 < (o3 + c3) / 2):                      # closes below bar-3 midpoint
            patterns.append({'name': 'Evening Star', 'type': 'bearish', 'strength': 'strong',
                             'description': '3-bar reversal: large green → indecision star → large red'})

        # Three White Soldiers — 3 consecutive strong bull bars, each closing higher
        if (c1 > o1 and c2 > o2 and c3 > o3 and              # all green
                c1 > c2 and c2 > c3 and                       # each closes higher
                o1 >= o2 and o2 >= o3 and                     # each opens at/above prior open (within body)
                o1 <= c2 and o2 <= c3 and                     # each opens within prior body
                body1 > rng1 * 0.40 and body2 > rng2 * 0.40 and body3 > rng3 * 0.40 and
                upper1 < body1 * 0.30 and upper2 < body2 * 0.30):   # small upper shadows
            patterns.append({'name': 'Three White Soldiers', 'type': 'bullish', 'strength': 'strong',
                             'description': '3 strong bull bars, each closing at new high — sustained buying pressure'})

        # Three Black Crows — 3 consecutive strong bear bars, each closing lower
        if (c1 < o1 and c2 < o2 and c3 < o3 and              # all red
                c1 < c2 and c2 < c3 and                       # each closes lower
                o1 <= o2 and o2 <= o3 and                     # each opens at/below prior open (within body)
                o1 >= c2 and o2 >= c3 and                     # each opens within prior body
                body1 > rng1 * 0.40 and body2 > rng2 * 0.40 and body3 > rng3 * 0.40 and
                lower1 < body1 * 0.30 and lower2 < body2 * 0.30):   # small lower shadows
            patterns.append({'name': 'Three Black Crows', 'type': 'bearish', 'strength': 'strong',
                             'description': '3 strong bear bars, each closing at new low — sustained selling pressure'})

    # ── RSI Divergence ───────────────────────────────────────────
    rsi_vals = df['RSI'].values
    lookback = 30
    if n >= lookback and not np.any(np.isnan(rsi_vals[-lookback:])):
        p = close[-lookback:]
        r = rsi_vals[-lookback:]

        # Bullish RSI divergence: price makes lower low, RSI makes higher low
        lows_i = [i for i in range(2, lookback - 2)
                  if p[i] < p[i-1] and p[i] < p[i-2] and p[i] < p[i+1] and p[i] < p[i+2]]
        if len(lows_i) >= 2:
            i1, i2 = lows_i[-2], lows_i[-1]
            if p[i2] < p[i1] * 0.99 and r[i2] > r[i1] + 5:
                patterns.append({'name': 'RSI Bullish Divergence', 'type': 'bullish', 'strength': 'strong',
                                 'description': f'Price lower low but RSI higher low ({r[i2]:.0f} > {r[i1]:.0f}) — momentum improving despite price weakness'})

        # Bearish RSI divergence: price makes higher high, RSI makes lower high
        highs_i = [i for i in range(2, lookback - 2)
                   if p[i] > p[i-1] and p[i] > p[i-2] and p[i] > p[i+1] and p[i] > p[i+2]]
        if len(highs_i) >= 2:
            i1, i2 = highs_i[-2], highs_i[-1]
            if p[i2] > p[i1] * 1.01 and r[i2] < r[i1] - 5:
                patterns.append({'name': 'RSI Bearish Divergence', 'type': 'bearish', 'strength': 'strong',
                                 'description': f'Price higher high but RSI lower high ({r[i2]:.0f} < {r[i1]:.0f}) — momentum weakening despite price strength'})

    # ── MACD Divergence ──────────────────────────────────────────
    macd_vals = df['MACD'].values
    if n >= lookback and not np.any(np.isnan(macd_vals[-lookback:])):
        p = close[-lookback:]
        m = macd_vals[-lookback:]
        lows_i = [i for i in range(2, lookback - 2)
                  if p[i] < p[i-1] and p[i] < p[i-2] and p[i] < p[i+1] and p[i] < p[i+2]]
        if len(lows_i) >= 2:
            i1, i2 = lows_i[-2], lows_i[-1]
            if p[i2] < p[i1] * 0.99 and m[i2] > m[i1]:
                patterns.append({'name': 'MACD Bullish Divergence', 'type': 'bullish', 'strength': 'moderate',
                                 'description': 'Price lower low but MACD higher low — momentum improving despite price weakness'})
        highs_i = [i for i in range(2, lookback - 2)
                   if p[i] > p[i-1] and p[i] > p[i-2] and p[i] > p[i+1] and p[i] > p[i+2]]
        if len(highs_i) >= 2:
            i1, i2 = highs_i[-2], highs_i[-1]
            if p[i2] > p[i1] * 1.01 and m[i2] < m[i1]:
                patterns.append({'name': 'MACD Bearish Divergence', 'type': 'bearish', 'strength': 'moderate',
                                 'description': 'Price higher high but MACD lower high — momentum fading despite price strength'})

    # Golden / Death Cross (SMA20 vs SMA50)
    if not df['SMA20'].isna().iloc[-1] and not df['SMA50'].isna().iloc[-1]:
        sma20 = df['SMA20'].values
        sma50 = df['SMA50'].values
        lookback = min(15, n - 1)
        crossed_up = sma20[-1] > sma50[-1] and sma20[-lookback] < sma50[-lookback]
        crossed_dn = sma20[-1] < sma50[-1] and sma20[-lookback] > sma50[-lookback]
        if crossed_up:
            patterns.append({'name': 'Golden Cross', 'type': 'bullish', 'strength': 'strong',
                              'description': 'SMA20 crossed above SMA50 — bullish momentum shift'})
        elif crossed_dn:
            patterns.append({'name': 'Death Cross', 'type': 'bearish', 'strength': 'strong',
                              'description': 'SMA20 crossed below SMA50 — bearish momentum shift'})

    # Bull Flag
    if n >= 30:
        past_ret = (close[-11] - close[-21]) / (close[-21] + 1e-9)
        recent_range = (max(close[-6:-1]) - min(close[-6:-1])) / (close[-6] + 1e-9)
        if past_ret > 0.05 and recent_range < 0.03:
            patterns.append({'name': 'Bull Flag', 'type': 'bullish', 'strength': 'moderate',
                              'description': 'Tight consolidation after strong uptrend'})

    # Bear Flag
    if n >= 30:
        past_ret = (close[-11] - close[-21]) / (close[-21] + 1e-9)
        recent_range = (max(close[-6:-1]) - min(close[-6:-1])) / (close[-6] + 1e-9)
        if past_ret < -0.05 and recent_range < 0.03:
            patterns.append({'name': 'Bear Flag', 'type': 'bearish', 'strength': 'moderate',
                              'description': 'Tight consolidation after strong downtrend'})

    # Double Bottom
    lows = [(i, low[i]) for i in range(5, n - 5) if low[i] == min(low[i-5:i+6])]
    if len(lows) >= 2:
        l1i, l1v = lows[-2]
        l2i, l2v = lows[-1]
        if abs(l1v - l2v) / (l1v + 1e-9) < 0.03 and (l2i - l1i) > 5:
            patterns.append({'name': 'Double Bottom', 'type': 'bullish', 'strength': 'strong',
                              'description': f'Two lows near ${l1v:.2f} — potential reversal'})

    # Double Top
    peaks = [(i, high[i]) for i in range(5, n - 5) if high[i] == max(high[i-5:i+6])]
    if len(peaks) >= 2:
        p1i, p1v = peaks[-2]
        p2i, p2v = peaks[-1]
        if abs(p1v - p2v) / (p1v + 1e-9) < 0.03 and (p2i - p1i) > 5:
            patterns.append({'name': 'Double Top', 'type': 'bearish', 'strength': 'strong',
                              'description': f'Two peaks near ${p1v:.2f} — potential reversal'})

    # RSI-based
    rsi = df['RSI'].values
    if not np.isnan(rsi[-1]):
        if rsi[-1] < 35 and close[-1] > close[-3]:
            patterns.append({'name': 'Oversold Bounce', 'type': 'bullish', 'strength': 'moderate',
                              'description': f'RSI {rsi[-1]:.0f} with price recovering'})
        if rsi[-1] > 70 and close[-1] < close[-3]:
            patterns.append({'name': 'Overbought Reversal', 'type': 'bearish', 'strength': 'moderate',
                              'description': f'RSI {rsi[-1]:.0f} with price fading'})

    # Volume breakout / breakdown
    vol_ma = df['Vol_MA20'].values
    if not np.isnan(vol_ma[-1]) and vol_ma[-1] > 0:
        vol_ratio = volume[-1] / vol_ma[-1]
        if vol_ratio > 2 and close[-1] > close[-2]:
            patterns.append({'name': 'Volume Breakout', 'type': 'bullish', 'strength': 'strong',
                              'description': f'{vol_ratio:.1f}x avg volume with price surge'})
        elif vol_ratio > 2 and close[-1] < close[-2]:
            patterns.append({'name': 'Volume Breakdown', 'type': 'bearish', 'strength': 'strong',
                              'description': f'{vol_ratio:.1f}x avg volume with price drop'})

    # Tight consolidation
    if n >= 20:
        r20 = (max(close[-20:]) - min(close[-20:])) / (close[-20] + 1e-9)
        r5 = (max(close[-5:]) - min(close[-5:])) / (close[-5] + 1e-9)
        if r20 > 0.08 and r5 < 0.02:
            patterns.append({'name': 'Tight Consolidation', 'type': 'neutral', 'strength': 'moderate',
                              'description': 'Compression — potential breakout setup'})

    return patterns


def compute_signals(df):
    signals = []
    close = df['Close'].iloc[-1]

    def v(col):
        val = df[col].iloc[-1]
        return None if pd.isna(val) else float(val)

    sma20, sma50, sma200 = v('SMA20'), v('SMA50'), v('SMA200')
    rsi = v('RSI')
    macd, macd_sig, macd_hist = v('MACD'), v('MACD_Signal'), v('MACD_Hist')
    bb_pct, bb_upper, bb_lower = v('BB_Pct'), v('BB_Upper'), v('BB_Lower')
    vol, vol_ma = float(df['Volume'].iloc[-1]), v('Vol_MA20')
    stoch_k, stoch_d = v('Stoch_K'), v('Stoch_D')
    adx = v('ADX')
    prev_close = float(df['Close'].iloc[-2]) if len(df) >= 2 else close

    if sma20:
        t = 'bullish' if close > sma20 else 'bearish'
        signals.append({'name': 'SMA 20', 'value': f'${close:.2f} {"above" if t=="bullish" else "below"} ${sma20:.2f}', 'type': t, 'weight': 10})
    if sma50:
        t = 'bullish' if close > sma50 else 'bearish'
        signals.append({'name': 'SMA 50', 'value': f'${close:.2f} {"above" if t=="bullish" else "below"} ${sma50:.2f}', 'type': t, 'weight': 15})
    if sma200:
        t = 'bullish' if close > sma200 else 'bearish'
        signals.append({'name': 'SMA 200', 'value': f'${close:.2f} {"above" if t=="bullish" else "below"} ${sma200:.2f}', 'type': t, 'weight': 20})

    if rsi is not None:
        if rsi > 70:
            signals.append({'name': 'RSI (14)', 'value': f'{rsi:.1f} — Overbought', 'type': 'bearish', 'weight': 12})
        elif rsi < 30:
            signals.append({'name': 'RSI (14)', 'value': f'{rsi:.1f} — Oversold', 'type': 'bullish', 'weight': 12})
        elif rsi > 55:
            signals.append({'name': 'RSI (14)', 'value': f'{rsi:.1f} — Bullish zone', 'type': 'bullish', 'weight': 7})
        elif rsi < 45:
            signals.append({'name': 'RSI (14)', 'value': f'{rsi:.1f} — Bearish zone', 'type': 'bearish', 'weight': 7})
        else:
            signals.append({'name': 'RSI (14)', 'value': f'{rsi:.1f} — Neutral', 'type': 'neutral', 'weight': 0})

    if macd is not None and macd_sig is not None:
        if macd > macd_sig and macd_hist and macd_hist > 0:
            signals.append({'name': 'MACD', 'value': f'Bullish ({macd:.3f} > {macd_sig:.3f})', 'type': 'bullish', 'weight': 15})
        elif macd < macd_sig and macd_hist and macd_hist < 0:
            signals.append({'name': 'MACD', 'value': f'Bearish ({macd:.3f} < {macd_sig:.3f})', 'type': 'bearish', 'weight': 15})
        else:
            signals.append({'name': 'MACD', 'value': f'Converging ({macd:.3f})', 'type': 'neutral', 'weight': 0})

    if bb_pct is not None:
        if bb_pct > 0.85:
            signals.append({'name': 'Bollinger Bands', 'value': f'Near upper band (${bb_upper:.2f})', 'type': 'bearish', 'weight': 8})
        elif bb_pct < 0.15:
            signals.append({'name': 'Bollinger Bands', 'value': f'Near lower band (${bb_lower:.2f})', 'type': 'bullish', 'weight': 8})
        elif bb_pct > 0.5:
            signals.append({'name': 'Bollinger Bands', 'value': f'Upper half ({bb_pct*100:.0f}%)', 'type': 'bullish', 'weight': 4})
        else:
            signals.append({'name': 'Bollinger Bands', 'value': f'Lower half ({bb_pct*100:.0f}%)', 'type': 'bearish', 'weight': 4})

    if vol_ma and vol_ma > 0:
        vr = vol / vol_ma
        if vr > 1.5 and close > prev_close:
            signals.append({'name': 'Volume', 'value': f'{vr:.1f}x avg — Strong buying', 'type': 'bullish', 'weight': 10})
        elif vr > 1.5 and close < prev_close:
            signals.append({'name': 'Volume', 'value': f'{vr:.1f}x avg — Strong selling', 'type': 'bearish', 'weight': 10})
        elif vr < 0.5:
            signals.append({'name': 'Volume', 'value': f'{vr:.1f}x avg — Low interest', 'type': 'neutral', 'weight': 0})
        else:
            signals.append({'name': 'Volume', 'value': f'{vr:.1f}x avg — Normal', 'type': 'neutral', 'weight': 0})

    if stoch_k is not None:
        if stoch_k < 20:
            signals.append({'name': 'Stochastic', 'value': f'K={stoch_k:.0f} — Oversold', 'type': 'bullish', 'weight': 8})
        elif stoch_k > 80:
            signals.append({'name': 'Stochastic', 'value': f'K={stoch_k:.0f} — Overbought', 'type': 'bearish', 'weight': 8})
        elif stoch_k is not None and stoch_d is not None and stoch_k > stoch_d:
            signals.append({'name': 'Stochastic', 'value': f'K={stoch_k:.0f} crossing up', 'type': 'bullish', 'weight': 4})
        else:
            signals.append({'name': 'Stochastic', 'value': f'K={stoch_k:.0f} crossing down', 'type': 'bearish', 'weight': 4})

    if adx is not None:
        if adx > 40:
            label = 'Very Strong Trend'
        elif adx > 25:
            label = 'Trending'
        else:
            label = 'Weak/Ranging'
        signals.append({'name': 'ADX Trend', 'value': f'{adx:.1f} — {label}', 'type': 'neutral', 'weight': 0})

    return signals


def score_technical(patterns, signals):
    score = 0
    max_score = 0
    for s in signals:
        w = s['weight']
        if s['type'] == 'bullish':
            score += w
        elif s['type'] == 'bearish':
            score -= w
        max_score += w
    for p in patterns:
        bonus = 15 if p['strength'] == 'strong' else 8
        if p['type'] == 'bullish':
            score += bonus
        elif p['type'] == 'bearish':
            score -= bonus
        if p['type'] != 'neutral':
            max_score += bonus
    if max_score == 0:
        return 0
    return round(max(-100, min(100, (score / max_score) * 100)))


def compute_atr_percentile(df):
    """Returns current ATR's percentile rank vs its 1-year history (0-100)."""
    atr_series = df['ATR'].dropna()
    if len(atr_series) < 20:
        return 50.0
    current_atr = float(atr_series.iloc[-1])
    return round(float((atr_series < current_atr).mean() * 100), 1)


def find_support_resistance(df):
    close = df['Close'].values
    high = df['High'].values
    low = df['Low'].values
    n = len(close)
    current = close[-1]

    # Multi-timeframe pivot detection: windows 3, 5, 10, 20
    raw_levels = {}

    for window in [3, 5, 10, 20]:
        for i in range(window, n - window):
            if high[i] == max(high[i - window: i + window + 1]):
                price = float(high[i])
                if abs(price - current) / current < 0.25:
                    key = round(price, 2)
                    if key not in raw_levels:
                        raw_levels[key] = {'price': price, 'count': 0,
                                           'type': 'resistance' if price > current else 'support'}
                    raw_levels[key]['count'] += 1
            if low[i] == min(low[i - window: i + window + 1]):
                price = float(low[i])
                if abs(price - current) / current < 0.25:
                    key = round(price, 2)
                    if key not in raw_levels:
                        raw_levels[key] = {'price': price, 'count': 0,
                                           'type': 'support' if price < current else 'resistance'}
                    raw_levels[key]['count'] += 1

    # Cluster nearby levels within 0.5%
    sorted_levels = sorted(raw_levels.values(), key=lambda x: x['price'])
    clustered = []
    for lv in sorted_levels:
        if not clustered or abs(lv['price'] - clustered[-1]['price']) / (clustered[-1]['price'] + 1e-9) > 0.005:
            clustered.append({'price': round(lv['price'], 2), 'count': lv['count'], 'type': lv['type']})
        else:
            prev = clustered[-1]
            total = prev['count'] + lv['count']
            prev['price'] = round((prev['price'] * prev['count'] + lv['price'] * lv['count']) / total, 2)
            prev['count'] = total

    # Add Moving Average levels
    for col, label in [('SMA20', 'SMA 20'), ('SMA50', 'SMA 50'), ('SMA200', 'SMA 200')]:
        val = df[col].iloc[-1]
        if not pd.isna(val):
            price = round(float(val), 2)
            if abs(price - current) / current < 0.25:
                clustered.append({
                    'price': price, 'count': 3, 'label': label,
                    'type': 'support' if price < current else 'resistance',
                })

    # Assign strength labels based on confirmation count
    for lv in clustered:
        c = lv['count']
        lv['strength'] = 'Very Strong' if c >= 8 else 'Strong' if c >= 4 else 'Moderate' if c >= 2 else 'Weak'

    clustered.sort(key=lambda x: abs(x['price'] - current))
    return clustered[:15]
