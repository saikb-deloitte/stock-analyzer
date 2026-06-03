import yfinance as yf
import numpy as np
import time as _time

# ── Sector P/E — live via ETF with 24 h cache, fallback to hardcoded ─────────
_SECTOR_ETF = {
    'Technology': 'XLK', 'Healthcare': 'XLV', 'Consumer Cyclical': 'XLY',
    'Financial Services': 'XLF', 'Communication Services': 'XLC',
    'Consumer Defensive': 'XLP', 'Energy': 'XLE', 'Industrials': 'XLI',
    'Basic Materials': 'XLB', 'Real Estate': 'XLRE', 'Utilities': 'XLU',
}
_SECTOR_PE_CACHE: dict = {}   # sector → (pe, fetched_ts)
_SECTOR_PE_TTL = 86_400       # 24 hours


def _fetch_live_sector_pe(sector: str) -> int | None:
    """Return live trailing P/E for the sector ETF, cached for 24 h."""
    etf = _SECTOR_ETF.get(sector)
    if not etf:
        return None
    cached = _SECTOR_PE_CACHE.get(sector)
    if cached and (_time.time() - cached[1]) < _SECTOR_PE_TTL:
        return cached[0]
    try:
        pe = yf.Ticker(etf).info.get('trailingPE')
        if pe and 5 < float(pe) < 200:
            val = round(float(pe))
            _SECTOR_PE_CACHE[sector] = (val, _time.time())
            return val
    except Exception:
        pass
    return None


def get_quarterly_trend(ticker):
    """
    Returns revenue and EPS trends across the last 4 quarters.
    Detects acceleration/deceleration and beat/miss patterns.
    """
    try:
        t = yf.Ticker(ticker)
        result = {}

        # ── Revenue trend ────────────────────────────────────────
        try:
            qf = t.quarterly_financials
            if qf is not None and not qf.empty:
                rev_row = None
                for label in ['Total Revenue', 'Revenue']:
                    if label in qf.index:
                        rev_row = qf.loc[label].dropna()
                        break
                if rev_row is not None and len(rev_row) >= 3:
                    # Columns are newest-first; take up to 5 quarters
                    rev = rev_row.iloc[:5]
                    quarters = [str(d)[:10] for d in rev.index]
                    values = [float(v) for v in rev.values]
                    qoq = []
                    for i in range(len(values) - 1):
                        base = values[i + 1]
                        if base and base != 0:
                            qoq.append(round((values[i] - base) / abs(base) * 100, 1))
                        else:
                            qoq.append(None)
                    # Trend: accelerating if most recent QoQ > prior QoQ
                    trend = 'stable'
                    if len(qoq) >= 2 and qoq[0] is not None and qoq[1] is not None:
                        if qoq[0] > qoq[1] + 2:
                            trend = 'accelerating'
                        elif qoq[0] < qoq[1] - 2:
                            trend = 'decelerating'
                    result['revenue'] = {
                        'quarters': quarters[:4],
                        'values': values[:4],
                        'qoq_pct': qoq[:3],
                        'trend': trend,
                        'latest_qoq': qoq[0] if qoq else None,
                    }
        except Exception:
            pass

        # ── EPS trend (actual vs estimate) ───────────────────────
        try:
            qe = t.quarterly_earnings
            if qe is not None and not qe.empty and 'Actual' in qe.columns:
                qe = qe.sort_index(ascending=False).head(4)
                eps_rows = []
                for date, row in qe.iterrows():
                    actual = float(row['Actual']) if not np.isnan(row['Actual']) else None
                    estimate = float(row['Estimate']) if 'Estimate' in qe.columns and not np.isnan(row['Estimate']) else None
                    surprise = None
                    if actual is not None and estimate is not None and estimate != 0:
                        surprise = round((actual - estimate) / abs(estimate) * 100, 1)
                    eps_rows.append({
                        'quarter': str(date)[:10],
                        'actual': actual,
                        'estimate': estimate,
                        'surprise_pct': surprise,
                        'beat': surprise > 0 if surprise is not None else None,
                    })
                beats = sum(1 for r in eps_rows if r['beat'] is True)
                result['eps'] = {
                    'quarters': eps_rows,
                    'consecutive_beats': beats,
                    'beat_rate': round(beats / len(eps_rows) * 100) if eps_rows else 0,
                }
        except Exception:
            pass

        return result if result else None
    except Exception:
        return None


SECTOR_PE = {
    'Technology': 28, 'Healthcare': 22, 'Consumer Cyclical': 20,
    'Financial Services': 14, 'Communication Services': 22,
    'Consumer Defensive': 18, 'Energy': 12, 'Industrials': 18,
    'Basic Materials': 14, 'Real Estate': 30, 'Utilities': 17,
}


def get_fundamentals(ticker):
    t = yf.Ticker(ticker)
    try:
        info = t.info or {}
    except Exception:
        info = {}
    # Fall back to static GitHub info when Yahoo .info is blocked/stubbed on Render
    if not info or len(info) < 5 or (info.get('trailingPE') is None and info.get('profitMargins') is None):
        try:
            import sys, os
            sys.path.insert(0, os.path.dirname(__file__))
            from static_fallback import static_fetch_info
            sinfo = static_fetch_info(ticker)
            if sinfo:
                merged = dict(sinfo)
                merged.update({k: v for k, v in info.items() if v is not None})
                info = merged
        except Exception:
            pass

    sector = info.get('sector', 'Technology')
    sector_pe = _fetch_live_sector_pe(sector) or SECTOR_PE.get(sector, 20)
    current_price = info.get('currentPrice') or info.get('regularMarketPrice') or 0

    metrics = {
        'pe_ratio': info.get('trailingPE'),
        'forward_pe': info.get('forwardPE'),
        'peg_ratio': info.get('pegRatio'),
        'price_to_book': info.get('priceToBook'),
        'revenue_growth': info.get('revenueGrowth'),
        'earnings_growth': info.get('earningsGrowth'),
        'gross_margin': info.get('grossMargins'),
        'profit_margin': info.get('profitMargins'),
        'roe': info.get('returnOnEquity'),
        'roa': info.get('returnOnAssets'),
        'debt_to_equity': info.get('debtToEquity'),
        'current_ratio': info.get('currentRatio'),
        'quick_ratio': info.get('quickRatio'),
        'dividend_yield': info.get('dividendYield'),
        'market_cap': info.get('marketCap'),
        'ev_to_ebitda': info.get('enterpriseToEbitda'),
        'ev_to_revenue': info.get('enterpriseToRevenue'),
        'analyst_target': info.get('targetMeanPrice'),
        'analyst_low': info.get('targetLowPrice'),
        'analyst_high': info.get('targetHighPrice'),
        'analyst_consensus': info.get('recommendationMean'),
        'num_analysts': info.get('numberOfAnalystOpinions'),
        'short_ratio': info.get('shortRatio'),
        'short_float_pct': info.get('shortPercentOfFloat'),
        'sector': sector,
        'sector_pe': sector_pe,
        'beta': info.get('beta'),
        '52w_high': info.get('fiftyTwoWeekHigh'),
        '52w_low': info.get('fiftyTwoWeekLow'),
        'description': info.get('longBusinessSummary', '')[:300] if info.get('longBusinessSummary') else '',
    }

    signals = []
    score = 0
    max_score = 0

    def add(name, value, sig_type, pts):
        nonlocal score, max_score
        signals.append({'name': name, 'value': value, 'type': sig_type, 'weight': abs(pts)})
        score += pts
        max_score += abs(pts)

    pe = metrics['pe_ratio']
    if pe and pe > 0 and pe < 500:
        if pe < sector_pe * 0.7:
            add('P/E Ratio', f'{pe:.1f}x (sector {sector_pe}x) — Undervalued', 'bullish', 15)
        elif pe < sector_pe:
            add('P/E Ratio', f'{pe:.1f}x (sector {sector_pe}x) — Fair value', 'bullish', 8)
        elif pe > sector_pe * 1.5:
            add('P/E Ratio', f'{pe:.1f}x (sector {sector_pe}x) — Overvalued', 'bearish', -12)
        else:
            max_score += 5
            signals.append({'name': 'P/E Ratio', 'value': f'{pe:.1f}x (sector {sector_pe}x) — Neutral', 'type': 'neutral', 'weight': 0})

    peg = metrics['peg_ratio']
    if peg and peg > 0 and peg < 20:
        if peg < 1:
            add('PEG Ratio', f'{peg:.2f} — Undervalued vs growth', 'bullish', 10)
        elif peg < 2:
            signals.append({'name': 'PEG Ratio', 'value': f'{peg:.2f} — Fair', 'type': 'neutral', 'weight': 0})
            max_score += 5
        else:
            add('PEG Ratio', f'{peg:.2f} — Overvalued vs growth', 'bearish', -8)

    rg = metrics['revenue_growth']
    if rg is not None:
        if rg > 0.20:
            add('Revenue Growth', f'{rg*100:.1f}% YoY — Strong', 'bullish', 12)
        elif rg > 0.05:
            add('Revenue Growth', f'{rg*100:.1f}% YoY — Healthy', 'bullish', 6)
        elif rg < 0:
            add('Revenue Growth', f'{rg*100:.1f}% YoY — Declining', 'bearish', -10)
        else:
            signals.append({'name': 'Revenue Growth', 'value': f'{rg*100:.1f}% YoY — Slow', 'type': 'neutral', 'weight': 0})
            max_score += 5

    eg = metrics['earnings_growth']
    if eg is not None:
        if eg > 0.25:
            add('Earnings Growth', f'{eg*100:.1f}% YoY — Exceptional', 'bullish', 14)
        elif eg > 0.10:
            add('Earnings Growth', f'{eg*100:.1f}% YoY — Strong', 'bullish', 8)
        elif eg < -0.10:
            add('Earnings Growth', f'{eg*100:.1f}% YoY — Declining', 'bearish', -12)
        else:
            signals.append({'name': 'Earnings Growth', 'value': f'{eg*100:.1f}% YoY — Slow', 'type': 'neutral', 'weight': 0})
            max_score += 5

    pm = metrics['profit_margin']
    if pm is not None:
        if pm > 0.20:
            add('Net Margin', f'{pm*100:.1f}% — Excellent', 'bullish', 8)
        elif pm > 0.08:
            add('Net Margin', f'{pm*100:.1f}% — Good', 'bullish', 4)
        elif pm < 0:
            add('Net Margin', f'{pm*100:.1f}% — Negative', 'bearish', -8)
        else:
            signals.append({'name': 'Net Margin', 'value': f'{pm*100:.1f}% — Average', 'type': 'neutral', 'weight': 0})
            max_score += 4

    roe = metrics['roe']
    if roe is not None:
        if roe > 0.20:
            add('ROE', f'{roe*100:.1f}% — Strong', 'bullish', 8)
        elif roe > 0.10:
            add('ROE', f'{roe*100:.1f}% — Good', 'bullish', 4)
        elif roe < 0:
            add('ROE', f'{roe*100:.1f}% — Negative', 'bearish', -8)
        else:
            signals.append({'name': 'ROE', 'value': f'{roe*100:.1f}% — Average', 'type': 'neutral', 'weight': 0})
            max_score += 4

    de = metrics['debt_to_equity']
    if de is not None:
        if de < 30:
            add('Debt/Equity', f'{de:.0f}% — Low debt', 'bullish', 6)
        elif de < 100:
            signals.append({'name': 'Debt/Equity', 'value': f'{de:.0f}% — Manageable', 'type': 'neutral', 'weight': 0})
            max_score += 3
        else:
            add('Debt/Equity', f'{de:.0f}% — High debt', 'bearish', -8)

    cons = metrics['analyst_consensus']
    if cons is not None:
        if cons <= 1.5:
            add('Analyst Consensus', f'{cons:.1f}/5 — Strong Buy', 'bullish', 15)
        elif cons <= 2.5:
            add('Analyst Consensus', f'{cons:.1f}/5 — Buy', 'bullish', 10)
        elif cons <= 3.5:
            signals.append({'name': 'Analyst Consensus', 'value': f'{cons:.1f}/5 — Hold', 'type': 'neutral', 'weight': 0})
            max_score += 8
        elif cons <= 4.5:
            add('Analyst Consensus', f'{cons:.1f}/5 — Underperform', 'bearish', -10)
        else:
            add('Analyst Consensus', f'{cons:.1f}/5 — Sell', 'bearish', -15)

    tgt = metrics['analyst_target']
    if tgt and current_price:
        upside = (tgt - current_price) / current_price
        if upside > 0.20:
            add('Analyst Target', f'${tgt:.2f} (+{upside*100:.0f}% upside)', 'bullish', 10)
        elif upside > 0.05:
            add('Analyst Target', f'${tgt:.2f} (+{upside*100:.0f}% upside)', 'bullish', 5)
        elif upside < -0.10:
            add('Analyst Target', f'${tgt:.2f} ({upside*100:.0f}% downside)', 'bearish', -10)
        else:
            signals.append({'name': 'Analyst Target', 'value': f'${tgt:.2f} (±{abs(upside)*100:.0f}%)', 'type': 'neutral', 'weight': 0})
            max_score += 5

    # Short squeeze / short interest flag
    short_float = metrics['short_float_pct']
    if short_float is not None and short_float > 0:
        if short_float > 0.20:
            add('Short Float', f'{short_float*100:.1f}% of float short — Squeeze potential', 'bullish', 5)
        elif short_float > 0.10:
            signals.append({'name': 'Short Float', 'value': f'{short_float*100:.1f}% — Elevated short interest', 'type': 'neutral', 'weight': 0})
            max_score += 2

    if max_score == 0:
        return 0, metrics, signals, False

    fa_score = round(max(-100, min(100, (score / max_score) * 100)))
    # fa_available = True when at least 2 meaningful data points were scored
    fa_available = len([s for s in signals if s.get('weight', 0) > 0]) >= 2
    return fa_score, metrics, signals, fa_available
