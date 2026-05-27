from datetime import datetime
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET
import re
import requests

# ── FinBERT lazy-loader ───────────────────────────────────────────────────────
_finbert = None
_finbert_ok = None  # None=untried  True=loaded  False=failed


def _get_finbert():
    global _finbert, _finbert_ok
    if _finbert_ok is not None:
        return _finbert if _finbert_ok else None
    try:
        from transformers import pipeline
        import torch
        device = 0 if torch.cuda.is_available() else -1
        _finbert = pipeline(
            'sentiment-analysis',
            model='ProsusAI/finbert',
            device=device,
            truncation=True,
            max_length=512,
        )
        _finbert_ok = True
    except Exception:
        _finbert_ok = False
    return _finbert if _finbert_ok else None


# ── Keyword fallback ──────────────────────────────────────────────────────────
# Each entry is matched as a whole word (or phrase) to avoid false positives.
# Multi-word phrases are matched literally with \b boundaries on first/last word.

POSITIVE = [
    # Earnings / revenue beats
    'beat', 'beats', 'exceeded', 'exceeds', 'topped', 'surpassed',
    'earnings beat', 'revenue beat', 'above estimates', 'above expectations',
    # Growth & performance
    'record', 'growth', 'surge', 'surges', 'outperform', 'outperformed',
    'profit', 'expansion', 'milestone', 'breakthrough', 'positive',
    # Corporate actions
    'upgrade', 'upgraded', 'raises guidance', 'raised guidance', 'raised outlook',
    'bullish', 'strong', 'innovation', 'launch', 'launches',
    'partnership', 'acquisition', 'merger', 'buyback', 'repurchase', 'dividend',
    'wins', 'win', 'awarded', 'contract',
    # FDA / regulatory
    'fda approval', 'fda approved', 'fda clearance', 'regulatory approval',
    # Market action
    'rally', 'soars', 'jumps', 'gains', 'boost', 'upside', 'momentum',
    # Analyst
    'buy rating', 'price target raised', 'overweight', 'outperform rating',
]

NEGATIVE = [
    # Earnings / revenue misses
    'misses', 'missed', 'below estimates', 'below expectations',
    'earnings miss', 'revenue miss', 'disappoints', 'disappointing', 'shortfall',
    # Dilution / capital structure
    'dilution', 'dilutive', 'share offering', 'secondary offering',
    'at-the-market offering', 'reverse split', 'reverse stock split',
    # Going concern / distress
    'going concern', 'bankruptcy', 'default', 'insolvency', 'liquidity crisis',
    'chapter 11', 'restructuring',
    # Decline & loss
    'decline', 'declines', 'cut', 'cuts', 'downgrade', 'downgraded',
    'loss', 'losses', 'deficit', 'slump', 'falls', 'drops', 'plunges',
    'sell rating', 'price target cut', 'underweight', 'underperform',
    # Legal & regulatory
    'lawsuit', 'investigation', 'probe', 'fine', 'penalty', 'fraud', 'scandal',
    'sec investigation', 'class action', 'subpoena',
    # Operations
    'recall', 'layoff', 'layoffs', 'halt', 'suspended', 'warning', 'weak',
    'guidance cut', 'guidance lowered', 'lowered guidance', 'lowered outlook',
    # Insider / leadership
    'resign', 'resignation', 'ceo departure', 'ceo resigns',
]

# Pre-compile word-boundary regex patterns for efficiency
_POS_RE = [re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE) for kw in POSITIVE]
_NEG_RE = [re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE) for kw in NEGATIVE]


def _keyword_score(text):
    """Returns raw float in [-1, 1] using word-boundary keyword matching."""
    pos = sum(1 for pat in _POS_RE if pat.search(text))
    neg = sum(1 for pat in _NEG_RE if pat.search(text))
    if pos > neg:
        return min(pos - neg, 4) / 4.0
    elif neg > pos:
        return -min(neg - pos, 4) / 4.0
    return 0.0


def fetch_supplementary_news(ticker):
    """Fetch additional headlines from Google News RSS — free, no API key."""
    try:
        url = (f'https://news.google.com/rss/search?q={ticker}+stock'
               f'&hl=en-US&gl=US&ceid=US:en')
        r = requests.get(url, timeout=5, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)
        articles = []
        for item in root.findall('.//item')[:8]:
            title = item.findtext('title', '')
            link = item.findtext('link', '#')
            pub_str = item.findtext('pubDate', '')
            source_el = item.find('source')
            publisher = source_el.text if source_el is not None else 'Google News'
            pub_ts = 0
            if pub_str:
                try:
                    pub_ts = int(parsedate_to_datetime(pub_str).timestamp())
                except Exception:
                    pub_ts = 0
            articles.append({
                'title': title,
                'summary': '',
                'publisher': publisher,
                'providerPublishTime': pub_ts,
                'link': link,
                'thumbnail': None,
            })
        return articles
    except Exception:
        return []


def analyze_news(ticker, raw_news):
    if not raw_news:
        return 0, []

    pipe = _get_finbert()
    analyzed = []
    total_weighted = 0.0
    total_weight   = 0.0  # track sum of weights for correct denominator

    for i, article in enumerate(raw_news[:15]):
        title = article.get('title', '')
        summary = article.get('summary', article.get('description', ''))
        text_full = (title + ' ' + summary).strip()
        text_lower = text_full.lower()

        if pipe and text_full:
            try:
                out = pipe(text_full[:512])[0]
                label = out['label'].lower()
                conf = float(out['score'])
                if label == 'positive':
                    raw, sentiment = conf, 'positive'
                elif label == 'negative':
                    raw, sentiment = -conf, 'negative'
                else:
                    raw, sentiment = 0.0, 'neutral'
            except Exception:
                raw = _keyword_score(text_lower)
                sentiment = 'positive' if raw > 0 else 'negative' if raw < 0 else 'neutral'
        else:
            raw = _keyword_score(text_lower)
            sentiment = 'positive' if raw > 0 else 'negative' if raw < 0 else 'neutral'

        # Recency-weighted: article 0 = weight 1.0, article 14 = weight 0.02
        weight = max(1.0 - (i * 0.07), 0.02)
        total_weighted += raw * weight
        total_weight   += weight

        pub_ts = article.get('providerPublishTime', 0)
        pub_date = datetime.fromtimestamp(pub_ts).strftime('%b %d, %Y') if pub_ts else 'Recent'

        analyzed.append({
            'title': title,
            'publisher': article.get('publisher', 'Yahoo Finance'),
            'date': pub_date,
            'url': article.get('link', '#'),
            'sentiment': sentiment,
            'thumbnail': (article.get('thumbnail') or {}).get('resolutions', [{}])[0].get('url', '')
                         if article.get('thumbnail') else '',
        })

    if not analyzed or total_weight == 0:
        return 0, []

    # Divide by sum of weights (not article count) for correct weighted average
    avg = total_weighted / total_weight
    normalized = round(max(-100, min(100, avg * 100)))
    return normalized, analyzed
