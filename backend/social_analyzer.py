"""
Social sentiment via Reddit (multiple subreddits) + news-volume proxy.
StockTwits API is no longer publicly accessible (OAuth required since 2024).
"""
import requests

_REDDIT_HEADERS = {"User-Agent": "StockSentimentBot/1.0 (educational research)"}

_POS = {"bullish","bull","moon","calls","long","buy","squeeze","breakout","rocket",
        "gain","gains","green","pump","rip","runner","explosive","massive","huge",
        "uptrend","oversold","accumulate","accumulating","strong","support"}
_NEG = {"bearish","bear","puts","short","sell","crash","dump","red","drop","tank",
        "loss","losses","avoid","worthless","dead","dilution","diluting","bankrupt",
        "downtrend","overbought","resist","resistance","overvalued","fraud","scam"}

_SUBREDDITS = ["wallstreetbets", "stocks", "StockMarket", "investing", "pennystocks"]


def _reddit_search(ticker, subreddit, timeframe="week"):
    """Search a single subreddit for ticker mentions. Returns list of post dicts."""
    try:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
        # sort=relevance + t=week → recent & relevant posts, not viral old ones
        params = {"q": ticker, "restrict_sr": "on", "sort": "relevance", "t": timeframe, "limit": 25}
        r = requests.get(url, params=params, headers=_REDDIT_HEADERS, timeout=8)
        if r.status_code != 200:
            return []
        return r.json().get("data", {}).get("children", [])
    except Exception:
        return []


def _score_text(text):
    words = set(text.lower().split())
    return len(words & _POS), len(words & _NEG)


def get_reddit_sentiment(ticker):
    """Search multiple subreddits for ticker mentions and compute sentiment."""
    ticker_variants = {ticker.lower(), f"${ticker.lower()}"}
    all_posts = []

    for sub in _SUBREDDITS:
        raw = _reddit_search(ticker, sub, timeframe="month")
        for p in raw:
            d = p.get("data", {})
            title = d.get("title", "")
            body = d.get("selftext", "")[:300]
            combined = (title + " " + body).lower()
            # Must contain the ticker symbol to count as a mention
            if not any(v in combined for v in ticker_variants):
                continue
            ps, ns = _score_text(title)
            all_posts.append({
                "title": title[:180],
                "subreddit": sub,
                "score": d.get("score", 0),
                "pos": ps,
                "neg": ns,
                "url": d.get("url", ""),
            })

    if not all_posts:
        return None

    total_pos = sum(p["pos"] for p in all_posts)
    total_neg = sum(p["neg"] for p in all_posts)
    total_scored = total_pos + total_neg

    sentiment_score = round((total_pos - total_neg) / total_scored * 100) if total_scored > 0 else 0
    bull_pct = round(total_pos / total_scored * 100, 1) if total_scored > 0 else 50.0
    bear_pct = round(total_neg / total_scored * 100, 1) if total_scored > 0 else 50.0

    # Top posts by upvotes for display
    top = sorted(all_posts, key=lambda x: x["score"], reverse=True)[:6]
    posts_display = []
    for p in top:
        sent = "Bullish" if p["pos"] > p["neg"] else "Bearish" if p["neg"] > p["pos"] else "Neutral"
        posts_display.append({
            "text": p["title"],
            "sentiment": sent,
            "user": f"r/{p['subreddit']}  ·  {p['score']} pts",
            "created_at": "",
        })

    return {
        "source": "Reddit",
        "score": sentiment_score,
        "bullish_pct": bull_pct,
        "bearish_pct": bear_pct,
        "message_count": len(all_posts),
        "scored_count": len(all_posts),
        "subreddits_searched": _SUBREDDITS,
        "posts": posts_display,
    }


def _news_volume_sentiment(ticker):
    """
    Proxy social sentiment from Yahoo Finance news volume.
    More articles = more attention. Recent negative/positive article ratio
    approximates community sentiment when no social data is available.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        news = t.news or []
        if not news:
            return None

        count = len(news)
        if count == 0:
            return None

        # Use FinBERT or keyword scores from news_analyzer if available
        # Here we just use count as an attention signal
        # Score: high article volume = attention (neutral by default)
        # This tells us HOW MUCH buzz there is, not direction
        attention = min(100, count * 10)  # 10 articles → score 100

        return {
            "source": "News Volume",
            "score": 0,           # direction-neutral
            "bullish_pct": 50.0,
            "bearish_pct": 50.0,
            "message_count": count,
            "scored_count": 0,
            "attention_score": attention,
            "posts": [{"text": n.get("title","")[:180], "sentiment": "Neutral",
                       "user": n.get("publisher",""), "created_at": ""}
                      for n in news[:4]],
        }
    except Exception:
        return None


def get_social_sentiment(ticker):
    """
    Returns combined social sentiment from Reddit + news volume proxy.
    StockTwits API was deprecated for public access in 2024.
    """
    reddit = get_reddit_sentiment(ticker)
    news_vol = _news_volume_sentiment(ticker)

    sources = [x for x in [reddit, news_vol] if x is not None]
    if not sources:
        return {"score": 0, "available": False, "total_mentions": 0, "sources": []}

    # Weight composite score by mention count (news_vol has scored_count=0, so it doesn't dilute)
    scored_sources = [s for s in sources if s["scored_count"] > 0]
    total_scored = sum(s["scored_count"] for s in scored_sources)
    avg_score = (
        round(sum(s["score"] * s["scored_count"] for s in scored_sources) / total_scored)
        if total_scored > 0 else 0
    )
    total_mentions = sum(s["message_count"] for s in sources)

    return {
        "score": avg_score,
        "available": True,
        "total_mentions": total_mentions,
        "sources": sources,
    }
