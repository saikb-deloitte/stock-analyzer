# 🆓 Deploying Centaur Prism on Render (Truly Free)

**Render's free tier = truly free, no credit card required, no expiring credits.**
Your app stays live forever as long as you don't exceed 750 hours/month
(plenty for a personal project).

The one catch: free instances **sleep after 15 min of no traffic**. First request
after sleep takes ~30 seconds to wake up. We fix that below with UptimeRobot.

Total setup time: **~15 minutes**. Total cost: **₹0/month forever.**

---

## Step 1 — Push to GitHub (5 min)

If you haven't already:

```bash
cd "C:/Users/saikb/.claude/Data/Claude Data/.claude/worktrees/peaceful-bose-cf2ca0"
git add .
git commit -m "Centaur Prism — Render-ready"
git push stock-analyzer claude/peaceful-bose-cf2ca0:centaur-prism
```

---

## Step 2 — Connect Render (5 min)

1. Go to [render.com](https://render.com) → sign in with GitHub (free, no card needed)
2. Click **"New +"** → **"Blueprint"**
3. Pick your `stock-analyzer` repo → branch `centaur-prism`
4. Render auto-detects `render.yaml` → click **"Apply"**
5. Wait ~3–5 minutes for first build

That's it. You get a free URL like `centaur-prism.onrender.com`.

> 💡 Your `render.yaml` already sets `PUBLIC_MODE=true` and auto-generates a
> `FLASK_SECRET`, so no manual env-var setup needed.

---

## Step 3 — Test it works

Visit your `*.onrender.com` URL. You'll see:
- Centaur Prism dashboard loads
- Educational disclaimer bar at top
- Freshness badge updates
- All views (Screener, Analyze, Watchlist, Compare, Picks) work

---

## Step 4 — Kill the cold start with UptimeRobot (5 min, free)

Without this, your app sleeps after 15 min idle → first visitor waits 30 sec.
With this, your app stays warm 24/7 → instant for every visitor.

### How:
1. Sign up at [uptimerobot.com](https://uptimerobot.com) (free, no card)
2. Click **"+ Add New Monitor"**
3. Settings:
   - **Monitor Type**: HTTP(s)
   - **Friendly Name**: Centaur Prism (Render)
   - **URL**: `https://centaur-prism.onrender.com/api/freshness`
   - **Monitoring Interval**: 5 minutes (free tier allows 5-min minimum)
4. Click **"Create Monitor"**

UptimeRobot now pings your `/api/freshness` endpoint every 5 minutes. The endpoint
is super lightweight (just returns a timestamp), so it barely uses any Render
hours but keeps the instance warm.

### Result:
- App stays awake 24/7
- Zero cold starts for your visitors
- Free Render hours stay well under 750/month limit
- You also get free uptime monitoring as a side benefit (email alerts if app dies)

---

## What you get with this setup

| Item | Value |
|---|---|
| Monthly cost | **₹0** |
| Cold starts | **None** (UptimeRobot keeps warm) |
| Custom domain support | ✅ Free (add yours later) |
| Auto SSL/HTTPS | ✅ Auto-issued |
| Build on git push | ✅ Auto-redeploys when you push to GitHub |
| Persistent SQLite (Prism Archive) | ⚠️ See note below |

---

## ⚠️ One important Render free-tier caveat

**The free tier has ephemeral disk.** That means your `.data_cache/` (bhavcopies,
sector history) and `prism_archive.db` (Prism Archive) **reset every time
Render restarts your instance** (which happens on every deploy + occasionally
on the free tier).

### What this means in practice:
- **Bhavcopy** — Refetches automatically on first use. ~3 sec to redownload. No real issue.
- **Sector history** — Resets, will rebuild over a few days as you use the app.
- **Prism Archive** — ⚠ This is the one to think about. Old prisms disappear on restart.

### Fix for Prism Archive (3 options):

**Option A: Accept it.** Free tier = ephemeral. If you care about archive longevity, this is fine for the first few weeks while you test.

**Option B: Use Render's $5/month disk add-on.** Persistent 1GB disk → archive survives restarts. Removes the free-forever benefit, but cheap.

**Option C: Migrate to Supabase free tier (Postgres) for archive storage.** ~30 min code change. Free Postgres up to 500MB. I can write the migration if you want.

For your first launch, **Option A is fine** — you're testing, not running production.

---

## Custom domain (optional, later)

Once you've got `centaur-prism.onrender.com` working:

1. Buy `centaurprism.in` (~₹600/year on Namecheap/GoDaddy)
2. Render dashboard → your service → **Settings** → **Custom Domain** → add it
3. Render shows you DNS records to add at your registrar
4. Wait 5–30 min for DNS, then auto-SSL kicks in

Add to your Instagram bio: `🔺 centaurprism.in`

---

## Monitoring

### Logs
Render dashboard → your service → **Logs** tab. Real-time.

### Usage
Render dashboard → your service → **Metrics** tab. Shows monthly hours used.

### Common issues

**App returns 502 / "Bad Gateway"**
Usually means gunicorn workers died (out of memory on free tier's 512MB).
Fix: edit `Procfile`, change `--workers 2` to `--workers 1` and redeploy.

**yfinance returns "Too Many Requests"**
Yahoo throttles cloud IPs. Already mitigated in `app.py` (15-min cache,
4 concurrent workers). If it gets worse, increase `CACHE_TTL` to 1800 (30 min).

**"ALL NSE" scan times out**
Free tier has a 60-second timeout. Already mitigated (default cap is 150
stocks on public deploys). If you still see timeouts, lower the cap further
via the screener dropdown.

---

## Render vs Railway vs Fly — honest take

| | Render (free) | Railway ($5/mo) | Fly.io (free, Mumbai) |
|---|---|---|---|
| Monthly cost | ₹0 | ~₹400 | ₹0 |
| Setup time | 15 min | 10 min | 30 min |
| Latency for Indian users | ~270ms (US/EU) | ~270ms | **~25ms** |
| Cold starts | 30s (fix: UptimeRobot) | None | None |
| Persistent storage | Ephemeral on free tier | Yes | Yes |
| Credit card required | **No** | Yes | Yes |

**If "no credit card, no monthly bill" is the priority → Render is the right choice.**
**If "best UX for Indian audience" is the priority → Fly.io Mumbai.**

You can always migrate later. The same `Procfile` works on all 3 hosts.

---

## Reverting

To take the app offline:
- Render dashboard → **Settings** → **Delete Service**
- Local app on your machine is untouched
