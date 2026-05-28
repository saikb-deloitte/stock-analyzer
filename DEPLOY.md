# 🚀 Deploying Centaur Prism to Railway

This guide takes you from local-only to a public URL anyone can use,
in **~25 minutes**, on Railway's free tier.

---

## Why Railway

| | Railway | Render | Fly.io |
|---|---|---|---|
| Free tier | $5/month credit (~500 hrs Hobby) | 750 hrs/month | 3 small VMs free |
| Cold-start time | ~3 sec | ~30 sec on free tier | ~5 sec |
| Setup steps | 4 | 5 | 8 |
| Auto SSL + custom domain | ✅ | ✅ | ✅ |

Railway wins for this stack.

---

## Pre-flight checklist (do this before deploying)

- [ ] You have a **GitHub account** (free is fine)
- [ ] You have **git installed** on your machine (`git --version`)
- [ ] You've **never committed** `.env`, secrets, or `.data_cache/` to a repo

---

## Step 1 — Push to GitHub (5 min)

```bash
cd "C:/Users/saikb/.claude/Data/Claude Data/.claude/worktrees/peaceful-bose-cf2ca0"

# Initialize repo (skip if already initialized)
git init
git add .
git status   # ← sanity-check that .data_cache, reel_audio, etc. are NOT staged
git commit -m "Initial Centaur Prism commit"

# Create a new private repo on github.com first, then:
git branch -M main
git remote add origin https://github.com/<your-username>/centaur-prism.git
git push -u origin main
```

If `git status` shows files you don't want public (audio files, .data_cache, post HTMLs),
double-check `.gitignore` and run `git rm --cached <file>` to unstage them.

---

## Step 2 — Connect Railway (5 min)

1. Go to [railway.app](https://railway.app) → sign in with GitHub
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Authorize Railway to read your repo list, then pick **`centaur-prism`**
4. Railway auto-detects Python from `requirements.txt` + `Procfile`
5. **Wait ~3 min** for the first build

---

## Step 3 — Set environment variables (1 min)

In the Railway dashboard for your project:

1. Click **Variables** (left sidebar)
2. Add these:

| Name | Value | Why |
|---|---|---|
| `PUBLIC_MODE` | `true` | Enables rate limiting + session-scoped archives |
| `FLASK_SECRET` | *(click "generate" or paste a random 32-char string)* | Signs session cookies securely |
| `PORT` | *leave unset* | Railway provides this automatically |

3. Click **"Deploy"** → wait ~1 min for restart

---

## Step 4 — Get your URL (instant)

In the Railway dashboard:
- Click **Settings** → **Networking** → **"Generate Domain"**
- You get a free URL like `centaur-prism-production.up.railway.app`

Visit it. You should see the full Centaur Prism dashboard. 🎉

---

## Step 5 (optional) — Custom domain (~₹600/year + 10 min)

If you want `centaurprism.in` or `prism.centaur.io`:

1. Buy the domain from Namecheap / GoDaddy / Cloudflare
2. In Railway → **Settings** → **Custom Domain** → add your domain
3. Railway shows you a `CNAME` record to add at your domain registrar
4. Wait 5–30 min for DNS to propagate
5. Railway auto-issues a Let's Encrypt SSL cert

Add the URL to your Instagram bio.

---

## After deploy — monitoring

### Logs
Railway dashboard → **Deployments** → click latest → **View Logs**

You'll see every request, errors, and the rate-limiter rejections (helpful to spot
abuse).

### Costs
Railway dashboard → **Usage**. Free $5 covers ~500 hours of always-on Hobby instance.
A side-project Flask app usually stays well under that. If it grows past free:

- Upgrade to Hobby ($5/month) — same plan, just no free credit limit
- Add Redis (~$5/month) if you want persistent rate-limit + cache state

---

## What gets deployed vs. stays local

### Goes to GitHub → Railway
- `app.py`, `run.py`, `requirements.txt`, `Procfile`, `runtime.txt`
- `templates/`, `static/` (if present)
- `start.bat`, `start.sh`
- Your README, this DEPLOY.md

### Stays on your machine only (excluded by `.gitignore`)
- `.data_cache/` (regenerated on Railway as the app runs)
- All `centaur_prism_post*.html` brand/content files
- `reel_audio/` voiceover files
- `portable_build/` distribution
- `.env`, `.venv`, build artifacts

### What happens to the Prism Archive?
- **Locally**: your `local` session keeps accumulating prisms
- **On Railway**: each browser/visitor gets their own session cookie → private archive per user
- **Your local archive does NOT sync to Railway** (and shouldn't — it would mix with public users)

---

## Troubleshooting

### Build fails with `ModuleNotFoundError: flask_limiter`
Make sure `flask-limiter>=3.5.0` is in `requirements.txt`. Re-deploy.

### App returns 502 / "Application failed to respond"
Check **Deployments → View Logs**. Most common: the gunicorn worker died.
- Try increasing the workers in `Procfile` from `--workers 2` to `--workers 1` (less memory)
- Or upgrade Railway tier if memory is the issue

### yfinance returns "Too Many Requests" errors
Yahoo rate-limits cloud IPs aggressively. Options:
- Reduce screener concurrency: edit `app.py`, find `ThreadPoolExecutor(max_workers=8)` → change to `4`
- Increase `CACHE_TTL` from 300 to 900 (15 min) — fewer Yahoo hits per user
- Use the NSE bhavcopy fallback more aggressively (already wired in)

### "ALL NSE" scan times out on Railway
Railway free tier has a 30-second request timeout. Workarounds:
- Lower `max_stocks` from 300 to 150 in `/api/screener` ALL NSE branch
- Or upgrade to Hobby tier ($5/month) which has no timeout

---

## Reverting / pulling Railway down

To take the app offline:
- Railway dashboard → **Settings** → **Danger Zone** → **Remove**
- Or: just delete the GitHub repo connection. Railway stops deploying.

The app on your local machine is untouched.

---

## Educational disclaimer on the public site

The in-app banner already covers this, but if you want to add an explicit
"About" / "Disclaimer" page later, add it via a new route in `app.py`
(e.g. `/about`) and link from the footer.

For Indian audience compliance, the language should make clear:
- Not SEBI-registered investment advice
- Educational analysis tool
- Author retains no fiduciary duty
- Users assume all risk
