# 📸 Daily Data Snapshot System

A **parallel, independent** data pipeline that runs alongside (but never touches)
the live Render/Fly app.

## What it does

Every day at **09:30 IST**, a GitHub Actions cron job:

1. Spins up an Ubuntu runner
2. Installs Python + dependencies
3. Runs `scripts/build_snapshot.py --all`
4. Generates JSON snapshots for NIFTY 50 + Bank + IT + Pharma + Auto + Midcap
5. Builds a long-term picks snapshot
6. Commits the JSON files to `static/data/`
7. Pushes back to the `Centaur-prism` branch

Result: fresh, daily-refreshed market data lives at:

```
static/data/snapshot_nifty_50.json
static/data/snapshot_nifty_bank.json
static/data/snapshot_nifty_it.json
static/data/snapshot_nifty_pharma.json
static/data/snapshot_nifty_auto.json
static/data/snapshot_nifty_midcap.json
static/data/snapshot_picks_all.json
static/data/manifest.json          ← list of everything
```

## Why this is a separate system

The existing live app (Render + Fly) keeps running unchanged. yfinance from
those cloud IPs is unreliable, causing intermittent 502s.

GitHub Actions runners are NOT rate-limited by Yahoo, so yfinance just works
there. Running the screener as a scheduled batch job sidesteps the problem
entirely — without changing the live app's architecture.

## How to use the static JSON files

Three ways, in order of complexity:

### A — Direct file access (zero code change)

The Flask app already serves anything under `static/`. Open in browser:

```
https://centaur-prism.fly.dev/static/data/snapshot_nifty_50.json
https://centaur-prism.onrender.com/static/data/snapshot_nifty_50.json
```

Or via raw GitHub (no app deployment needed):

```
https://raw.githubusercontent.com/saikb-deloitte/stock-analyzer/Centaur-prism/static/data/snapshot_nifty_50.json
```

### B — Frontend fallback (optional follow-up)

Add a few lines to `templates/index.html`'s `loadScreener()` to fall back to
the static snapshot when the live API fails:

```js
const data = await fetchJSON('/api/screener?index=' + idx).catch(async (e) => {
  console.warn('Live screener failed, falling back to snapshot:', e.message);
  const snap = await fetchJSON('/static/data/snapshot_' + slug(idx) + '.json');
  return snap.stocks;
});
```

### C — Separate static-only viewer (future)

Build a tiny new page (e.g., `/snapshot`) that reads only the JSON files,
no Python/yfinance involved at all. Useful as a guaranteed-available view.

## Running it manually

From your local machine (with yfinance working):

```powershell
cd "C:\Users\saikb\.claude\Data\Claude Data\.claude\worktrees\peaceful-bose-cf2ca0"

# Just NIFTY 50 (fastest, ~1 min)
python scripts/build_snapshot.py

# All common indices (~5 min)
python scripts/build_snapshot.py --all

# Specific indices
python scripts/build_snapshot.py --indices "NIFTY Bank,NIFTY IT"
```

The JSON files appear in `static/data/`. Commit and push to ship to production.

## Triggering the GitHub Action manually

In GitHub:

1. Go to **Actions** tab
2. Click **"Daily data snapshot"** in the left sidebar
3. Click **"Run workflow"** dropdown on the right
4. Pick branch `Centaur-prism` → click **Run workflow**

The run takes ~5-10 minutes. New JSON files commit automatically.

## What happens to live deploys when the bot commits?

- **Fly.io**: ✅ Smart — `fly-deploy.yml` is configured to ignore changes under
  `static/data/**`, so bot commits don't trigger a redeploy.
- **Render**: ⚠️ Render auto-deploys on every push regardless. Each daily
  snapshot commit causes Render to redeploy (~5 min). If this becomes
  annoying, options:
    1. Disable auto-deploy in Render settings (manual deploys only)
    2. Use Render's "skip CI" comment magic: `[skip render]` in commit message
    3. Just accept the 5-min daily refresh window

## Cost

- GitHub Actions: ~5-10 min/day = well within the 2000 free min/month limit
- Storage: ~150 KB per day of JSON committed — negligible
- Cron infrastructure: free

## Disabling

If you want to pause the daily snapshots:

1. GitHub → Actions tab
2. Click **"Daily data snapshot"** workflow
3. Click `...` menu (top right) → **"Disable workflow"**

The static JSON files remain in the repo; the cron just stops adding new ones.

## File structure created

```
scripts/
├── build_snapshot.py          ← the data-builder
└── SNAPSHOT_README.md         ← this file

.github/workflows/
└── data-snapshot.yml          ← the daily cron

static/data/
├── snapshot_nifty_50.json     ← created by cron
├── snapshot_nifty_bank.json
├── snapshot_nifty_it.json
├── snapshot_picks_all.json
└── manifest.json              ← index of all snapshots
```

## What is NOT changed by this system

| File | Status |
|---|---|
| `app.py` | ❌ NOT touched |
| `templates/index.html` | ❌ NOT touched |
| `Procfile` | ❌ NOT touched |
| `Dockerfile` | ❌ NOT touched |
| `fly.toml` | ❌ NOT touched |
| `render.yaml` | ❌ NOT touched |
| Live URLs | ❌ NOT changed |
| User experience | ❌ NOT changed |

The system is purely additive. You can ignore it forever and the live app
keeps working exactly as before.
