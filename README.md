# NSE Stock Analyzer

A self-hosted web app for analyzing NSE stocks with technical indicators, fundamentals, chart patterns, market news, and algorithmic short-term / mid-term trade plans (entry, stop-loss, targets, risk).

---

## Quick Start (this machine, already working)

Double-click `start.bat` (Windows) or run `./start.sh` (macOS / Linux).
The browser opens automatically at http://localhost:5001.

To stop: press `Ctrl + C` in the console window.

---

## Option A — Run on Another Machine (Recommended for Personal Daily Use)

**Best if:** you want a private, free, offline-capable tool that runs entirely on your own PC. No internet needed except for yfinance to fetch data.

### One-time setup on the other PC

1. **Install Python 3.10 or newer**
   - Windows: download from https://python.org/downloads
     → **Important:** tick **"Add Python to PATH"** during install.
   - macOS: `brew install python` (or download from python.org)
   - Linux: `sudo apt install python3 python3-pip`

2. **Copy the project folder** to the new PC. Options:
   - USB stick / external drive
   - Email yourself a zip
   - Cloud sync: OneDrive / Google Drive / Dropbox
   - Git: `git clone <your-repo-url>` if you push it to GitHub

3. **First run** — double-click `start.bat` (Windows) or `./start.sh` (Mac/Linux).
   The first launch installs dependencies (~2 min). Subsequent launches are instant.

### Daily usage

- Double-click `start.bat` → browser opens automatically → analyze stocks → close the window when done.
- No internet beyond Yahoo Finance API calls is required.

### Make it even faster — pin to taskbar

- **Windows:** right-click `start.bat` → "Create shortcut" → drag the shortcut to your Desktop or pin to taskbar.
- **Mac:** drag `start.sh` to the Dock (you may need to `chmod +x start.sh` once).

---

## Option B — Free Cloud Hosting (Access from Anywhere)

**Best if:** you want to analyze stocks from your phone or any random PC without installing anything.

The free tier on Render.com works well for personal use. The app sleeps after 15 min of inactivity and takes ~30s to wake up on first request — that's the trade-off for free.

### Steps

1. **Create a free GitHub account** at https://github.com if you don't have one.

2. **Push this code to GitHub:**
   ```bash
   cd "C:\Users\saikb\.claude\Data\Claude Data\.claude\worktrees\peaceful-bose-cf2ca0"
   git init
   git add .
   git commit -m "NSE Stock Analyzer"
   git branch -M main
   git remote add origin https://github.com/<your-username>/nse-analyzer.git
   git push -u origin main
   ```

3. **Sign up at https://render.com** (free, GitHub login works).

4. **Create a new Web Service:**
   - Click **"New +" → "Web Service"**
   - Connect your GitHub repo
   - Render auto-detects `render.yaml` and configures everything
   - Click **"Create Web Service"**

5. **Wait ~3 min** for the first deploy. You get a permanent URL like:
   `https://nse-analyzer-xyz.onrender.com`

6. **Bookmark that URL.** Open it on any device, anywhere — phone, tablet, laptop.

### Render free tier notes
- 750 hours/month free (enough for personal use)
- Sleeps after 15 min idle → first request takes ~30s to wake up
- 512 MB RAM, sufficient for this app
- No credit card required

### Alternative free hosts

| Host | Free Tier | Notes |
|---|---|---|
| **Render.com** | Yes, 750 hrs/mo | Recommended — config already included |
| **Railway.app** | $5 credit/mo (~500 hrs) | Faster wake-ups, also auto-deploys from GitHub |
| **PythonAnywhere** | Yes, always-on | Only works with their custom WSGI setup, takes more config |
| **Fly.io** | Yes, 3 small VMs | Requires CLI installation |

---

## Option C — Keep Server on This PC, Access from Anywhere

**Best if:** you want internet access to the app but don't want to deploy/maintain anything in the cloud. Trade-off: this PC must stay on whenever you want to use the app.

### Cloudflare Tunnel (free, no signup of payment card required)

1. **Install cloudflared:**
   ```powershell
   winget install --id Cloudflare.cloudflared
   ```

2. **Start your app** (`start.bat`) as usual.

3. **In a separate terminal, run:**
   ```powershell
   cloudflared tunnel --url http://localhost:5001
   ```

4. **Cloudflare prints a public URL** like `https://random-words.trycloudflare.com` — open it from any device.
   Closing the terminal closes the tunnel.

### Permanent URL via Cloudflare (optional)
If you want a fixed URL that doesn't change each session, you need a Cloudflare account + a domain. Skip this for occasional use.

---

## Daily Workflow

Whichever option you chose:

1. **Open the app** (double-click `start.bat`, or open the Render URL).
2. **Refresh the screener** — click "Refresh" to get fresh scores for the selected index (NIFTY 50 / Bank / IT / Pharma / Auto / Midcap).
3. **Click "Analyze" on any row** (or search a ticker) to see:
   - Score gauges (Technical, Fundamental, Short-term, Mid-term)
   - Trading Levels: entry, stop-loss, Target 1, Target 2, R:R
   - Risk panel with volatility, ATR%, beta
   - Candlestick chart with EMA20/50/200 + Bollinger Bands
   - RSI + MACD subplots
   - Tabs: Technical Signals, Fundamentals, Chart Patterns, News

### Best time to run
After NSE market close (post 3:45 PM IST) — that's when daily candles are settled and signals are cleanest. Avoid analyzing intraday because levels shift as the day progresses.

### Cache behavior
- Stock analysis results are cached for **5 minutes** to avoid hammering Yahoo Finance.
- Screener results are cached per index for **5 minutes**.
- Click the **Refresh** button to force a fresh fetch.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `Python is not installed or not in PATH` | Re-install Python and tick "Add to PATH". On Windows, restart your terminal. |
| Screener stuck "Fetching..." | Yahoo Finance is rate-limiting. Wait 60s and refresh. |
| `No data found for XYZ.NS` | Ticker may be delisted or wrong. Try the BSE suffix: search `XYZ.BO`. |
| Port 5001 already in use | Edit `run.py` and change `port=5001` to e.g. `port=5002`. |
| Browser doesn't open automatically | Manually visit http://localhost:5001 |
| Charts show "NaN" or missing | Refresh hard with Ctrl+Shift+R; clear cache. |
| Render deploy fails | Check the build log — usually a missing dep. Confirm `requirements.txt` is committed. |

---

## File structure

```
nse-analyzer/
├── app.py                # Flask backend + analysis engine
├── run.py                # Local launcher
├── start.bat             # Windows one-click run
├── start.sh              # macOS / Linux one-click run
├── requirements.txt      # Python dependencies
├── Procfile              # Cloud deploy entry (Render / Heroku)
├── render.yaml           # Render.com config
├── templates/
│   └── index.html        # Full UI (HTML + CSS + JS inline)
└── README.md             # This file
```

---

## Updating the stock universe

Edit `app.py` → find `NSE_INDICES = { ... }` near the top. Add/remove tickers (use the `.NS` suffix for NSE, `.BO` for BSE).

To find a ticker, search the company on https://finance.yahoo.com — the URL shows the symbol.

---

## Disclaimer

This tool provides algorithmic analysis for educational purposes only. The technical scores, trading levels, and risk assessments are computed from historical price data and do not constitute investment advice. Always combine with your own due diligence, position-sizing rules, and risk management. Past performance doesn't predict future results.
