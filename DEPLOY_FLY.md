# ✈️ Deploying Centaur Prism on Fly.io (Mumbai region)

**Why Fly.io Mumbai is the right move for you:**
- 🇮🇳 Mumbai datacenter (`bom`) → **~25ms latency** for Indian audience (vs ~270ms from US/EU hosts)
- 💰 **Effectively free** for personal/portfolio traffic — machine auto-stops when idle, $0 charges
- 💪 More headroom than Render free tier — same 512MB RAM but with proper resource isolation
- 🚀 **No cold-start sleep** like Render (3-sec wake on idle-stop vs 30-sec on Render free)

Total setup time: **~25 minutes**

---

## ✅ Pre-flight checklist

- [ ] Your code is pushed to `github.com/saikb-deloitte/stock-analyzer` branch `Centaur-prism` (done — we just did this)
- [ ] You have a credit/debit card to add to Fly (no charges if you stay within free credits — required by Fly even for free tier to prevent abuse)
- [ ] PowerShell or Command Prompt access on Windows

---

## Step 1 — Install `flyctl` (Fly's CLI) — 3 min

Open **PowerShell** (not CMD) and run:

```powershell
iwr https://fly.io/install.ps1 -useb | iex
```

Close and reopen PowerShell when it finishes (PATH needs to refresh).

Verify:
```powershell
flyctl version
```
Should print something like `fly v0.x.x ...`

---

## Step 2 — Sign up + log in — 3 min

```powershell
flyctl auth signup
```

This opens a browser. Sign in with GitHub (recommended) or email.

Once signed up, Fly will prompt you to add a payment method. **Add a card — Fly won't charge you within free credits**, but this is required.

If you already have an account:
```powershell
flyctl auth login
```

---

## Step 3 — Deploy — 8 min

In your project folder:

```powershell
cd "C:\Users\saikb\.claude\Data\Claude Data\.claude\worktrees\peaceful-bose-cf2ca0"
flyctl launch --copy-config --name centaur-prism --region bom
```

What this does:
- Reads `fly.toml` we just wrote
- Confirms app name = `centaur-prism`, region = `bom` (Mumbai)
- **Skips creating Postgres / Redis** (we don't need them) — answer **No** to any addon prompts
- Builds the Docker image from your `Dockerfile`
- Pushes to Fly's registry
- Boots a machine in Mumbai
- Shows you the deploy URL

You'll see output like:
```
✓ Setting up Postgres (skipped)
✓ Setting up Redis (skipped)
✓ Building image with Dockerfile (~3-4 min)
✓ Pushing image to fly.io registry
✓ Provisioning machine in bom region
✓ Waiting for health checks
✓ App is live at https://centaur-prism.fly.dev
```

---

## Step 4 — Set the FLASK_SECRET — 30 sec

```powershell
flyctl secrets set FLASK_SECRET=$(python -c "import secrets;print(secrets.token_urlsafe(32))")
```

This generates a random secret + sets it as an env var on Fly. The app auto-restarts to pick it up (~10 sec).

---

## Step 5 — Open your app — 10 sec

```powershell
flyctl open
```

That opens `https://centaur-prism.fly.dev` in your default browser.

Or visit directly: **https://centaur-prism.fly.dev**

You should see the full Centaur Prism dashboard. Test:
- Analyze TCS → loads in ~2 sec
- Compare SBIN vs HDFCBANK → side-by-side prisms
- Picks view → top long-term picks render
- All from Mumbai datacenter → much faster than Render

---

## Step 6 — Keep monitoring (optional)

### View logs in real-time
```powershell
flyctl logs
```

### View machine status
```powershell
flyctl status
```

### View deployed metrics
```powershell
flyctl dashboard
```

This opens the Fly web UI showing CPU, memory, requests, latency.

---

## 💰 What this actually costs

Fly.io pricing for our config:
- Machine: shared-cpu-1x, 512MB → ~$1.94/month if always-on
- BUT: `auto_stop_machines = "stop"` means it shuts down when idle
- Result: pay only for the seconds your app actually handled requests

**For personal/cousins testing**: ~$0/month
**For ~10-50 IG visitors per day**: ~$0.50/month (covered by free credits)
**For ~500 visitors/day sustained**: ~$3-5/month

You also get $5/month in free credits (post-Aug 2024 policy), which covers small apps indefinitely.

---

## 🔁 Future deploys

After the initial setup, deploying changes is one command:

```powershell
git add .
git commit -m "..."
git push stock-analyzer claude/peaceful-bose-cf2ca0:Centaur-prism

# Then deploy:
flyctl deploy
```

`flyctl deploy` rebuilds the Docker image and rolls it out in ~2 minutes.

---

## ⚙️ Configuration changes

| Want to… | Run this |
|---|---|
| Set/update an env var | `flyctl secrets set KEY=value` |
| Remove an env var | `flyctl secrets unset KEY` |
| Restart the app | `flyctl restart` |
| Upgrade RAM to 1GB | Edit `fly.toml` → `memory = "1gb"` → `flyctl deploy` |
| Add a 2nd region | `flyctl regions add sin` (Singapore) |
| Scale to 2 machines | `flyctl scale count 2` |

---

## 🚫 Decommissioning Render

After Fly.io is working, you have two URLs:
- `centaur-prism.onrender.com` (old, Render)
- `centaur-prism.fly.dev` (new, Fly Mumbai)

**Decision: keep both for ~1 week, then shut Render down.**

To shut down Render:
1. render.com → your service → **Settings** → **Suspend** (preserves data) or **Delete Service**
2. Update any IG bio / WhatsApp links to point at Fly URL
3. Update your UptimeRobot monitor to ping the Fly URL instead

---

## ⚠️ Common first-deploy issues

### Build fails with `gcc not found`
The Dockerfile installs gcc. If something else is missing, paste the build error and I'll add the apt package.

### Health check failing
The `/api/freshness` endpoint must respond within 10s. Check `flyctl logs` for crash output.

### "Image too large"
Our `.dockerignore` excludes the heavy stuff (audio, design HTMLs, portable_build). If the image is still > 500MB, paste `flyctl deploy` output for diagnosis.

### App stays in "Starting" forever
Run `flyctl logs` and look for Python tracebacks. Most common: missing env var.

### Cold start is too slow
Default cold-start is 3-5 sec. If unacceptable:
- Set `min_machines_running = 1` in `fly.toml` (machine never stops, ~$2/mo always-on)
- Combine with UptimeRobot pinging every 5 min to keep warm

---

## 🌐 Custom domain (later, optional)

Same as Render but on Fly:

```powershell
flyctl certs add centaurprism.in
```

Then add the DNS records Fly shows you at your domain registrar (Namecheap, GoDaddy, Cloudflare). Wait ~5-30 min for DNS propagation. Auto-SSL via Let's Encrypt.

---

## 📊 What's different from Render?

| | Render (where we were) | Fly.io Mumbai (new home) |
|---|---|---|
| Latency from Mumbai | ~270ms | **~25ms** |
| Cold-start delay | 30s sleep | 3s machine-start |
| RAM | 512MB | 512MB (same — can upgrade easily) |
| Persistent disk on free | No | Optional via Volumes ($0.15/GB/mo) |
| Auto-deploy from GitHub | Yes (auto) | Yes (run `flyctl deploy`) |
| Build time | 5 min | 3-4 min |
| Free tier policy | Sleeps after 15 min | Auto-stops, charges per second |

---

## Ready?

Open PowerShell and start at **Step 1** (install flyctl). Paste any errors here and I'll debug.

Most common path → `iwr | iex` → `flyctl auth signup` → `flyctl launch` → done in 25 min.
