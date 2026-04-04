# PolyBot — Polymarket Up/Down Bracket Bot

Automated bracket trading bot for Polymarket's crypto "Up or Down" short-window markets.
Targets ETH and BTC 15-minute and 1-hour markets, buying both Up and Down legs when
their combined ask price falls below a profitable threshold.

---

## Architecture

```
polybot/
├── bot/
│   ├── main.py        — Entrypoint, orchestration
│   ├── config.py      — All parameters (edit before deploying)
│   ├── scanner.py     — WebSocket price listener + bracket detector
│   ├── trader.py      — Order placement, risk controls, sim engine
│   ├── redeemer.py    — Alchemy webhook + on-chain redemption
│   └── state.py       — Shared state + metrics
├── dashboard/
│   ├── app.py         — aiohttp web server + REST API
│   └── index.html     — Single-page dashboard UI
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

**Ports:**
- `8080` — Dashboard (HTTP, access via SSH tunnel only)
- `8082` — Alchemy webhook receiver

---

## Prerequisites

- VPS running Ubuntu 22.04+ (Helsinki or closer to London)
- Docker + Docker Compose installed
- A Polygon wallet with USDC funded
- Polymarket account with API keys generated
- Alchemy account with a Polygon Mainnet app

---

## Setup

### 1. Copy files to your VPS

```bash
scp -r polybot/ user@your-vps-ip:~/polybot
ssh user@your-vps-ip
cd ~/polybot
```

### 2. Install Docker (if not already installed)

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker
```

### 3. Create your .env file

```bash
cp .env.example .env
nano .env
```

Fill in all values:
- `PRIVATE_KEY` — your Polygon wallet private key (the wallet that holds USDC)
- `FUNDER_ADDRESS` — your Polymarket proxy wallet (Settings → Profile on polymarket.com)
- `POLY_API_KEY/SECRET/PASSPHRASE` — generate at polymarket.com/settings → API
- `ALCHEMY_API_KEY` — from dashboard.alchemy.com (create a Polygon Mainnet app)
- `DASHBOARD_SECRET` — choose a strong password for the dashboard

### 4. Fund your wallet

Bridge USDC to Polygon and send to your `FUNDER_ADDRESS`:
- Use Polymarket's built-in bridge at polymarket.com
- Or bridge via https://wallet.polygon.technology

### 5. Generate Polymarket API credentials

On polymarket.com:
1. Connect your wallet
2. Go to Settings → API Keys
3. Create a new key and save all three values (key, secret, passphrase)

### 6. Configure Alchemy webhook (for fast redemptions)

1. Go to dashboard.alchemy.com → Notify → Create Webhook
2. Type: **Mined Transaction**
3. Network: Polygon Mainnet
4. Address: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` (Polymarket CTF Exchange)
5. URL: `http://YOUR_VPS_IP:8082/webhook`
6. Note: Port 8082 must be open on your VPS firewall for Alchemy

### 7. Open port 8082 only (8080 stays localhost-only)

```bash
# UFW (Ubuntu firewall)
sudo ufw allow 8082/tcp
sudo ufw deny 8080/tcp    # dashboard is SSH-tunnel only
sudo ufw enable
```

### 8. Build and start

```bash
# Build the container
docker compose build

# Start in background
docker compose up -d

# Check it's running
docker compose ps
docker compose logs -f
```

---

## Accessing the Dashboard

The dashboard runs on port 8080 but is **only bound to localhost** for security.
Access it via SSH tunnel from your local machine:

### ssh -i C:\Users\Angus\.ssh\polymarket_deploy -L 8080:localhost:8080 angus@95.216.211.236 -N

###

```bash
# On your local machine (not the VPS):
ssh -L 8080:localhost:8080 user@your-vps-ip -N
```

Then open your browser: **http://localhost:8080**

Username: (any)
Password: whatever you set as `DASHBOARD_SECRET`

### Dashboard features:
- Start / Stop the bot
- Switch between Simulation and Live mode
- Set simulation starting balance
- Real-time metrics: PnL, win rate, spread, latency
- Live charts: cumulative PnL, bracket volume, spread + latency distributions
- Open brackets table
- Recent trades table
- Live-edit strategy parameters (threshold, position size, etc.)
- Trade log viewer

---

## Running in Simulation Mode

Simulation mode is recommended before going live. It:
- Makes **no real orders** — all bracket placement is simulated
- Models realistic CLOB latency (~32ms p50, ~85ms p99)
- Applies a 92% fill probability (some brackets miss)
- Deducts realistic Polymarket taker fees (1%)
- Deducts estimated Polygon gas costs (~$0.002/redemption)
- Lets you test any starting wallet size

**To run simulation:**
1. Open dashboard via SSH tunnel
2. Select "SIM MODE"
3. Set your desired starting balance
4. Click START

Watch the metrics for 24–48 hours. If the bot is consistently profitable in sim,
gradually scale position size and switch to live mode.

---

## Strategy Parameters

Edit these in `bot/config.py` or via the dashboard Config panel:

| Parameter | Default | Description |
|---|---|---|
| `target_windows` | `["15M", "1H"]` | Which time windows to trade |
| `target_assets` | `["ETH", "BTC"]` | Which assets to watch |
| `bracket_threshold` | `0.985` | Buy bracket when Up+Down < this |
| `position_size_usdc` | `10.0` | USDC per leg (2x total per bracket) |
| `max_concurrent_brackets` | `20` | Maximum open brackets at once |
| `max_wallet_exposure_pct` | `0.60` | Never deploy more than 60% of wallet |
| `cancel_unfilled_after_s` | `30` | Cancel stale unfilled orders |
| `taker_fee_pct` | `0.01` | Polymarket taker fee (1%) |

**Recommended scaling path:**
1. Start with `position_size_usdc = 10.0` in sim
2. Validate for 48h, confirm win rate > 55%
3. Switch to live with `position_size_usdc = 10.0`
4. After 24h live validation, scale to `25.0` then `46.0`

---

## Common Commands

### Container management

```bash
# Start bot
docker compose up -d

# Stop bot
docker compose down

# Restart
docker compose restart

# View live logs
docker compose logs -f

# View last 100 lines
docker compose logs --tail=100

# Check container status
docker compose ps

# Rebuild after code changes
docker compose build && docker compose up -d

# no cache
docker compose build --no-cache
```

### Log analysis

```bash
# View all trade log entries
cat logs/trades.jsonl | python3 -m json.tool

# Count total brackets
wc -l logs/trades.jsonl

# Show only winning trades
grep '"status": "won"' logs/trades.jsonl | wc -l

# Show only losing trades
grep '"status": "lost"' logs/trades.jsonl | wc -l

# Calculate total net PnL from log
cat logs/trades.jsonl | python3 -c "
import sys, json
lines = [json.loads(l) for l in sys.stdin if l.strip()]
resolved = [l for l in lines if l.get('event') == 'resolved']
total = sum(l.get('actual_net', 0) for l in resolved)
print(f'Total net PnL: \${total:.4f} over {len(resolved)} resolved brackets')
"

# Average latency
cat logs/trades.jsonl | python3 -c "
import sys, json
lines = [json.loads(l) for l in sys.stdin if l.strip()]
lats = [l['latency_ms'] for l in lines if l.get('latency_ms')]
if lats: print(f'Avg latency: {sum(lats)/len(lats):.1f}ms over {len(lats)} orders')
"

# Win rate calculation
cat logs/trades.jsonl | python3 -c "
import sys, json
lines = [json.loads(l) for l in sys.stdin if l.strip()]
resolved = [l for l in lines if l.get('event') == 'resolved']
won = sum(1 for l in resolved if l.get('actual_net', 0) > 0)
print(f'Win rate: {won}/{len(resolved)} = {won/max(len(resolved),1)*100:.1f}%')
"

# Most profitable asset
cat logs/trades.jsonl | python3 -c "
import sys, json
from collections import defaultdict
lines = [json.loads(l) for l in sys.stdin if l.strip()]
pnl = defaultdict(float)
for l in lines:
    if l.get('event') == 'resolved' and l.get('asset'):
        pnl[l['asset']] += l.get('actual_net', 0)
for asset, p in sorted(pnl.items(), key=lambda x: -x[1]):
    print(f'{asset}: \${p:.4f}')
"

# Worst latency brackets
cat logs/trades.jsonl | python3 -c "
import sys, json
lines = [json.loads(l) for l in sys.stdin if l.strip()]
slow = sorted([l for l in lines if l.get('latency_ms', 0) > 100], key=lambda x: -x['latency_ms'])
for l in slow[:10]:
    print(f\"{l['bracket_id']} {l.get('asset','')} {l['latency_ms']:.0f}ms\")
"
```

### Monitoring

```bash
# Watch container resource usage
docker stats polybot

# Check disk usage of logs
du -sh logs/

# Rotate logs if large (keeps last 10000 lines)
tail -n 10000 logs/trades.jsonl > logs/trades.jsonl.tmp && mv logs/trades.jsonl.tmp logs/trades.jsonl

# Check Alchemy webhook is reachable
curl -X POST http://localhost:8082/webhook -H 'Content-Type: application/json' -d '{"activity":[]}'
```

### SSH tunnel (access dashboard from local machine)

```bash
# Basic tunnel
ssh -L 8080:localhost:8080 user@YOUR_VPS_IP -N

# Keep tunnel alive (add to ~/.ssh/config for persistence)
# Host polybot-vps
#   HostName YOUR_VPS_IP
#   User your-user
#   LocalForward 8080 localhost:8080
#   ServerAliveInterval 30
#   ServerAliveCountMax 3

# Then just:
ssh -N polybot-vps
```

### Updating the bot

```bash
# Pull new code to VPS
scp -r polybot/ user@your-vps-ip:~/polybot

# On VPS: rebuild and restart
cd ~/polybot
docker compose down
docker compose build
docker compose up -d
```

---

## Performance Benchmarks to Watch

| Metric | Healthy | Investigate |
|---|---|---|
| Win rate | > 55% | < 52% |
| Avg spread | > 1.2% | < 0.8% |
| Avg latency | < 50ms | > 100ms |
| Fee/gross ratio | < 40% | > 60% |
| Brackets/day | 30–100 | < 10 or > 200 |

---

## Troubleshooting

**No brackets detected:**
- Check WebSocket is connecting: `docker compose logs | grep "WebSocket connected"`
- Confirm markets are being tracked: watch the "Markets Tracked" metric on dashboard
- Try adding "5M" to `target_windows` in config for higher volume

**Orders not filling:**
- Liquidity may be thin — reduce `position_size_usdc`
- Check if `bracket_threshold` is too aggressive (try 0.980)

**High latency (>100ms):**
- Check VPS location relative to London (eu-west-2)
- Verify no other processes consuming CPU: `docker stats`
- Consider migrating to Amsterdam/Frankfurt VPS

**Redemptions not working:**
- Check `ALCHEMY_API_KEY` is set correctly
- Confirm port 8082 is open: `sudo ufw status`
- Check webhook URL in Alchemy dashboard points to correct VPS IP
- Fallback polling runs every 30s regardless, so redemptions will still happen

**Dashboard not accessible:**
- Confirm SSH tunnel is running on your local machine
- Check container is healthy: `docker compose ps`

---

## Security Notes

- Never expose port 8080 publicly — always use SSH tunnel
- Keep `.env` out of any git repository
- Use a dedicated wallet for the bot with only the USDC you intend to trade
- Rotate Polymarket API keys periodically
- Monitor USDC balance — set `max_wallet_exposure_pct` conservatively

---

## Disclaimer

This bot interacts with real financial markets and real USDC. Start with simulation
mode and small position sizes. Trading involves risk of loss. This software is
provided as-is with no warranty.
