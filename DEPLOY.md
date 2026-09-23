# Cloud Relay — Deployment Checklist

> **Presentation is tomorrow.** Follow these steps in order. Total time: ~8 minutes.

---

## Step 1 — Deploy to Render (5 minutes)

### 1a. Create a GitHub repo for the cloud_relay folder

```bash
# From inside the cloud_relay/ folder
git init
git add .
git commit -m "Initial cloud relay deployment"
# Create a repo on GitHub, then:
git remote add origin https://github.com/YOUR_USERNAME/cloud-audio-relay.git
git push -u origin main
```

### 1b. Connect to Render

1. Go to **[render.com](https://render.com)** → **New** → **Web Service**
2. Connect your GitHub account and select the `cloud-audio-relay` repo
3. Render auto-detects `render.yaml` — click **Deploy**
4. Wait ~2 minutes for the build to complete
5. Your URL will be something like: `https://cloud-audio-relay.onrender.com`

### 1c. Verify deployment

Open your browser and visit:
```
https://cloud-audio-relay.onrender.com/status
```
You should see JSON like:
```json
{
  "uptime_seconds": 42.1,
  "broadcaster_connected": false,
  "listener_count": 0,
  "total_chunks_relayed": 0
}
```

---

## Step 2 — Configure the Broadcaster (1 minute)

Open `broadcaster.py` on your laptop and update **line 54**:

```python
# BEFORE
RELAY_URL: str = "wss://YOUR-APP-NAME.onrender.com/ws/broadcast_uplink"

# AFTER (use your actual Render URL)
RELAY_URL: str = "wss://cloud-audio-relay.onrender.com/ws/broadcast_uplink"
```

Install dependencies on your laptop:
```bash
pip install sounddevice websockets numpy
```

### Test: List your audio devices
```bash
python broadcaster.py --list-devices
```
Look for **CABLE Output (VB-Audio Virtual Cable)** or similar.

### Test: Start broadcasting
```bash
python broadcaster.py
# Or force a specific device:
python broadcaster.py --device 12
```

You should see:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  UPLINK CONNECTED — streaming audio ↑
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## Step 3 — Student Listener URL

Tell students to open:
```
https://cloud-audio-relay.onrender.com/listen
```

They tap **"TAP TO LISTEN"** and audio plays immediately with:
- ≤80 ms latency
- Auto-reconnect on drop
- Screen stays awake (Wake Lock API)
- Neon EQ visualizer

---

## Render Free Tier — Important Notes

| Issue | Solution |
|-------|----------|
| **Cold start** (~30s delay after 15 min idle) | Visit `/status` 1 min before class starts to wake it up |
| **50-second timeout** | Already handled — server sends ping every 20s |
| **Bandwidth limit** | Free tier: 100 GB/month. 50 students × 44100×2 bytes/s = ~4 MB/s. 1-hour session ≈ 14 GB. Should be fine. |

### Recommended: Upgrade to Starter plan ($7/mo)
- Always-on (no cold starts)
- 1 GB RAM (vs 512 MB free)

---

## Architecture at a Glance

```
[Your Laptop]                          [Render Cloud]              [50 Students]
     │                                       │                           │
VB-Audio Virtual Cable                       │                    Chrome/Safari
     │ WASAPI Loopback                       │                    /listen page
     │ sounddevice                           │                           │
     │ float32 → Int16                       │              wss://  /ws/listen
     │                                  BroadcastRelay                  │
     └──── wss:// /ws/broadcast_uplink ────► fan-out ──────────────────►│
                                        asyncio.Queue               AudioContext
                                        (per listener)          Jitter Buffer ≤80ms
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `broadcaster.py` can't find VB-Cable | Run `--list-devices`, then `--device <index>` |
| Students hear nothing | Check `/status` — is `broadcaster_connected: true`? |
| Audio stutters on student side | Likely their CPU is throttled. Close other browser tabs. |
| Render URL gives 404 | Render free tier may have spun down — wait 30s and reload |
| High latency on student side | Their network is slow; the strict jitter buffer will snap back automatically |
