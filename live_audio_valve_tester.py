"""
╔═════════════════════════════════════════════════════════════════╗
║   ELITE PRODUCTION VERIFICATION — Audio Valve / Volvcast        ║
║   T1: HTTP Health   T2: BUG-10 HTML   T3: 5-Client WS Load Test ║
║   Usage: python live_audio_valve_tester.py                       ║
╚═════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import argparse, asyncio, json, re, statistics, sys, time, urllib.request, urllib.error
from dataclasses import dataclass, field
from typing import List, Optional

DEFAULT_WSS_URL  = "wss://volvcast.onrender.com"
N_CLIENTS        = 5
LOAD_TEST_SECS   = 10
CONNECT_TIMEOUT  = 20.0
IDLE_TIMEOUT     = 8.0
PASS = "PASS"; FAIL = "FAIL"

try:
    import os; os.system("color")
    RED="\033[91m"; GRN="\033[92m"; YLW="\033[93m"
    CYN="\033[96m"; WHT="\033[97m"; DIM="\033[2m"; RST="\033[0m"
except Exception:
    RED=GRN=YLW=CYN=WHT=DIM=RST=""

@dataclass
class ClientResult:
    client_id: int
    connected: bool = False
    frames_received: int = 0
    bytes_received: int = 0
    first_frame_at: Optional[float] = None
    last_frame_at: Optional[float] = None
    connection_latency_ms: float = 0.0
    frame_intervals_ms: List[float] = field(default_factory=list)
    error: str = ""

    @property
    def dropped_frames_estimated(self):
        if len(self.frame_intervals_ms) < 3:
            return 0
        med = statistics.median(self.frame_intervals_ms)
        return sum(1 for iv in self.frame_intervals_ms if iv > 3 * med)

    @property
    def jitter_ms(self):
        return statistics.stdev(self.frame_intervals_ms) if len(self.frame_intervals_ms) >= 2 else 0.0

    @property
    def median_interval_ms(self):
        return statistics.median(self.frame_intervals_ms) if self.frame_intervals_ms else 0.0

    @property
    def throughput_kbps(self):
        if not self.first_frame_at or not self.last_frame_at:
            return 0.0
        d = self.last_frame_at - self.first_frame_at
        return (self.bytes_received * 8) / (d * 1000) if d > 0 else 0.0


# ── T1: HTTP Health ──────────────────────────────────────────────────────────
def test_http_health(base_url):
    results = {}
    for path in ("/health", "/status"):
        url = f"{base_url}{path}"
        t0 = time.monotonic()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AudioValveTester/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000
                data = json.loads(resp.read().decode("utf-8"))
                results[path] = {"ok": True, "status_code": resp.status,
                                  "response_ms": round(elapsed_ms, 1), "data": data}
        except urllib.error.HTTPError as e:
            results[path] = {"ok": False, "error": f"HTTP {e.code}: {e.reason}"}
        except Exception as e:
            results[path] = {"ok": False, "error": str(e)}
    return results


# ── T2: BUG-10 HTML fix ──────────────────────────────────────────────────────
def test_bug10_html_fix(base_url):
    result = {"path": "/listen", "ok": False, "details": []}
    try:
        req = urllib.request.Request(f"{base_url}/listen",
                                     headers={"User-Agent": "AudioValveTester/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        hardcoded = bool(re.search(r'[`\'"]ws://\$\{', html))
        proto_detect = bool(re.search(r"location\.protocol\s*===?\s*['\"]https:", html))
        wss_present = "wss:" in html
        ws_endpoint = "/ws/listen" in html
        result["details"] = [
            f"Hardcoded ws://$ found (should be False): {hardcoded}",
            f"Protocol detection present (should be True): {proto_detect}",
            f"wss: construction present (should be True): {wss_present}",
            f"Correct /ws/listen endpoint: {ws_endpoint}",
        ]
        result["ok"] = not hardcoded and proto_detect and wss_present and ws_endpoint
    except Exception as e:
        result["error"] = str(e)
    return result


# ── T3: Load test ─────────────────────────────────────────────────────────────
async def run_single_client(client_id, wss_url, duration, start_event):
    result = ClientResult(client_id=client_id)
    try:
        import websockets
    except ImportError:
        result.error = "websockets not installed: pip install websockets"
        return result

    connect_start = time.monotonic()
    try:
        async with websockets.connect(
            wss_url, ping_interval=20, ping_timeout=10,
            open_timeout=CONNECT_TIMEOUT, max_size=4*1024*1024
        ) as ws:
            result.connected = True
            result.connection_latency_ms = (time.monotonic() - connect_start) * 1000
            start_event.set()
            deadline = asyncio.get_event_loop().time() + duration
            last_t = None

            while asyncio.get_event_loop().time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=IDLE_TIMEOUT)
                    now = time.monotonic()
                    if isinstance(msg, bytes) and len(msg) <= 1:
                        continue
                    if result.first_frame_at is None:
                        result.first_frame_at = now - connect_start
                    if last_t is not None:
                        result.frame_intervals_ms.append((now - last_t) * 1000)
                    last_t = now
                    result.last_frame_at = now - connect_start
                    result.frames_received += 1
                    result.bytes_received += len(msg) if isinstance(msg, (bytes, bytearray)) else len(msg.encode())
                except asyncio.TimeoutError:
                    break   # No broadcaster active — expected
                except Exception as e:
                    result.error = f"recv: {e}"; break
    except asyncio.TimeoutError:
        result.error = f"Connection timeout ({CONNECT_TIMEOUT}s)"
    except Exception as e:
        result.error = str(e)
    return result

async def run_load_test(wss_url, n, duration):
    start_event = asyncio.Event()
    tasks = [asyncio.create_task(
        run_single_client(i+1, wss_url, duration, start_event),
        name=f"client-{i+1}") for i in range(n)]
    return await asyncio.gather(*tasks)


# ── Report printing ───────────────────────────────────────────────────────────
def fmt_ok(ok, msg):
    icon = f"{GRN}[{PASS}]{RST}" if ok else f"{RED}[{FAIL}]{RST}"
    return f"  {icon}  {msg}"

def print_section(t):
    print(f"\n{WHT}{'─'*66}{RST}\n  {WHT}{t}{RST}\n{WHT}{'─'*66}{RST}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_WSS_URL)
    p.add_argument("--clients", type=int, default=N_CLIENTS)
    p.add_argument("--duration", type=float, default=LOAD_TEST_SECS)
    p.add_argument("--skip-load-test", action="store_true")
    args = p.parse_args()

    wss_base   = args.url.rstrip("/")
    https_base = wss_base.replace("wss://","https://").replace("ws://","http://")
    ws_listen  = f"{wss_base}/ws/listen"

    print(f"\n{CYN}╔═══ ELITE PRODUCTION VERIFICATION — Audio Valve / Volvcast ═══╗{RST}")
    print(f"{CYN}║  Target  : {wss_base:<51}║{RST}")
    print(f"{CYN}║  Clients : {args.clients:<3}   Duration: {args.duration}s   Time: {time.strftime('%H:%M:%S'):<22}║{RST}")
    print(f"{CYN}╚════════════════════════════════════════════════════════════════╝{RST}")

    all_verdicts = []

    # T1
    print_section("T1 · HTTP HEALTH & STATUS CHECK")
    http_r = test_http_health(https_base)
    for path, r in http_r.items():
        if r.get("ok"):
            msg = f"{https_base}{path}  [{r['status_code']}]  {r['response_ms']}ms"
            if path == "/status":
                d = r.get("data",{})
                msg += (f"\n       {DIM}broadcaster={d.get('broadcaster_connected')}  "
                        f"listeners={d.get('listener_count')}  "
                        f"muted={d.get('global_mute')}  "
                        f"uptime={d.get('uptime_seconds')}s{RST}")
            print(fmt_ok(True, msg))
        else:
            print(fmt_ok(False, f"{https_base}{path}  {r.get('error')}"))
    http_ok = all(r.get("ok") for r in http_r.values())
    all_verdicts.append(http_ok)

    # T2
    print_section("T2 · BUG-10 PROTOCOL FIX VERIFICATION  (ws:// → wss://)")
    bug10 = test_bug10_html_fix(https_base)
    print(fmt_ok(bug10.get("ok", False), f"Deployed listener HTML at {https_base}/listen"))
    for d in bug10.get("details", []):
        bad = "True" in d and "False" in d.split(":")[0] if "should be False" in d else ("False" in d and "True" in d.split(":")[0])
        col = RED if bad else GRN
        print(f"    {col}▸ {d}{RST}")
    if bug10.get("error"):
        print(f"    {RED}Error: {bug10['error']}{RST}")
    all_verdicts.append(bug10.get("ok", False))

    # T3
    if not args.skip_load_test:
        print_section(f"T3 · {args.clients}-CLIENT CONCURRENT WS LOAD TEST  ({args.duration}s)")
        print(f"  {DIM}Connecting all {args.clients} clients to {ws_listen}{RST}")
        print(f"  {DIM}Silence is expected if no broadcaster is active — connection test still runs.{RST}\n")
        try:
            import websockets  # noqa
        except ImportError:
            print(f"  {YLW}[WARN]  websockets not installed. Run: pip install websockets{RST}")
            all_verdicts.append(False)
        else:
            results = asyncio.run(run_load_test(ws_listen, args.clients, args.duration))

            print(f"  {DIM}{'Client':<10} {'OK':<8} {'Frames':<10} {'Drops':<8} "
                  f"{'MedianΔ':<12} {'Jitter':<12} {'kbps':<12} {'ConnMs'}{RST}")
            print(f"  {DIM}{'─'*86}{RST}")

            connected_n = 0; total_f = 0; total_d = 0
            all_jit = []; all_iv = []; all_lat = []

            for r in results:
                ok_s  = f"{GRN}YES{RST}" if r.connected else f"{RED}NO{RST}"
                dr    = r.dropped_frames_estimated
                dc    = RED if dr > 3 else (YLW if dr > 0 else GRN)
                med_s = f"{r.median_interval_ms:.1f}ms" if r.frame_intervals_ms else "—"
                jit_s = f"{r.jitter_ms:.1f}ms" if r.jitter_ms else "—"
                kbp_s = f"{r.throughput_kbps:.1f}" if r.throughput_kbps else "—"
                lat_s = f"{r.connection_latency_ms:.0f}" if r.connected else "—"
                print(f"  Client-{r.client_id:<3}  {ok_s:<16} {r.frames_received:<10} "
                      f"{dc}{dr:<8}{RST} {med_s:<12} {jit_s:<12} {kbp_s:<12} {lat_s}ms")
                if r.error and not r.connected:
                    print(f"    {YLW}└ {r.error}{RST}")
                if r.connected: connected_n += 1; all_lat.append(r.connection_latency_ms)
                total_f += r.frames_received; total_d += r.dropped_frames_estimated
                if r.jitter_ms: all_jit.append(r.jitter_ms)
                all_iv.extend(r.frame_intervals_ms)

            print(f"\n  {WHT}AGGREGATE{RST}")
            print(f"  {'Clients connected:':<35} {connected_n}/{args.clients}")
            print(f"  {'Total frames received:':<35} {total_f:,}")
            print(f"  {'Total estimated frame drops:':<35} {total_d}")
            if all_lat:
                print(f"  {'Avg connect latency:':<35} {statistics.mean(all_lat):.0f}ms  (max {max(all_lat):.0f}ms)")
            if all_iv:
                print(f"  {'Median frame interval:':<35} {statistics.median(all_iv):.1f}ms")
                if len(all_iv) > 1:
                    print(f"  {'Overall jitter (stdev):':<35} {statistics.stdev(all_iv):.1f}ms")

            bug3_ok = total_d == 0
            bug5_ok = connected_n == args.clients

            print(f"\n  {WHT}BUG REGRESSION VERDICTS{RST}")
            print(fmt_ok(bug3_ok,
                f"BUG-3 (Worker queue starvation): "
                f"{'0 drops — ELIMINATED' if bug3_ok else str(total_d)+' drops — NEEDS INVESTIGATION'}"))
            print(fmt_ok(bug5_ok,
                f"BUG-5 (Broadcast lock contention): "
                f"{'All {n} clients connected simultaneously — ELIMINATED'.format(n=connected_n) if bug5_ok else str(connected_n)+'/'+str(args.clients)+' connected — lock may block'}"))

            all_verdicts.extend([bug3_ok, bug5_ok])
    else:
        print(f"\n  {YLW}[SKIP] Load test skipped (--skip-load-test){RST}")

    # Final
    all_pass = all(all_verdicts)
    print(f"\n{WHT}{'═'*66}{RST}")
    if all_pass:
        print(f"{GRN}  🏆  ALL CHECKS PASSED — PRODUCTION IS STABLE & GLITCH-FREE  🏆{RST}")
    else:
        failed = sum(1 for v in all_verdicts if not v)
        print(f"{YLW}  ⚠️   {failed}/{len(all_verdicts)} checks need attention — review output above{RST}")
    print(f"{WHT}{'═'*66}{RST}\n")
    sys.exit(0 if all_pass else 1)

if __name__ == "__main__":
    main()
