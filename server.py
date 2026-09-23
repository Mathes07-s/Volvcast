"""
╔══════════════════════════════════════════════════════════════════════════════╗
║       CLOUD RELAY SERVER v3.0 — Live Audience Operations Center              ║
║       FastAPI / Uvicorn · Render-optimised · Zero-latency relay              ║
║                                                                              ║
║  Architecture:                                                               ║
║                                                                              ║
║   [Broadcaster Laptop]                                                       ║
║        │  websockets client                                                  ║
║        ▼                                                                     ║
║   /ws/broadcast_uplink  ◄─────── single broadcaster connection               ║
║        │                                                                     ║
║        │  raw Int16 PCM bytes (in-memory, never touches disk)                ║
║        ▼                                                                     ║
║   BroadcastRelay.distribute()  ←── global_mute replaces audio with silence  ║
║        │                                                                     ║
║        ▼  fan-out to all connected listeners                                 ║
║   /ws/listen  ──────────────────► [2000 Student Phones / Browsers]           ║
║                                                                              ║
║   /listen     ──────────────────► Serves the pitch-black listener UI         ║
║   /admin      ──────────────────► Live Audience Operations Center (Admin UI) ║
║   /ws/admin_stats ──────────────► SSE stream: listener count + uptime        ║
║   POST /admin/mute ─────────────► Toggle Global Mute (silence all audio)     ║
║   POST /admin/disconnect_all ───► Kick all listeners (force reconnect)       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Render deployment notes:
  - Set start command: uvicorn server:app --host 0.0.0.0 --port $PORT
  - Free tier has a 50-second inactivity timeout; WebSocket ping/pong keeps
    connections alive indefinitely.
  - No disk I/O, no state persistence — pure memory relay.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.websockets import WebSocketState

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("CloudRelay")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
# WebSocket keep-alive interval (seconds).
# Render will kill idle connections after 50 s; we ping every 20 s.
WS_PING_INTERVAL: int = 20
WS_PING_TIMEOUT: int  = 10

# Maximum number of audio chunks queued per listener before drops.
# Keeps fast-to-slow listeners from accumulating unbounded memory.
LISTENER_QUEUE_MAXSIZE: int = 32


# ─────────────────────────────────────────────────────────────────────────────
# BROADCAST RELAY CORE
# Thread-safe, asyncio-native fan-out to all connected WebSocket listeners.
# ─────────────────────────────────────────────────────────────────────────────
class BroadcastRelay:
    """
    Central relay hub.

    The broadcaster uplink pushes raw bytes here via `distribute(chunk)`.
    Every registered listener has its own asyncio.Queue; the relay puts the
    chunk into every queue non-blockingly (drops oldest frame on overflow).

    v3.0 additions:
      - global_mute:  when True, distribute() sends a silence frame instead
                      of the real audio chunk (DSP path is untouched).
      - active_listener_ws: tracks live WebSocket objects for mass-disconnect.
      - admin_stats_queues: SSE queues for the admin dashboard.
    """

    def __init__(self) -> None:
        # Map listener_id -> asyncio.Queue[bytes | None]
        self._listeners: dict[int, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

        # ── v3.0: Admin controls ───────────────────────────────────────────
        # Global mute: replaces real audio with Int16 silence frames
        self.global_mute: bool = False

        # Track live listener WebSocket objects for mass-disconnect
        self.active_listener_ws: dict[int, WebSocket] = {}

        # SSE queues for /ws/admin_stats  (one queue per admin browser tab)
        self._admin_queues: Set[asyncio.Queue] = set()

        # Telemetry
        self.broadcaster_connected: bool = False
        self.broadcaster_id: Optional[str] = None
        self.total_chunks_relayed: int = 0
        self.listener_count: int = 0
        self._start_time: float = time.time()

    # ── Listener lifecycle ────────────────────────────────────────────────────

    async def register_listener(self, listener_id: int, ws: WebSocket) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=LISTENER_QUEUE_MAXSIZE)
        async with self._lock:
            self._listeners[listener_id] = q
            self.active_listener_ws[listener_id] = ws
            self.listener_count = len(self._listeners)
        logger.info(
            f"Listener #{listener_id} registered. "
            f"Total listeners: {self.listener_count}"
        )
        return q

    async def unregister_listener(self, listener_id: int) -> None:
        async with self._lock:
            q = self._listeners.pop(listener_id, None)
            self.active_listener_ws.pop(listener_id, None)
            self.listener_count = len(self._listeners)
        if q is not None:
            # Poison-pill to unblock the sender loop
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass
        logger.info(
            f"Listener #{listener_id} removed. "
            f"Total listeners: {self.listener_count}"
        )

    # ── Audio fan-out ─────────────────────────────────────────────────────────

    async def distribute(self, chunk: bytes) -> None:
        """
        Fan out one raw PCM chunk to every registered listener queue.
        Non-blocking: if a listener's queue is full, drop the oldest frame
        instead of blocking the relay (sacrifices a stale frame, not latency).

        When global_mute is active, we replace the real chunk with an equal-
        length silence frame (Int16 zeros) so listeners hear nothing but the
        audio engine stays warm and jitter-buffer intact.
        """
        self.total_chunks_relayed += 1

        # ── Global mute: substitute silence of the same byte length ──────────
        if self.global_mute:
            chunk = bytes(len(chunk))  # len-matched block of 0x00

        async with self._lock:
            targets = list(self._listeners.values())

        for q in targets:
            if q.full():
                # Evict oldest frame to make room — strict latency policy
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                q.put_nowait(chunk)
            except asyncio.QueueFull:
                pass  # Extremely unlikely after the eviction above

    # ── Admin SSE helpers ─────────────────────────────────────────────────────

    def register_admin_sse(self) -> asyncio.Queue:
        """Register a new admin dashboard tab for SSE stat pushes."""
        q: asyncio.Queue = asyncio.Queue(maxsize=8)
        self._admin_queues.add(q)
        return q

    def unregister_admin_sse(self, q: asyncio.Queue) -> None:
        self._admin_queues.discard(q)

    async def push_admin_stats(self) -> None:
        """
        Background task: pushes current stats to all admin SSE queues
        every 2 seconds. Launched once from the lifespan.
        """
        while True:
            await asyncio.sleep(2)
            payload = self.stats()
            dead: list[asyncio.Queue] = []
            for q in list(self._admin_queues):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    # Admin tab is too slow — evict oldest to prevent backlog
                    try:
                        q.get_nowait()
                        q.put_nowait(payload)
                    except Exception:
                        dead.append(q)
                except Exception:
                    dead.append(q)
            for q in dead:
                self._admin_queues.discard(q)

    def stats(self) -> dict:
        uptime = round(time.time() - self._start_time, 1)
        return {
            "uptime_seconds": uptime,
            "broadcaster_connected": self.broadcaster_connected,
            "broadcaster_id": self.broadcaster_id,
            "listener_count": self.listener_count,
            "total_chunks_relayed": self.total_chunks_relayed,
            "global_mute": self.global_mute,
        }


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL RELAY INSTANCE  (lives for the lifetime of the process)
# ─────────────────────────────────────────────────────────────────────────────
relay = BroadcastRelay()

# ─────────────────────────────────────────────────────────────────────────────
# LISTENER HTML  (served inline — no static-file server needed on Render)
# ─────────────────────────────────────────────────────────────────────────────
# We read index.html from disk if present, otherwise fall back to the embedded
# minimal version. This lets you ship a richer index.html without changing
# server.py.
_THIS_DIR = Path(__file__).parent
_INDEX_PATH = _THIS_DIR / "index.html"

def _get_listener_html() -> str:
    if _INDEX_PATH.exists():
        return _INDEX_PATH.read_text(encoding="utf-8")
    # Minimal embedded fallback (the real UI is in index.html)
    return """<!DOCTYPE html><html><head><title>Audio Relay</title></head>
<body style="background:#000;color:#64ffda;font-family:monospace;display:flex;
align-items:center;justify-content:center;height:100vh;margin:0;">
<h1>index.html not found -- deploy it alongside server.py</h1>
</body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# LIFESPAN
# ─────────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the background SSE stats pusher
    stats_task = asyncio.create_task(
        relay.push_admin_stats(), name="admin-stats-pusher"
    )
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║   Cloud Relay Server v3.0 -- ONLINE              ║")
    logger.info("╠══════════════════════════════════════════════════╣")
    logger.info("║  Broadcaster uplink : /ws/broadcast_uplink       ║")
    logger.info("║  Listener WebSocket : /ws/listen                 ║")
    logger.info("║  Listener UI        : /listen                    ║")
    logger.info("║  Admin Dashboard    : /admin                     ║")
    logger.info("║  Admin Stats SSE    : /ws/admin_stats            ║")
    logger.info("║  Status API         : /status                    ║")
    logger.info("║  Keep-Alive Health  : /health                    ║")
    logger.info("╚══════════════════════════════════════════════════╝")
    yield
    stats_task.cancel()
    try:
        await stats_task
    except asyncio.CancelledError:
        pass
    logger.info("Cloud Relay Server shutting down.")


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Cloud Audio Relay",
    description="Zero-latency PCM audio relay for classroom presentations.",
    version="3.0.0",
    lifespan=lifespan,
)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
@app.get("/listen", response_class=HTMLResponse, include_in_schema=False)
async def serve_listener_ui():
    """Serve the pitch-black listener UI."""
    return HTMLResponse(content=_get_listener_html())


@app.get("/health")
async def health_check():
    """Keep-Alive endpoint for GitHub Actions cron pinging."""
    return {"status": "alive"}


@app.get("/status")
async def status():
    """Full telemetry snapshot (includes mute state)."""
    return relay.stats()


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN DASHBOARD — served at /admin
# ─────────────────────────────────────────────────────────────────────────────
_ADMIN_PATH = _THIS_DIR / "admin.html"


@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)
async def serve_admin_ui():
    """Serve the Live Audience Operations Center admin dashboard."""
    if _ADMIN_PATH.exists():
        return HTMLResponse(content=_ADMIN_PATH.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>admin.html not found — deploy it alongside server.py</h1>", status_code=404)


# ─────────────────────────────────────────────────────────────────────────────
# SSE — ADMIN STATS STREAM  (/ws/admin_stats)
# Uses Server-Sent Events (SSE) not WebSocket — more reliable for dashboards.
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/ws/admin_stats")
async def admin_stats_sse():
    """
    SSE stream that pushes live stats every 2 seconds to the admin dashboard.
    Route kept as /ws/admin_stats to match the original spec, but uses SSE
    (text/event-stream) which is simpler and more reliable for UI polling.
    """
    q = relay.register_admin_sse()

    async def event_generator():
        try:
            # Send an immediate snapshot on connect
            yield f"data: {json.dumps(relay.stats())}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"data: {json.dumps(payload)}\n\n"
                except asyncio.TimeoutError:
                    # SSE keep-alive comment to prevent proxy timeouts
                    yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            relay.unregister_admin_sse(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # Disables Nginx/Render proxy buffering
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN CONTROL ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/admin/mute")
async def toggle_mute():
    """
    Toggle the global mute. When muted, the relay substitutes all incoming
    broadcaster frames with Int16 silence (zero bytes) before fan-out.
    The broadcaster connection and listener connections stay alive.
    """
    relay.global_mute = not relay.global_mute
    state = "MUTED" if relay.global_mute else "UNMUTED"
    logger.warning(f"[ADMIN] Global mute toggled → {state}")
    return {"global_mute": relay.global_mute, "state": state}


@app.post("/admin/disconnect_all")
async def disconnect_all_listeners():
    """
    Forcefully close every active listener WebSocket.
    Listeners with auto-reconnect will rejoin automatically on page refresh.
    Use this to purge stale/dead connections or to enforce a hard reset.
    """
    async with relay._lock:
        snapshot = dict(relay.active_listener_ws)  # copy under lock

    kicked = 0
    for lid, ws in snapshot.items():
        try:
            if ws.client_state == WebSocketState.CONNECTED:
                await ws.close(code=1001)  # 1001 = Going Away
                kicked += 1
        except Exception:
            pass  # Already disconnected; no-op

    logger.warning(f"[ADMIN] Disconnect-All fired — {kicked} listeners kicked.")
    return {"kicked": kicked}


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET — BROADCASTER UPLINK
# Only ONE broadcaster is expected at a time.  If a second one connects while
# the first is alive, we still relay from both (handy for failover).
# ─────────────────────────────────────────────────────────────────────────────
_listener_counter: int = 0


@app.websocket("/ws/broadcast_uplink")
async def broadcaster_uplink(ws: WebSocket):
    await ws.accept()
    client_host = ws.client.host if ws.client else "unknown"
    logger.info(f"[UPLINK] Broadcaster CONNECTED from {client_host}")

    relay.broadcaster_connected = True
    relay.broadcaster_id = client_host

    try:
        while True:
            # receive_bytes() will raise WebSocketDisconnect on close,
            # and will block until data or a control frame arrives.
            chunk: bytes = await asyncio.wait_for(
                ws.receive_bytes(),
                timeout=WS_PING_INTERVAL + WS_PING_TIMEOUT + 5,
            )
            if chunk:
                await relay.distribute(chunk)

    except asyncio.TimeoutError:
        logger.warning("Broadcaster uplink timed out (no data or ping received).")
    except WebSocketDisconnect as exc:
        logger.info(f"[UPLINK] Broadcaster disconnected (code={exc.code}).")
    except Exception as exc:
        logger.error(f"Broadcaster uplink error: {exc!r}")
    finally:
        relay.broadcaster_connected = False
        relay.broadcaster_id = None
        # Try to close cleanly if socket is still open
        if ws.client_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except Exception:
                pass
        logger.info("[UPLINK] Broadcaster uplink handler cleaned up.")


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET — LISTENER
# Each student browser opens one of these.  The handler:
#   1. Registers a queue with the relay.
#   2. Starts a background task that drains the queue and sends frames.
#   3. Concurrently runs the Starlette ping-pong loop to keep the connection
#      alive through Render's 50-second idle timeout.
# ─────────────────────────────────────────────────────────────────────────────

@app.websocket("/ws/listen")
async def listener_ws(ws: WebSocket):
    global _listener_counter
    _listener_counter += 1
    listener_id = _listener_counter

    await ws.accept()
    client_host = ws.client.host if ws.client else "unknown"
    logger.info(f"[LISTEN] Listener #{listener_id} connected from {client_host}")

    queue = await relay.register_listener(listener_id, ws)

    # ── Sender coroutine ──────────────────────────────────────────────────
    async def _sender():
        """Drain the queue and ship bytes to the browser."""
        while True:
            chunk = await queue.get()
            if chunk is None:
                # Poison pill — unregister was called
                break
            if len(chunk) == 0:
                # Empty heartbeat from ping loop, skip
                continue
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_bytes(chunk)
            except Exception:
                break

    # ── Receiver coroutine (keeps connection alive, handles close) ────────
    async def _receiver():
        """
        Absorb any messages sent by the client (browsers send nothing, but
        the pong frames from our ping are handled at the protocol level).
        This coroutine exits when the client disconnects.
        """
        try:
            while True:
                msg = await ws.receive()
                # If the browser closes the tab we get a disconnect message
                if msg.get("type") == "websocket.disconnect":
                    break
        except WebSocketDisconnect:
            pass
        except Exception:
            pass

    # ── Ping keepalive coroutine ──────────────────────────────────────────
    async def _ping_loop():
        """
        Send a WebSocket ping every WS_PING_INTERVAL seconds so Render
        never kills us due to inactivity.
        """
        try:
            while True:
                await asyncio.sleep(WS_PING_INTERVAL)
                if ws.client_state != WebSocketState.CONNECTED:
                    break
                try:
                    # Starlette native ping (sends a WebSocket PING control frame)
                    await ws.send_bytes(b"\x00")  # 1-byte heartbeat; JS side ignores it
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    # ── Run all three concurrently; cancel siblings when one exits ────────
    sender_task   = asyncio.create_task(_sender(),    name=f"sender-{listener_id}")
    receiver_task = asyncio.create_task(_receiver(),  name=f"receiver-{listener_id}")
    ping_task     = asyncio.create_task(_ping_loop(), name=f"ping-{listener_id}")

    try:
        done, pending = await asyncio.wait(
            [sender_task, receiver_task, ping_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        # Await pending to suppress "Task was destroyed" warnings
        await asyncio.gather(*pending, return_exceptions=True)

    finally:
        await relay.unregister_listener(listener_id)
        if ws.client_state != WebSocketState.DISCONNECTED:
            try:
                await ws.close()
            except Exception:
                pass
        logger.info(f"[LISTEN] Listener #{listener_id} handler cleaned up.")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT  (local dev only — Render uses the start command above)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        ws_ping_interval=WS_PING_INTERVAL,
        ws_ping_timeout=WS_PING_TIMEOUT,
    )
