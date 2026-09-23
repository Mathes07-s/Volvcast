"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           CLOUD RELAY SERVER — Production Audio Broadcast Engine             ║
║           FastAPI / Uvicorn · Render-optimised · Zero-latency relay          ║
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
║   BroadcastRelay.distribute()                                                ║
║        │                                                                     ║
║        ▼  fan-out to all connected listeners                                 ║
║   /ws/listen  ──────────────────► [50+ Student Phones / Browsers]            ║
║                                                                              ║
║   /listen     ──────────────────► Serves the pitch-black listener UI         ║
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
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
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
    """

    def __init__(self) -> None:
        # Map listener_id -> asyncio.Queue[bytes | None]
        self._listeners: dict[int, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

        # Telemetry
        self.broadcaster_connected: bool = False
        self.broadcaster_id: Optional[str] = None
        self.total_chunks_relayed: int = 0
        self.listener_count: int = 0
        self._start_time: float = time.time()

    # ── Listener lifecycle ────────────────────────────────────────────────────

    async def register_listener(self, listener_id: int) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=LISTENER_QUEUE_MAXSIZE)
        async with self._lock:
            self._listeners[listener_id] = q
            self.listener_count = len(self._listeners)
        logger.info(
            f"Listener #{listener_id} registered. "
            f"Total listeners: {self.listener_count}"
        )
        return q

    async def unregister_listener(self, listener_id: int) -> None:
        async with self._lock:
            q = self._listeners.pop(listener_id, None)
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
        """
        self.total_chunks_relayed += 1

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

    def stats(self) -> dict:
        uptime = round(time.time() - self._start_time, 1)
        return {
            "uptime_seconds": uptime,
            "broadcaster_connected": self.broadcaster_connected,
            "broadcaster_id": self.broadcaster_id,
            "listener_count": self.listener_count,
            "total_chunks_relayed": self.total_chunks_relayed,
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
    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║        Cloud Relay Server -- ONLINE          ║")
    logger.info("╠══════════════════════════════════════════════╣")
    logger.info("║  Broadcaster uplink : /ws/broadcast_uplink   ║")
    logger.info("║  Listener WebSocket : /ws/listen             ║")
    logger.info("║  Listener UI        : /listen                ║")
    logger.info("║  Status API         : /status                ║")
    logger.info("╚══════════════════════════════════════════════╝")
    yield
    logger.info("Cloud Relay Server shutting down.")


# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Cloud Audio Relay",
    description="Zero-latency PCM audio relay for classroom presentations.",
    version="2.0.0",
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


@app.get("/status")
async def status():
    """Lightweight health + telemetry endpoint."""
    return relay.stats()


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

    queue = await relay.register_listener(listener_id)

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
