"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         LOCAL BROADCASTER — WASAPI Loopback Capture & Cloud Uplink           ║
║         sounddevice (WASAPI) · websockets · Auto-reconnect · Int16 PCM       ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Flow:                                                                       ║
║    VB-Audio Virtual Cable (WASAPI loopback)                                  ║
║         │  float32 stereo frames                                              ║
║         ▼                                                                     ║
║    sounddevice InputStream (blocksize=BLOCKSIZE)                              ║
║         │  convert float32 → int16  (clamp + dither)                         ║
║         │  keep only LEFT channel if mono output selected                     ║
║         ▼                                                                     ║
║    asyncio.Queue  (bounded, drops oldest on overflow)                         ║
║         │                                                                     ║
║         ▼  websockets client (wss:// to Render)                              ║
║    /ws/broadcast_uplink  ────────────► Cloud Relay Server                    ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage:
    python broadcaster.py

    # Force a specific device index (useful when auto-detect picks wrong device):
    python broadcaster.py --device 12

    # Adjust gain (1.0 = unity, 2.0 = +6 dB):
    python broadcaster.py --gain 1.5

Requirements (Windows only):
    pip install sounddevice websockets numpy

Edit RELAY_URL below to match your Render deployment URL.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import queue
import signal
import sys
import threading
import time
from typing import Optional

import numpy as np
import sounddevice as sd
import websockets
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    WebSocketException,
)

# ─────────────────────────────────────────────────────────────────────────────
# ★  CONFIGURE THIS  ★
# Replace with your actual Render URL.  Must use wss:// (not ws://) for Render.
# ─────────────────────────────────────────────────────────────────────────────
RELAY_URL: str = "wss://volvcast.onrender.com/ws/broadcast_uplink"

# ─────────────────────────────────────────────────────────────────────────────
# AUDIO CONFIGURATION — must match server-side expectations
# ─────────────────────────────────────────────────────────────────────────────
SAMPLE_RATE:  int   = 44100     # Hz — matches listener AudioContext sampleRate
CHANNELS:     int   = 1         # 1 = mono (lower bandwidth), 2 = stereo
BLOCKSIZE:    int   = 1024      # Frames per callback (~23 ms at 44100 Hz)
DTYPE_CAPTURE: str  = "float32" # sounddevice native dtype
GAIN:         float = 1.0       # Amplification multiplier (1.0 = unity)

# ─────────────────────────────────────────────────────────────────────────────
# NETWORK CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
RECONNECT_DELAY_INIT:  float = 1.0   # seconds for first reconnect attempt
RECONNECT_DELAY_MAX:   float = 16.0  # exponential back-off ceiling
SEND_QUEUE_MAXSIZE:    int   = 8     # max queued chunks (8 * 23ms = ~184ms max client-side delay)

# WebSocket options - We need ping_interval to force downstream traffic so Render's
# load balancer doesn't drop the connection (code 1005). But we keep ping_timeout=None
# so the client doesn't mistakenly disconnect itself if a pong is delayed by audio.
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT  = None

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Broadcaster")


# ─────────────────────────────────────────────────────────────────────────────
# DEVICE UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _is_wasapi(device: dict) -> bool:
    try:
        api_info = sd.query_hostapis(device["hostapi"])
        return "WASAPI" in api_info.get("name", "").upper()
    except Exception:
        return False

def _is_mme_or_ds(device: dict) -> bool:
    try:
        api_name = sd.query_hostapis(device["hostapi"]).get("name", "").upper()
        return "MME" in api_name or "DIRECTSOUND" in api_name
    except Exception:
        return False


def find_vb_cable_device() -> Optional[int]:
    """
    Locate the VB-Audio Virtual Cable INPUT device (loopback capture).
    We specifically look for an INPUT device whose name contains 'cable'
    so we capture the virtual cable's output through WASAPI loopback.
    """
    keywords = ("cable", "vb-audio", "virtual", "voicemeeter")
    try:
        devices = sd.query_devices()
        for idx, dev in enumerate(devices):
            if dev.get("max_input_channels", 0) < 1:
                continue
            name_lower = dev["name"].lower()
            if any(k in name_lower for k in keywords) and _is_mme_or_ds(dev):
                logger.info(f"VB-Cable auto-detected: [{idx}] {dev['name']}")
                return idx
    except Exception as exc:
        logger.error(f"Device scan error: {exc}")
    return None


def find_wasapi_loopback_device() -> Optional[int]:
    """
    Fallback: return the first WASAPI input device (which in WASAPI loopback
    mode will capture what's playing on the default output).
    """
    try:
        devices = sd.query_devices()
        for idx, dev in enumerate(devices):
            if dev.get("max_input_channels", 0) < 1:
                continue
            if _is_wasapi(dev):
                logger.info(f"WASAPI loopback fallback: [{idx}] {dev['name']}")
                return idx
    except Exception as exc:
        logger.error(f"Device scan error: {exc}")
    return None


def list_all_input_devices():
    """Print all input-capable devices to help the user pick one."""
    print("\n+----------------------------------------------------------+")
    print("|              Available Input Devices                     |")
    print("+----------------------------------------------------------+")
    try:
        for idx, dev in enumerate(sd.query_devices()):
            if dev.get("max_input_channels", 0) < 1:
                continue
            try:
                api = sd.query_hostapis(dev["hostapi"])["name"]
            except Exception:
                api = "?"
            print(f"|  [{idx:>3}] {dev['name']:<35} ({api})")
    except Exception as exc:
        print(f"|  Error: {exc}")
    print("+----------------------------------------------------------+\n")


# ─────────────────────────────────────────────────────────────────────────────
# AUDIO CAPTURE ENGINE
# sounddevice callback → bounded asyncio.Queue
# ─────────────────────────────────────────────────────────────────────────────

class AudioCapture:
    """
    Runs a sounddevice InputStream in a background thread.
    Audio frames are placed into an asyncio-compatible queue for the
    WebSocket sender to consume.
    """

    def __init__(
        self,
        device_index: Optional[int],
        gain: float,
        send_queue: asyncio.Queue,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._device_index = device_index
        self._gain = gain
        self._send_queue = send_queue
        self._loop = loop
        self._stream: Optional[sd.InputStream] = None
        self._stop = threading.Event()
        self._frames_captured = 0
        self._overflows = 0

    def _callback(
        self,
        indata: np.ndarray,
        frames: int,
        time_info,
        status: sd.CallbackFlags,
    ) -> None:
        """
        Called by sounddevice on every audio block.
        Runs in the PortAudio real-time thread — MUST NOT block.
        """
        if status.input_overflow:
            self._overflows += 1
            if self._overflows % 50 == 1:
                logger.warning(f"Input overflow (total: {self._overflows})")

        # Apply gain and clamp
        frame = indata[:, 0:CHANNELS] * self._gain  # shape: (frames, CHANNELS)
        np.clip(frame, -1.0, 1.0, out=frame)

        # Convert float32 → int16  (32768 * float32 → PCM16)
        int16_frame = (frame * 32767).astype(np.int16)

        # Flatten to bytes (L,R,L,R for stereo or just L for mono)
        raw_bytes = int16_frame.tobytes()

        self._frames_captured += frames

        # Thread-safe push to asyncio queue
        try:
            self._loop.call_soon_threadsafe(self._enqueue, raw_bytes)
        except RuntimeError:
            pass  # Loop is closing

    def _enqueue(self, raw_bytes: bytes) -> None:
        """Called in the asyncio thread via call_soon_threadsafe."""
        if self._send_queue.full():
            try:
                self._send_queue.get_nowait()  # Drop oldest
            except asyncio.QueueEmpty:
                pass
        try:
            self._send_queue.put_nowait(raw_bytes)
        except asyncio.QueueFull:
            pass

    def start(self) -> None:
        dev_name = "auto-detect" if self._device_index is None else str(self._device_index)
        logger.info(f"Opening audio capture on device [{dev_name}] ...")
        logger.info(f"  Sample rate : {SAMPLE_RATE} Hz")
        logger.info(f"  Channels    : {CHANNELS}")
        logger.info(f"  Block size  : {BLOCKSIZE} frames (~{BLOCKSIZE/SAMPLE_RATE*1000:.1f} ms)")
        logger.info(f"  Gain        : {self._gain:.2f}x")

        self._stream = sd.InputStream(
            device=self._device_index,
            samplerate=SAMPLE_RATE,
            channels=max(2, CHANNELS),  # Usually needs at least 2ch; we downmix after
            dtype=DTYPE_CAPTURE,
            blocksize=BLOCKSIZE,
            latency="low",
            callback=self._callback,
        )
        self._stream.start()
        logger.info("Audio capture STARTED ✓")

    def stop(self) -> None:
        self._stop.set()
        if self._stream:
            try:
                self._stream.stop()
                self._stream.close()
            except Exception:
                pass
            self._stream = None
        logger.info(f"Audio capture stopped. Frames captured: {self._frames_captured:,}")


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET UPLINK — with exponential back-off reconnection
# ─────────────────────────────────────────────────────────────────────────────

async def run_uplink(send_queue: asyncio.Queue, relay_url: str) -> None:
    """
    Drain send_queue and forward chunks to the cloud relay over WebSocket.
    Auto-reconnects with exponential back-off on any disconnect.
    """
    delay = RECONNECT_DELAY_INIT

    while True:
        logger.info(f"Connecting to relay: {relay_url}")
        try:
            async with websockets.connect(
                relay_url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                # Allow large binary messages (up to 16 MB)
                max_size=16 * 1024 * 1024,
                # Increase open_timeout for slow Render cold starts
                open_timeout=30,
            ) as ws:
                logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                logger.info("  UPLINK CONNECTED — streaming audio ↑")
                logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                delay = RECONNECT_DELAY_INIT  # Reset back-off on success

                while True:
                    chunk: bytes = await send_queue.get()
                    
                    # Dynamic batching: if the queue has backed up, pull multiple chunks 
                    # into a single WebSocket frame to reduce TCP overhead and recover fast.
                    batch = bytearray(chunk)
                    while not send_queue.empty() and len(batch) < 16384:
                        try:
                            batch.extend(send_queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                            
                    await ws.send(bytes(batch))

        except (ConnectionClosedOK, ConnectionClosedError) as exc:
            logger.warning(f"WebSocket closed: {exc}. Reconnecting in {delay:.1f}s …")
        except OSError as exc:
            logger.error(f"Network error: {exc}. Reconnecting in {delay:.1f}s …")
        except WebSocketException as exc:
            logger.error(f"WebSocket error: {exc}. Reconnecting in {delay:.1f}s …")
        except Exception as exc:
            logger.error(f"Unexpected uplink error: {exc!r}. Reconnecting in {delay:.1f}s …")

        await asyncio.sleep(delay)
        delay = min(delay * 2, RECONNECT_DELAY_MAX)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="VB-Audio → Cloud Relay broadcaster",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--device", type=int, default=None,
                   help="sounddevice input device index. Omit for auto-detect.")
    p.add_argument("--gain", type=float, default=GAIN,
                   help="Audio gain multiplier (1.0 = unity).")
    p.add_argument("--url", type=str, default=RELAY_URL,
                   help="Cloud relay WebSocket URL.")
    p.add_argument("--list-devices", action="store_true",
                   help="Print all input devices and exit.")
    return p.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    relay_url = args.url

    if "YOUR-APP-NAME" in relay_url:
        logger.error(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  ERROR: RELAY_URL is not configured!\n"
            "  Edit broadcaster.py and set RELAY_URL to your Render\n"
            "  deployment URL, e.g.:\n"
            "    wss://my-audio-relay.onrender.com/ws/broadcast_uplink\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        sys.exit(1)

    # Device selection
    device_index = args.device
    if device_index is None:
        device_index = find_vb_cable_device()
        if device_index is None:
            logger.warning("VB-Cable not found — falling back to first WASAPI loopback device.")
            device_index = find_wasapi_loopback_device()
        if device_index is None:
            logger.error(
                "No suitable WASAPI input device found.\n"
                "Run with --list-devices to see all devices, then use --device <index>."
            )
            sys.exit(1)

    loop = asyncio.get_event_loop()
    send_queue: asyncio.Queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    # Start audio capture (runs in PortAudio thread)
    capture = AudioCapture(
        device_index=device_index,
        gain=args.gain,
        send_queue=send_queue,
        loop=loop,
    )
    capture.start()

    # Graceful shutdown on Ctrl+C / SIGTERM
    shutdown_event = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support loop.add_signal_handler for all signals
            signal.signal(sig, _signal_handler)

    print()
    print("+----------------------------------------------------------+")
    print("|         Local Broadcaster - RUNNING                      |")
    print("|                                                          |")
    print(f"|  Target  : {relay_url[:50]:<50} |")
    print("|                                                          |")
    print("|  Press Ctrl+C to stop.                                   |")
    print("+----------------------------------------------------------+")
    print()

    try:
        # Run uplink until shutdown
        uplink_task = asyncio.create_task(run_uplink(send_queue, relay_url))
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [uplink_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    finally:
        capture.stop()
        logger.info("Broadcaster shut down cleanly.")


def main() -> None:
    args = parse_args()

    if args.list_devices:
        list_all_input_devices()
        sys.exit(0)

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
