"""OKX WebSocket client - handles only connection, subscription, and raw message decoding."""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

import msgspec
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from src.time_helpers import now_epoch_ms

# Temporary debug: verify import path
import src.time_helpers as th
print(f"DEBUG time_helpers import: {th.__file__}")
# Compare direct vs helper (one-time check)
_direct_ns = time.monotonic_ns()
_helper_ns = th.now_mono_ns()
print(f"DEBUG time.monotonic_ns()={_direct_ns}, now_mono_ns()={_helper_ns}, diff={_direct_ns - _helper_ns}")


_decoder = msgspec.json.Decoder()


async def okx_stream(
    url: str,
    symbols: list[str],
    channels: list[str],
    stop: asyncio.Event,
) -> AsyncIterator[tuple[int, int, int, dict]]:
    """
    Async generator that yields (ts_recv_epoch_ms, ts_recv_mono_ns, ts_decoded_mono_ns, msg_dict) tuples.
    
    Args:
        url: WebSocket URL (e.g., "wss://ws.okx.com:8443/ws/v5/public")
        symbols: List of trading pairs (e.g., ["BTC-USDT", "ETH-USDT"])
        channels: List of channel names (e.g., ["books5", "trades"])
        stop: Event to signal shutdown
        
    Yields:
        Tuple of (ts_recv_epoch_ms, ts_recv_mono_ns, ts_decoded_mono_ns, msg_dict) where:
        - ts_recv_epoch_ms: epoch ms (for exchange→recv latency)
        - ts_recv_mono_ns: monotonic ns at frame receipt (for recv→decode latency)
        - ts_decoded_mono_ns: monotonic ns after JSON decode (for decode→proc latency)
        - msg_dict: decoded JSON message
    """
    # Build subscription arguments (all symbols × all channels)
    sub_args = [{"channel": ch, "instId": sym} for sym in symbols for ch in channels]
    sub_payload = {"op": "subscribe", "args": sub_args}
    sub_text = msgspec.json.encode(sub_payload).decode("utf-8")
    
    attempt = 0
    while not stop.is_set():
        try:
            async with connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_queue=1024,
                open_timeout=10.0,
            ) as ws:
                # Send subscription
                await ws.send(sub_text)
                attempt = 0  # Reset on successful connection
                
                # Read frames
                async for raw in ws:
                    if stop.is_set():
                        break
                    
                    # Capture receive timestamp IMMEDIATELY when frame arrives (first line after async for)
                    ts_recv_epoch_ms = now_epoch_ms()  # epoch: for exchange→recv latency
                    ts_recv_mono_ns = time.monotonic_ns()  # monotonic: for recv→decode latency (direct call for reliability)
                    
                    # Decode JSON (this is the work between recv and decoded timestamps)
                    try:
                        if isinstance(raw, bytes):
                            msg = _decoder.decode(raw)
                        elif isinstance(raw, str):
                            msg = _decoder.decode(raw.encode("utf-8"))
                        else:
                            continue
                        
                        if not isinstance(msg, dict):
                            continue
                        
                        # Capture decode timestamp IMMEDIATELY after decode completes (before any other processing)
                        # NOTE: On Windows, time.monotonic_ns() has microsecond precision (values end in 000000).
                        # If decode completes within the same microsecond as frame receipt, ts_decoded_mono_ns
                        # will equal ts_recv_mono_ns, resulting in 0ns latency. This is expected on Windows.
                        ts_decoded_mono_ns = time.monotonic_ns()  # direct call for reliability
                        
                        # Temporary debug: print timestamps for first 5 messages
                        if not hasattr(okx_stream, '_debug_count'):
                            okx_stream._debug_count = 0
                        if okx_stream._debug_count < 5:
                            recv_to_decode = ts_decoded_mono_ns - ts_recv_mono_ns
                            print(f"DEBUG okx_ws[{okx_stream._debug_count}]: recv={ts_recv_mono_ns} decoded={ts_decoded_mono_ns} | recv->decode={recv_to_decode}ns")
                            okx_stream._debug_count += 1
                            
                        yield (ts_recv_epoch_ms, ts_recv_mono_ns, ts_decoded_mono_ns, msg)
                        
                    except msgspec.DecodeError:
                        # Skip invalid JSON
                        continue
                        
        except (ConnectionClosed, OSError, asyncio.TimeoutError):
            if stop.is_set():
                break
            # Exponential backoff with jitter
            base_delay = min(30.0, 0.25 * (2 ** attempt))
            delay = base_delay * (0.8 + 0.4 * (time.time() % 1.0))
            attempt += 1
            await asyncio.sleep(delay)
        except Exception:
            if stop.is_set():
                break
            await asyncio.sleep(1.0)
