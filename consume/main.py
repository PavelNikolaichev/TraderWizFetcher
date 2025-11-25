from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
import redis.asyncio as aioredis
import os
import logging
import asyncio
import json
import uuid
from typing import Dict, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)  # type: ignore
    app.state.clients = {}  # client_id -> asyncio.Queue
    app.state.reader_task = asyncio.create_task(_redis_reader_loop())
    try:
        await app.state.redis.ping()
        logger.info("Connected to Redis successfully.")
    except Exception:
        logger.exception("Unable to connect to Redis during startup.")
        app.state.reader_task.cancel()
        raise

    yield

    # Shutdown logic
    reader = getattr(app.state, "reader_task", None)
    if reader:
        reader.cancel()
        try:
            await reader
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Exception while awaiting reader_task shutdown.")

    redis_client = getattr(app.state, "redis", None)
    if redis_client:
        try:
            await redis_client.close()
            logger.info("Redis connection closed.")
        except Exception:
            logger.exception("Error while closing Redis connection.")


app = FastAPI(lifespan=lifespan)

REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_STREAM = os.getenv("REDIS_STREAM", "data_channel")

XREAD_BLOCK_MS = int(os.getenv("XREAD_BLOCK_MS", "5000"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))
CLIENT_QUEUE_MAXSIZE = int(os.getenv("CLIENT_QUEUE_MAXSIZE", "128"))
FANOUT_BATCH_SIZE = int(os.getenv("FANOUT_BATCH_SIZE", "10"))


def _validate_config() -> None:
    if not REDIS_URL:
        raise ValueError("REDIS_URL environment variable is not set.")


_validate_config()

logger.info(f"Configured to listen on Redis stream: {REDIS_STREAM}")


async def _redis_reader_loop() -> None:
    """
    One background task that performs XREAD on the stream and broadcasts to all
    currently connected client queues.
    """
    redis_client = getattr(app.state, "redis", None)
    if redis_client is None:
        logger.error("Redis client not available in reader loop.")
        return

    # Start reading only new messages by default
    last_id = "$"
    backoff = 0.1

    try:
        while True:
            try:
                streams = await redis_client.xread(
                    {REDIS_STREAM: last_id},
                    count=FANOUT_BATCH_SIZE,
                    block=XREAD_BLOCK_MS,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Redis xread failed; retrying with backoff.")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)
                continue

            # Reset backoff on success
            backoff = 0.1

            if not streams:
                continue

            messages_to_broadcast = []
            for _stream_name, messages in streams:
                for message_id, data in messages:
                    # normalize payload: prefer 'data' field, else whole data dict
                    try:
                        if isinstance(data, dict) and "data" in data:
                            payload = data["data"]
                            if not isinstance(payload, str):
                                payload_text = json.dumps(payload, ensure_ascii=False)
                            else:
                                payload_text = payload
                        else:
                            payload_text = json.dumps(data, ensure_ascii=False)
                    except Exception:
                        payload_text = json.dumps({"raw": str(data)})
                        logger.exception(
                            "Failed to parse message data; using fallback payload."
                        )

                    messages_to_broadcast.append((message_id, payload_text))

                    last_id = message_id

            if not messages_to_broadcast:
                continue

            clients = getattr(app.state, "clients", {})
            if not clients:
                continue

            # For each client queue, try to enqueue each message.
            # If the queue is full, drop the oldest entry then enqueue.
            for client_id, q in list(clients.items()):
                for _, payload_text in messages_to_broadcast:
                    try:
                        q.put_nowait(payload_text)
                    except asyncio.QueueFull:
                        # Drop oldest and try again (non-blocking).
                        try:
                            _ = q.get_nowait()
                        except Exception:
                            logger.debug(
                                "Client queue full and couldn't drop oldest; skipping enqueue for client %s",
                                client_id,
                            )
                            continue
                        try:
                            q.put_nowait(payload_text)
                        except asyncio.QueueFull:
                            logger.debug(
                                "Client queue still full after dropping oldest; skipping for client %s",
                                client_id,
                            )
                            continue

    except asyncio.CancelledError:
        logger.info("Redis reader loop cancelled; exiting.")
    except Exception:
        logger.exception("Unhandled exception in redis reader loop.")
    finally:
        logger.info("Redis reader loop terminated.")


async def _register_client() -> Tuple[str, asyncio.Queue[str]]:
    """
    Create a new bounded queue for a client and register it in app.state.clients.
    Returns client_id and queue.
    """
    client_id = str(uuid.uuid4())
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)

    clients = getattr(app.state, "clients", None)
    if clients is None:
        raise RuntimeError("Client hub not initialized")

    clients[client_id] = q
    logger.info("Registered client %s (total clients=%d)", client_id, len(clients))

    return client_id, q


async def _unregister_client(client_id: str) -> None:
    clients = getattr(app.state, "clients", None)
    if not clients:
        return
    q = clients.pop(client_id, None)
    if q:
        # drain the queue to release references
        try:
            while True:
                q.get_nowait()
        except Exception:
            pass
    logger.info(
        "Unregistered client %s (remaining clients=%d)", client_id, len(clients)
    )


async def _client_event_generator(request: Request, q: asyncio.Queue[str]):
    """
    Generator that yields SSE-formatted strings from the client's queue.
    Uses HEARTBEAT_INTERVAL to send periodic comments to keep connections alive.
    """
    try:
        while True:
            # If client disconnected from the HTTP connection, stop
            if await request.is_disconnected():
                logger.info("Detected client disconnect.")
                break

            try:
                # Wait for next message with heartbeat timeout
                payload_text = await asyncio.wait_for(
                    q.get(), timeout=HEARTBEAT_INTERVAL
                )
                # sanitize payload to avoid raw newlines breaking SSE lines
                safe_text = payload_text.replace("\r", "\\r").replace("\n", "\\n")
                yield f"data: {safe_text}\n\n"
            except asyncio.TimeoutError:
                # No message in heartbeat interval -> send SSE comment to keep intermediaries alive
                yield ":\n\n"
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Error while reading from client queue; terminating generator."
                )
                break
    finally:
        # generator exit (client disconnect or error) will be handled by caller to unregister
        logger.debug("Client event generator exiting.")


@app.get("/stream/price-feed")
async def sse_price_feed(request: Request, start_id: str | None = None):
    """
    SSE endpoint. Optional query param 'start_id' to set stream pointer:
      - not provided -> reads new messages only (default behavior of background reader)
      - this parameter is currently informational; to use replay you'd modify the background reader.
    """
    # Register client and create queue
    client_id, q = await _register_client()

    async def _gen():
        try:
            # Wrap the client's queue generator
            async for item in _client_event_generator(request, q):
                yield item
        finally:
            # Always clean up client state when generator ends
            await _unregister_client(client_id)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",  # disable buffering so events flow immediately
    }
    return StreamingResponse(_gen(), media_type="text/event-stream", headers=headers)
