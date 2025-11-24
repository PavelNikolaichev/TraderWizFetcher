from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import redis.asyncio as aioredis
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "data_channel")


def _validate_config():
    """
    Validate essential configuration parameters. Should be called only when the module is imported.
    Raises ValueError if any required configuration is missing or invalid."""
    if REDIS_URL == "":
        raise ValueError("REDIS_URL environment variable is not set.")


redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)  # type: ignore


async def event_generator():
    """
    Async generator that yields Server-Sent Events (SSE) from Redis Pub/Sub channel.
    """
    pubsub = redis_client.pubsub()  # type: ignore
    await pubsub.subscribe(REDIS_CHANNEL)  # type: ignore

    try:
        while True:
            message = await pubsub.get_message(  # type: ignore
                ignore_subscribe_messages=True, timeout=3.0
            )
            logger.info(f"Received message from Redis: {message}")
            if message:
                yield f"data: {message['data']}\n\n"
    except Exception as e:
        logger.error(f"Error in event_generator: {e}")


@app.get("/stream/price-feed")
async def sse_price_feed():
    """
    Endpoint to stream price feed data as Server-Sent Events (SSE).
    """
    return StreamingResponse(event_generator(), media_type="text/event-stream")


_validate_config()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
