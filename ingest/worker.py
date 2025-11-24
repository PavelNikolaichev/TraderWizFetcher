import asyncio
import json
from logging import getLogger
import aiohttp

from config import DATA_SOURCE_API_KEY, DATA_SOURCE_URL, REDIS_CHANNEL, FETCH_INTERVAL
from redis_client import RedisPublisher
from http_client import fetch_json
from db import Database

logger = getLogger(__name__)


async def run_worker():
    """
    Start the ingestion worker that fetches data from a specified URL
    and publishes it to a Redis channel at regular intervals.
    """
    redis = RedisPublisher()
    db = Database()

    logger.info(f"Starting ingestion worker service... Fetching from {DATA_SOURCE_URL}")

    try:
        await db.connect()
    except Exception as e:
        logger.error(f"Could not connect to DB, exiting: {e}")
        return

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    data = await fetch_json(session, DATA_SOURCE_URL, DATA_SOURCE_API_KEY)
                    if data:
                        logger.info(f"Fetched data: {len(str(data))} bytes")

                        await db.save_market_data(data)

                        message = json.dumps(data)
                        await redis.publish_with_retry(REDIS_CHANNEL, message)

                        logger.info(
                            "Message published",
                            extra={
                                "channel": REDIS_CHANNEL,
                                "message_len": len(message),
                            },
                        )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error in ingestion worker: {e}")

                await asyncio.sleep(FETCH_INTERVAL)
    finally:
        await db.close()
