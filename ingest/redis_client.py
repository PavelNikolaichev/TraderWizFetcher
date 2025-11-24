import asyncio
import random
import redis.asyncio as aioredis
from logging import getLogger

from config import (
    REDIS_URL,
    REDIS_BACKOFF_BASE,
    REDIS_MAX_RETRIES,
)
from health import health

logger = getLogger(__name__)


class RedisPublisher:
    """
    Redis client with exponential backoff retry logic for publishing messages.
    """

    def __init__(self):
        self.client = aioredis.from_url(REDIS_URL, decode_responses=True)  # type: ignore

    async def publish_with_retry(self, channel: str, message: str):
        """
        Publish a message to a Redis channel with exponential backoff retry logic.
        :param channel: The Redis channel to publish to.
        :param message: The message to publish.
        :raises Exception: If publishing fails after maximum retries.
        """
        attempt = 0

        while True:
            try:
                await self.client.publish(channel, message)  # type: ignore
                health.redis_ready = True
                return

            except Exception as e:
                health.redis_ready = False
                attempt += 1

                if attempt > REDIS_MAX_RETRIES:
                    logger.error(f"Redis publish failed after {attempt} attempts: {e}")
                    raise

                delay = REDIS_BACKOFF_BASE * (2 ** (attempt - 1))
                delay += random.uniform(0, 0.3)  # to prevent thundering herd problem

                logger.warning(
                    f"Redis publish failed (attempt {attempt}/{REDIS_MAX_RETRIES}). "
                    f"Retrying in {delay:.2f}s"
                )

                await asyncio.sleep(delay)

    async def close(self):
        """
        Close the Redis client connection.
        """
        await self.client.close()
