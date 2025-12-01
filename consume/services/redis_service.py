import asyncio
import json
import logging
import redis.asyncio as aioredis
from typing import Optional
from config import REDIS_URL, REDIS_STREAM, FANOUT_BATCH_SIZE, XREAD_BLOCK_MS
from managers.client_manager import ClientManager

logger = logging.getLogger(__name__)


class RedisService:
    def __init__(self, client_manager: ClientManager):
        self.redis: Optional[aioredis.Redis] = None
        self.client_manager = client_manager
        self.reader_task: Optional[asyncio.Task[None]] = None
        self.running = False

    async def connect(self):
        self.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            await self.redis.ping()
            logger.info("Connected to Redis successfully.")
        except Exception:
            logger.exception("Unable to connect to Redis.")
            raise

    async def start_reader(self):
        if not self.redis:
            await self.connect()
        self.running = True
        self.reader_task = asyncio.create_task(self._reader_loop())

    async def stop(self):
        self.running = False
        if self.reader_task:
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Exception while awaiting reader_task shutdown.")

        if self.redis:
            try:
                await self.redis.close()
                logger.info("Redis connection closed.")
            except Exception:
                logger.exception("Error while closing Redis connection.")

    async def _reader_loop(self) -> None:
        if self.redis is None:
            logger.error("Redis client not available in reader loop.")
            return

        last_id = "$"
        backoff = 0.1

        logger.info(f"Configured to listen on Redis stream: {REDIS_STREAM}")

        try:
            while self.running:
                try:
                    streams = await self.redis.xread(
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

                backoff = 0.1

                if not streams:
                    continue

                messages_to_broadcast = []
                for _stream_name, messages in streams:
                    for message_id, data in messages:
                        try:
                            if isinstance(data, dict) and "data" in data:
                                payload = data["data"]
                                if not isinstance(payload, str):
                                    payload_text = json.dumps(
                                        payload, ensure_ascii=False
                                    )
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

                if messages_to_broadcast:
                    await self.client_manager.broadcast(messages_to_broadcast)

        except asyncio.CancelledError:
            logger.info("Redis reader loop cancelled; exiting.")
        except Exception:
            logger.exception("Unhandled exception in redis reader loop.")
        finally:
            logger.info("Redis reader loop terminated.")
