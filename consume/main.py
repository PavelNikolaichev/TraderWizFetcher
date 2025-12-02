import logging
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from config import validate_config, HEARTBEAT_INTERVAL
from managers.client_manager import ClientManager
from services.redis_service import RedisService
from services.db_service import DatabaseService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

validate_config()

client_manager = ClientManager()
redis_service = RedisService(client_manager)
db_service = DatabaseService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        await db_service.connect()

        # Initialize history from DB
        history = await db_service.get_recent_market_data(limit=200)
        redis_service.initialize_history(history)

        await redis_service.connect()
        await redis_service.start_reader()
    except Exception:
        logger.error("Failed to start services")
        # raise

    yield

    # Shutdown
    await redis_service.stop()
    await db_service.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def _client_event_generator(request: Request, q: asyncio.Queue[str]):
    try:
        while True:
            if await request.is_disconnected():
                logger.info("Detected client disconnect.")
                break

            try:
                payload_text = await asyncio.wait_for(
                    q.get(), timeout=HEARTBEAT_INTERVAL
                )
                safe_text = payload_text.replace("\r", "\\r").replace("\n", "\\n")
                yield f"data: {safe_text}\n\n"
            except asyncio.TimeoutError:
                yield ":\n\n"
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Error while reading from client queue; terminating generator."
                )
                break
    finally:
        logger.debug("Client event generator exiting.")


@app.get("/stream/price-feed")
async def sse_price_feed(request: Request, start_id: str | None = None):
    client_id, q = await client_manager.register_client()

    async def _gen():
        try:
            # Send historical data first
            try:
                historical_data = redis_service.get_history()
                for data_str in historical_data:
                    if await request.is_disconnected():
                        break
                    safe_text = data_str.replace("\r", "\\r").replace("\n", "\\n")
                    yield f"data: {safe_text}\n\n"
            except Exception:
                logger.exception("Failed to fetch/send historical data")

            # Then stream live events
            async for item in _client_event_generator(request, q):
                yield item
        finally:
            await client_manager.unregister_client(client_id)

    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(_gen(), media_type="text/event-stream", headers=headers)
