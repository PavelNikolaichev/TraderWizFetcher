"""
Main entry point for the ingestion process.
Ingests the data from the specified source and pushes it into the queue.
"""

import asyncio
import signal
from logging import getLogger

from utils import init_logging
from worker import run_worker

logger = getLogger(__name__)

shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    logger.info("Shutdown signal received. Stopping ingestion process...")
    shutdown_event.set()


async def main():
    init_logging()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

    worker_task = asyncio.create_task(run_worker())

    await shutdown_event.wait()

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        logger.info("Worker stopped")

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
