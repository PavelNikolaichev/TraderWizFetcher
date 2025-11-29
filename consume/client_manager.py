import asyncio
import uuid
import logging
from typing import Dict, Tuple, List
from config import CLIENT_QUEUE_MAXSIZE

logger = logging.getLogger(__name__)


class ClientManager:
    def __init__(self):
        self.clients: Dict[str, asyncio.Queue[str]] = {}

    async def register_client(self) -> Tuple[str, asyncio.Queue[str]]:
        client_id = str(uuid.uuid4())
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=CLIENT_QUEUE_MAXSIZE)
        self.clients[client_id] = q
        logger.info(
            "Registered client %s (total clients=%d)", client_id, len(self.clients)
        )
        return client_id, q

    async def unregister_client(self, client_id: str) -> None:
        q = self.clients.pop(client_id, None)
        if q:
            # drain the queue to release references
            try:
                while not q.empty():
                    q.get_nowait()
            except Exception:
                pass
        logger.info(
            "Unregistered client %s (remaining clients=%d)",
            client_id,
            len(self.clients),
        )

    async def broadcast(self, messages: List[Tuple[str, str]]) -> None:
        if not self.clients:
            return

        for client_id, q in list(self.clients.items()):
            for _, payload_text in messages:
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
