import asyncpg
import json
import logging
from typing import Optional, List
from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        self.dsn = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            try:
                self.pool = await asyncpg.create_pool(self.dsn, command_timeout=10)
                logger.info("Connected to PostgreSQL")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Closed PostgreSQL connection")

    async def get_recent_market_data(self, limit: int = 200) -> List[str]:
        """
        Fetch the most recent market data records.
        Returns a list of JSON strings, ordered chronologically (oldest to newest).
        """
        if not self.pool:
            await self.connect()

        query = """
        SELECT data
        FROM (
            SELECT data, timestamp
            FROM market_data
            ORDER BY timestamp DESC
            LIMIT $1
        ) sub
        ORDER BY timestamp ASC;
        """

        try:
            async with self.pool.acquire() as connection:
                rows = await connection.fetch(query, limit)
                # rows are Record objects. row['data'] is the JSON string (or dict if asyncpg decodes jsonb automatically)
                # asyncpg decodes jsonb to python objects (dict/list/etc) by default unless configured otherwise.
                # But wait, in ingest/clients/db.py, it inserts json.dumps(data).
                # Let's check if asyncpg decodes it back.
                # Usually asyncpg decodes JSONB to string if no codec is set, OR to python object if set_type_codec is used.
                # By default, asyncpg returns JSONB as string. Wait, no.
                # "By default, asyncpg will decode JSON/JSONB values to Python objects."
                # Let's assume it returns a dict/list, so we need to dump it back to string for SSE.

                results = []
                for row in rows:
                    data = row["data"]
                    if isinstance(data, str):
                        results.append(data)
                    else:
                        results.append(json.dumps(data))
                return results
        except Exception as e:
            logger.error(f"Failed to fetch recent market data: {e}")
            return []
