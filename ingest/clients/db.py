from typing import Any, Optional
import asyncpg
import json
from logging import getLogger
from config import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
)

logger = getLogger(__name__)


class Database:
    def __init__(self):
        self.dsn = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.pool: Optional[asyncpg.Pool] = None

    async def is_connected(self) -> bool:
        """
        Check if the database connection pool is established.
        :return: True if connected, False otherwise.
        """
        return self.pool is not None

    async def connect(self):
        if not self.pool:
            try:
                # Set a timeout for connection
                self.pool = await asyncpg.create_pool(self.dsn, command_timeout=10)
                logger.info("Connected to PostgreSQL")
                await self.init_db()
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}")
                raise

    async def close(self):
        if self.pool:
            await self.pool.close()
            logger.info("Closed PostgreSQL connection")

    async def init_db(self):
        """Initialize the database schema."""
        create_table_query = """
    CREATE TABLE IF NOT EXISTS market_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        data JSONB NOT NULL
    );
    """
        # TODO: we have timestamp in table itself, we should use that instead of extracting from JSONB
        # Playing dirty in here:
        create_index_query = """
    CREATE UNIQUE INDEX IF NOT EXISTS unique_market_data_timestamp
    ON market_data ((data->>'timestamp'));
    """
        async with self.pool.acquire() as connection:
            await connection.execute(create_table_query)
            await connection.execute(create_index_query)
            logger.info("Database schema and unique index initialized")

    async def save_market_data(self, data: dict[str, Any]):
        """Save raw market data to the database."""
        if not self.pool:
            await self.connect()

        insert_query = """
        INSERT INTO market_data (data) VALUES ($1);
        """
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(insert_query, json.dumps(data))
                logger.info("Saved market data to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to save data to PostgreSQL: {e}")
