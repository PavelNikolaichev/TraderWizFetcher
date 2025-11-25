from typing import Optional
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
        async with self.pool.acquire() as connection:
            await connection.execute(create_table_query)
            logger.info("Database schema initialized")

    async def save_market_data(self, data: dict):
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
