import os

DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL", "")
DATA_SOURCE_API_KEY = os.getenv("DATA_SOURCE_API_KEY", "")
REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "data_channel")

POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "market_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 60 * 20)) # 20 minutes by default to fit into coincap free api tier
REDIS_MAX_RETRIES = int(os.getenv("REDIS_MAX_RETRIES", 5))
REDIS_BACKOFF_BASE = int(os.getenv("REDIS_BACKOFF_BASE", 1))  # seconds


def _validate_config():
    """
    Validate essential configuration parameters. Should be called only when the module is imported.
    Raises ValueError if any required configuration is missing or invalid.
    """

    if DATA_SOURCE_URL == "":
        raise ValueError("DATA_SOURCE_URL environment variable is not set.")
    if REDIS_URL == "":
        raise ValueError("REDIS_URL environment variable is not set.")
    if FETCH_INTERVAL <= 0:
        raise ValueError("FETCH_INTERVAL must be a positive integer.")
    if REDIS_MAX_RETRIES <= 0:
        raise ValueError("REDIS_MAX_RETRIES must be a positive integer.")
    if REDIS_BACKOFF_BASE <= 0:
        raise ValueError("REDIS_BACKOFF_BASE must be a positive integer.")


_validate_config()
