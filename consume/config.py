import os

REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_STREAM = os.getenv("REDIS_STREAM", "data_channel")

XREAD_BLOCK_MS = int(os.getenv("XREAD_BLOCK_MS", "5000"))
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "15"))
CLIENT_QUEUE_MAXSIZE = int(os.getenv("CLIENT_QUEUE_MAXSIZE", "128"))
FANOUT_BATCH_SIZE = int(os.getenv("FANOUT_BATCH_SIZE", "10"))

def validate_config() -> None:
    if not REDIS_URL:
        raise ValueError("REDIS_URL environment variable is not set.")
