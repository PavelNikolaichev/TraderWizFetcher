from aiohttp import ClientTimeout, ClientSession
from logging import getLogger
from health import health

logger = getLogger(__name__)


async def fetch_json(session: ClientSession, url: str, api_key: str = "") -> dict:
    """
    Fetch JSON data from a URL with error handling and health status update.
    :param session: The aiohttp ClientSession to use for the request.
    :param url: The URL to fetch data from.
    :return: The JSON data as a dictionary.
    :raises Exception: If the fetch fails.
    """
    try:
        # TODO: handle auth better
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        async with session.get(
            url, headers=headers, timeout=ClientTimeout(total=10)
        ) as resp:
            resp.raise_for_status()

            data = await resp.json()
            health.last_fetch_ok = True

            return data
    except Exception as e:
        health.last_fetch_ok = False
        logger.error("Fetch failed", exc_info=e)

        raise
