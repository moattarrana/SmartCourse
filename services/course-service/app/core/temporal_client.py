"""Cached Temporal client used by the publish endpoint to start workflows."""
import asyncio

from temporalio.client import Client

from app.core.config import settings

_client: Client | None = None
_lock = asyncio.Lock()


async def get_temporal_client() -> Client:
    global _client
    if _client is None:
        async with _lock:
            if _client is None:
                _client = await Client.connect(
                    settings.TEMPORAL_HOST, namespace=settings.TEMPORAL_NAMESPACE
                )
    return _client
