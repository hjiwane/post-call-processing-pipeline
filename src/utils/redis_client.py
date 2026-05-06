try:
    import redis.asyncio as aioredis
except ModuleNotFoundError:  # pragma: no cover - local test fallback
    aioredis = None

from src.config import settings


class InMemoryRedis:
    """Tiny async Redis fallback for local tests when redis is not installed."""

    def __init__(self):
        self._values = {}

    async def get(self, key: str):
        return self._values.get(key)

    async def set(self, key: str, value, ex=None):
        self._values[key] = value
        return True

    async def incrby(self, key: str, amount: int):
        self._values[key] = int(self._values.get(key) or 0) + int(amount)
        return self._values[key]

    async def expire(self, key: str, seconds: int):
        return key in self._values

    async def ttl(self, key: str):
        return 60 if key in self._values else -2

    async def rpush(self, key: str, value):
        self._values.setdefault(key, []).append(value)
        return len(self._values[key])

    async def lpop(self, key: str):
        values = self._values.get(key) or []
        if not values:
            return None
        return values.pop(0)

    async def llen(self, key: str):
        return len(self._values.get(key) or [])


if aioredis is not None:
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
else:
    redis_client = InMemoryRedis()


async def get_redis():
    return redis_client
