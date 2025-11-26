import redis
from app.core.config import settings

def get_redis_client():
    """
    Returns a Redis client instance.
    """
    return redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True
    )

redis_client = get_redis_client()
