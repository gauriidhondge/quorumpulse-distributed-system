import json
import redis
from common.config import REDIS_HOST, REDIS_PORT
_client: redis.Redis | None = None
QUEUE_KEY = 'urgent_requests'

def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    return _client

def publish_urgent_request(request: dict) -> None:
    r = get_redis()
    r.lpush(QUEUE_KEY, json.dumps(request))

def consume_urgent_request(timeout: int=5) -> dict | None:
    r = get_redis()
    item = r.brpop(QUEUE_KEY, timeout=timeout)
    return json.loads(item[1]) if item else None
