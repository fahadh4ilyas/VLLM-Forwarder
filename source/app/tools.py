import json
import hashlib
import logging
import redis.asyncio as redis
from aiokafka import AIOKafkaProducer
from .config import config

LOGGER = logging.getLogger('uvicorn.error')


# ==========================================
# Kafka
# ==========================================

def get_kafka_producer() -> AIOKafkaProducer | None:
    if not config.kafka_bootstrap_servers:
        return None
    return AIOKafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers.split(','),
    )


# ==========================================
# Redis
# ==========================================

def get_redis_connection_pool() -> redis.ConnectionPool:
    return redis.ConnectionPool(
        host=config.redis_host,
        port=config.redis_port,
        db=config.redis_db,
        password=config.redis_auth,
        decode_responses=True,
        socket_timeout=config.redis_timeout,
    )


def get_redis_client(pool: redis.ConnectionPool) -> redis.Redis:
    return redis.Redis.from_pool(connection_pool=pool)


# ==========================================
# Hashing
# ==========================================

def hash_json(json_obj: dict) -> str:
    canonical_json = json.dumps(json_obj, sort_keys=True)
    json_str = canonical_json.encode('utf-8')
    hash_obj = hashlib.sha256(json_str)
    return hash_obj.hexdigest()
