import os
from celery.schedules import crontab

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache", 
    "CACHE_DEFAULT_TIMEOUT": 300, 
    "CACHE_KEY_PREFIX": "superset_cache_", 
    "CACHE_REDIS_URL": "redis://redis:6379/1"
    }
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache", 
    "CACHE_DEFAULT_TIMEOUT": 300, 
    "CACHE_KEY_PREFIX": "superset_results_", ""
    "CACHE_REDIS_URL": "redis://redis:6379/2"
    }