# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import redis as _redis_lib

redis = _redis_lib.Redis(host="redis")

# How long (in seconds) a cached query plan stays in Redis before auto-expiring.
# Override via the REDIS_CACHE_TTL environment variable.
CACHE_TTL = int(os.getenv("REDIS_CACHE_TTL", "300"))


def set_cache(key: str, value: str) -> None:
    """Store *value* under *key* in Redis with a fixed TTL.

    Using a TTL ensures stale entries are automatically evicted even if
    the corresponding write-path cache invalidation is somehow missed.

    Args:
        key:   The cache key (typically the request URL path + query string).
        value: The JSON-serialised query plan to cache.
    """
    redis.set(key, value, ex=CACHE_TTL)


def remove_cache(entity: str) -> None:
    """Delete all Redis cache entries that mention *entity* in their key.

    Called by create / update / delete endpoints so that subsequent GET
    requests re-fetch fresh data from the database instead of serving a
    stale cached query plan.

    Args:
        entity: The SensorThings entity name, e.g. ``"Things"``, ``"Sensors"``,
                ``"Observations"``, ``"Datastreams"``, etc.  The scan pattern
                used is ``*/<entity>*`` so it matches collection and single-item
                paths (``/Things``, ``/Things(1)``, ``/Things(1)/Datastreams``…)
                without accidentally invalidating unrelated entity caches.
    """
    pattern = f"*/{entity}*"
    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=pattern)
        if keys:
            redis.delete(*keys)
        if cursor == 0:
            break

