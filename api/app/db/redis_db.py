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

import redis

redis = redis.Redis(host="redis")


def remove_cache(path):
    """
    Remove the cache for the specified path.

    Args:
        path (str): The path to remove the cache for.

    Returns:
        None
    """
    # Pattern da cercare nelle chiavi (ad esempio 'testop')
    pattern = "*{}*".format(path)

    # Itera su tutte le chiavi che corrispondono al pattern
    cursor = 0
    while True:
        cursor, keys = redis.scan(cursor=cursor, match=pattern)
        if keys:
            # Cancella le chiavi trovate
            redis.delete(*keys)
        if cursor == 0:
            break
