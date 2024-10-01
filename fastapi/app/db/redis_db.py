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
