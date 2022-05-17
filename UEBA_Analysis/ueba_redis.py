import time

import redis


def get_redis_ins():
    pool = redis.ConnectionPool(host='127.0.0.1', port=5051, db=0,
                                decode_responses=True)
    r = redis.Redis(connection_pool=pool)
    return r


def set_hash(r, name, value):
    r.set(name, value, ex=60)


def get_hash(r, name):
    return r.get(name)
