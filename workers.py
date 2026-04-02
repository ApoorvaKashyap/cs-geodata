from rq import Worker

from src.utils.redis_client import get_redis_client

w = Worker(["layers", "id"], connection=get_redis_client())

if __name__ == "__main__":
    w.work()
