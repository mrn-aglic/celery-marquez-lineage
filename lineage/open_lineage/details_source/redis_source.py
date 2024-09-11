from redis import StrictRedis


def get_redis_instance():
    return StrictRedis(host="redis", port=6379, db=2)


def store_job_details(run_id: str, job_name: str, job_namespace: str):
    r = get_redis_instance()

    r.hset(run_id, mapping={"name": job_name, "namespace": job_namespace})
    r.expire(run_id, 86400)  # TTL = 86400 seconds (1 day)


def get_job_details(run_id: str):
    r = get_redis_instance()

    run_data = r.hgetall(run_id)

    if not run_data:
        return None

    return {k.decode("utf-8"): v.decode("utf-8") for k, v in run_data.items()}
