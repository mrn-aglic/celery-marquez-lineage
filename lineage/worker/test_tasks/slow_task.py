import time

from lineage.celeryapp import app


@app.task
def slow_task():
    iterations = 30
    sleep_seconds = 1

    for i in range(iterations):
        time.sleep(sleep_seconds)
        print(f"Iteration: {i}")
