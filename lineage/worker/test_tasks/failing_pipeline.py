from lineage.celeryapp import app


@app.task
def failure(*args, **kwargs):
    raise ValueError("I'm supposed to fail")


@app.task
def pipeline():
    return failure.s().apply_async()
