from lineage.celeryapp import app
from lineage.open_lineage.lineage_task import LineageTask


@app.task(base=LineageTask, bind=True)
def failure(self, *args, **kwargs):
    raise ValueError("I'm supposed to fail")


@app.task(base=LineageTask)
def pipeline():
    return failure.s().apply_async()
