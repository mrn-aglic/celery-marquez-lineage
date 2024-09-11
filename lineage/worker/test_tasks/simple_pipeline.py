from lineage.celeryapp import app
from lineage.open_lineage.lineage_task import LineageTask


@app.task
def printer(*args, **kwargs):
    print("Hello from printer :-)")


@app.task
def forty_two(*args, **kwargs):
    return 42


@app.task
def lineage_pipeline(*args, **kwargs):
    return (forty_two.s() | printer.s(test="hello")).apply_async()
