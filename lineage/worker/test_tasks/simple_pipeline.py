from celery import group

from lineage.celeryapp import app


@app.task
def forty_two(*args, **kwargs):
    return 42


@app.task
def printer(*args, **kwargs):
    print("Hello from printer :-)")


@app.task
def lineage_pipeline(*args, **kwargs):
    return (forty_two.s() | printer.s(test="hello")).apply_async()


@app.task
def pipeline_group():
    return group(
        forty_two.s().set(lineage_name="forty_two_1"),
        forty_two.s().set(lineage_name="forty_two_2"),
        forty_two.s().set(lineage_name="forty_two_3"),
        forty_two.s().set(lineage_name="forty_two_4"),
    ).apply_async()
