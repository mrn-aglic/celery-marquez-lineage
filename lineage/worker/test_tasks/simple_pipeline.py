from lineage.celeryapp import app


@app.task(bind=True)
def printer(self):
    print("Hello from printer :-)")


@app.task
def pipeline():
    return printer.s().apply_async()
