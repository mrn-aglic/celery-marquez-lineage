import os

from celery import Celery

from . import celeryconfig
from .celeryconfig import task_queues

app = Celery("lineage", task_cls="lineage.open_lineage.lineage_task:LineageTask")

app.config_from_object(celeryconfig)

instance = os.environ.get("instance")

if instance == "scheduler":
    app.control.purge()
