from celery import Task
from openlineage.client.event_v2 import RunState

from lineage.open_lineage import client


class LineageTask(Task):
    def __init__(self):
        self.client = client.LineageClient()

        self.task_job_name = None

    def before_start(self, task_id, args, kwargs):
        self.task_job_name = self.client.get_job_name_from_task_name(self.name)

        self.client.submit_event(
            event_type=RunState.START,
            run_id=self.request.id,
            name=self.task_job_name,
        )

    def __call__(self, *args, **kwargs):
        self.client.submit_event(
            event_type=RunState.RUNNING,
            run_id=self.request.id,
            name=self.task_job_name,
        )

        result = super().__call__(*args, **kwargs)

        return result

    def on_success(self, retval, task_id, args, kwargs):
        self.client.submit_event(
            event_type=RunState.COMPLETE,
            run_id=task_id,
            name=self.task_job_name,
        )

    # pylint: disable=too-many-arguments
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.client.submit_event(
            event_type=RunState.FAIL,
            run_id=task_id,
            name=self.task_job_name,
        )
