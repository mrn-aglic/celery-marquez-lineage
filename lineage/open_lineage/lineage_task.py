from celery import Task
from openlineage.client.event_v2 import RunState
from openlineage.client.generated.parent_run import ParentRunFacet

from lineage.open_lineage import client


class LineageTask(Task):
    def __init__(self):
        self.client = None
        self.task_job_name = None

    def before_start(self, task_id, args, kwargs):
        self.client = client.LineageClient()

        self.parent_name = None
        self.parent_namespace = None

    def _get_parent_run_details(self):
        if self.request.parent_id:
            parent_run_details = self.client.get_job_details(self.request.parent_id)

            self.parent_name = parent_run_details["name"]
            self.parent_namespace = parent_run_details["namespace"]

    def _get_parent_facet(self) -> dict[str, ParentRunFacet] | None:
        if not self.request.parent_id:
            return None

        parent_run_facet = self.client.create_parent_run_facet(
            parent_run_id=self.request.parent_id,
            parent_job_name=self.parent_name,
            parent_namespace=self.parent_namespace,
        )

        return {"parent": parent_run_facet}

    def __call__(self, *args, **kwargs):

        print(args)
        print(kwargs)
        print(self.request.args)
        print(self.request.kwargs)

        job_name = self.client.get_job_name_from_task_name(self.name)

        self._get_parent_run_details()
        run_facets = self._get_parent_facet()

        print(f"JOB NAME: {job_name}")

        self.client.submit_event(
            event_type=RunState.START,
            run_id=self.request.id,
            name=job_name,
            run_facets=run_facets,
        )

        result = super().__call__(*args, **kwargs)

        return result

    def on_success(self, retval, task_id, args, kwargs):

        job_name = self.client.get_job_name_from_task_name(self.name)

        run_facets = self._get_parent_facet()

        self.client.submit_event(
            event_type=RunState.COMPLETE,
            run_id=task_id,
            name=job_name,
            run_facets=run_facets,
        )

    # pylint: disable=too-many-arguments
    def on_failure(self, exc, task_id, args, kwargs, einfo):

        error_facet = self.client.create_error_message_facet(
            error_message=str(exc),
            stack_trace=einfo.traceback,
        )

        error_facet = {"errorMessage": error_facet}

        run_facets = self._get_parent_facet()

        self.client.submit_event(
            event_type=RunState.FAIL,
            run_id=task_id,
            name=job_name,
            run_facets=run_facets,
        )
