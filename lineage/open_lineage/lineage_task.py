from celery import Task
from openlineage.client.event_v2 import RunState
from openlineage.client.generated.parent_run import ParentRunFacet

from lineage.open_lineage import client
from lineage.open_lineage.consts import DEFAULT_NAMESPACE


class LineageTask(Task):
    def __init__(self):
        self.client = None
        self.task_job_name = None

        self.parent_name = None
        self.parent_namespace = None

    def _get_input_dataset(self):
        if self.parent_name is None:
            return None

        dataset_name = self.parent_name.split(".")[-1]

        return [
            self.client.create_input_dataset(
                name=dataset_name,
                namespace=DEFAULT_NAMESPACE,
            )
        ]

    def _get_output_dataset(self, retval):

        if retval is None:
            return None

        return [
            self.client.create_output_dataset(
                name=self.task_job_name,
                namespace=DEFAULT_NAMESPACE,
            )
        ]

    def _get_parent_facet(self) -> dict[str, ParentRunFacet] | None:
        if not self.request.parent_id:
            return {}

        parent_run_facet = self.client.create_parent_run_facet(
            parent_run_id=self.request.parent_id,
            parent_job_name=self.parent_name,
            parent_namespace=self.parent_namespace,
        )

        return {"parent": parent_run_facet}

    def _get_job_name_from_properties(self):
        prop_name = "lineage_name"

        return self.request.properties.get(prop_name, None)

    def _get_job_name(self):
        return (
            self._get_job_name_from_properties()
            or self.client.get_job_name_from_task_name(self.name)
        )

    def before_start(self, task_id, args, kwargs):

        self.client = client.LineageClient()

        self.task_job_name = self._get_job_name()

        self._get_parent_run_details()

        run_facets = self._get_parent_facet()

        self.client.submit_event(
            event_type=RunState.RUNNING,
            run_id=task_id,
            name=self.task_job_name,
            run_facets=run_facets,
            inputs=self._get_input_dataset(),
        )

    def _get_parent_run_details(self):
        if self.request.parent_id:

            parent_run_details = self.client.get_job_details(self.request.parent_id)

            self.parent_name = parent_run_details["name"]
            self.parent_namespace = parent_run_details["namespace"]

    def __call__(self, *args, **kwargs):

        self._get_parent_run_details()

        run_facets = self._get_parent_facet()

        self.client.submit_event(
            event_type=RunState.RUNNING,
            run_id=self.request.id,
            name=self.task_job_name,
            run_facets=run_facets,
            inputs=self._get_input_dataset(),
        )

        result = super().__call__(*args, **kwargs)

        self.client.store_job_details_redis(
            run_id=self.request.id,
            name=self.task_job_name,
            namespace=DEFAULT_NAMESPACE,
            parent_prefix=self.parent_name,
        )

        return result

    def on_success(self, retval, task_id, args, kwargs):

        run_facets = self._get_parent_facet()

        self.client.submit_event(
            event_type=RunState.COMPLETE,
            run_id=task_id,
            name=self.task_job_name,
            run_facets=run_facets,
            outputs=self._get_output_dataset(retval),
        )

    # pylint: disable=too-many-arguments
    def on_failure(self, exc, task_id, args, kwargs, einfo):

        error_facet = self.client.create_error_message_facet(
            error_message=str(exc),
            stack_trace=einfo.traceback,
        )

        error_facet = {"errorMessage": error_facet}

        run_facets = self._get_parent_facet()

        run_facets = {**error_facet, **run_facets}

        self.client.submit_event(
            event_type=RunState.FAIL,
            run_id=task_id,
            name=self.task_job_name,
            run_facets=run_facets,
        )
