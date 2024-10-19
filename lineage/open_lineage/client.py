import typing

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunState
from openlineage.client.generated.base import (
    InputDataset,
    OutputDataset,
    RunEvent,
    RunFacet,
)
from openlineage.client.transport import HttpTransport
from openlineage.client.transport.http import HttpCompression, HttpConfig
from requests.exceptions import HTTPError
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from lineage.app import config
from lineage.open_lineage.consts import DEFAULT_NAMESPACE
from lineage.open_lineage.core import datasets, events, facets
from lineage.open_lineage.details_source import marquez_source, redis_source

http_config = HttpConfig(
    url=config.MARQUEZ_URL,
    endpoint="api/v1/lineage",
    timeout=120,
    verify=False,
    compression=HttpCompression.GZIP,
)

http_transport = HttpTransport(http_config)


class HttpError500(HTTPError):
    pass


class LineageClient:
    def __init__(self):
        self.lineage_client = OpenLineageClient(transport=http_transport)

    @retry(
        retry=retry_if_exception_type(HttpError500),
        stop=stop_after_delay(5),
        wait=wait_fixed(1),
    )
    def _retry_emit(self, run_event: RunEvent):
        try:
            self.lineage_client.emit(run_event)
        except HTTPError as http_error:
            if http_error.response.status_code == 500:
                print("Got 500 internal server error.")
                raise HttpError500(http_error) from http_error
            raise http_error

    # pylint: disable=too-many-arguments
    def submit_event(
        self,
        event_type: RunState,
        run_id: str,
        name: str,
        run_facets: dict[str, RunFacet] | None = None,
        namespace=DEFAULT_NAMESPACE,
        inputs: typing.Optional[list[InputDataset]] = None,
        outputs: typing.Optional[list[OutputDataset]] = None,
    ):
        inputs = inputs or []
        outputs = outputs or []

        run_event = events.create_event(
            event_type=event_type,
            run_id=run_id,
            run_facets=run_facets,
            name=name,
            namespace=namespace,
            inputs=inputs,
            outputs=outputs,
        )

        self._retry_emit(run_event)
        # self.lineage_client.emit(run_event)

    def get_job_name_from_task_name(self, task_name: str):
        return task_name.split(".")[-1]

    def create_error_message_facet(self, error_message, stack_trace):
        return facets.create_error_facet(
            message=error_message,
            stack_trace=stack_trace,
        )

    def create_parent_run_facet(
        self,
        parent_run_id: str,
        parent_namespace: str,
        parent_job_name: str,
    ):
        return facets.create_parent_run_facet(
            parent_run_id=parent_run_id,
            parent_namespace=parent_namespace,
            parent_job_name=parent_job_name,
        )

    def create_input_dataset(self, name: str, namespace: str):
        return datasets.create_input_dataset(name, namespace)

    def create_output_dataset(self, name: str, namespace: str):
        return datasets.create_output_dataset(name, namespace)

    def get_job_details_marquez(self, job_id: str):
        return marquez_source.get_job_details(job_id)

    def store_job_details_redis(
        self, run_id: str, name: str, namespace: str, parent_prefix: str | None = None
    ):
        job_name = f"{parent_prefix}.{name}" if parent_prefix is not None else name

        redis_source.store_job_details(
            run_id=run_id,
            job_name=job_name,
            job_namespace=namespace,
        )

    def get_job_details_redis(self, run_id: str):
        return redis_source.get_job_details(run_id=run_id)

    def get_job_details(self, job_id: str):
        job_details = self.get_job_details_redis(job_id)

        if job_details is None:
            job_details = self.get_job_details_marquez(job_id)

        return job_details
