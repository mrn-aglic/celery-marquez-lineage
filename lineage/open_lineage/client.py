from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunState
from openlineage.client.transport import HttpTransport
from openlineage.client.transport.http import HttpCompression, HttpConfig

from lineage.app import config
from lineage.open_lineage.consts import DEFAULT_NAMESPACE
from lineage.open_lineage.core import events, facets

http_config = HttpConfig(
    url=config.MARQUEZ_URL,
    endpoint="api/v1/lineage",
    timeout=120,
    verify=False,
    compression=HttpCompression.GZIP,
)

http_transport = HttpTransport(http_config)


class LineageClient:
    def __init__(self):
        self.lineage_client = OpenLineageClient(transport=http_transport)

    # pylint: disable=too-many-arguments
    def submit_event(
        self,
        event_type: RunState,
        run_id: str,
        name: str,
        run_facets: dict | None = None,
        namespace=DEFAULT_NAMESPACE,
    ):
        run_event = events.create_event(
            event_type=event_type,
            run_id=run_id,
            run_facets=run_facets,
            name=name,
            namespace=namespace,
        )

        self.lineage_client.emit(run_event)

    def get_job_name_from_task_name(self, task_name: str):
        return task_name.split(".")[-1]

    def create_error_message_facet(self, error_message, stack_trace):
        return facets.create_error_facet(
            message=error_message,
            stack_trace=stack_trace,
        )
