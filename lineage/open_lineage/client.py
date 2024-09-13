from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunState
from openlineage.client.transport import HttpTransport
from openlineage.client.transport.http import HttpCompression, HttpConfig

from lineage.app import config
from lineage.open_lineage.consts import DEFAULT_NAMESPACE
from lineage.open_lineage.core import events

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

    def submit_event(
        self,
        event_type: RunState,
        run_id: str,
        name: str,
        namespace=DEFAULT_NAMESPACE,
    ):
        run_event = events.create_event(
            event_type=event_type,
            run_id=run_id,
            name=name,
            namespace=namespace,
        )

        self.lineage_client.emit(run_event)

    def get_job_name_from_task_name(self, task_name: str):
        return task_name.split(".")[-1]
