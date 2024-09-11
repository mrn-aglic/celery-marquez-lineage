from datetime import UTC, datetime

from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.generated.base import InputDataset, OutputDataset, RunFacet


def create_job(namespace: str, name: str) -> Job:
    return Job(namespace=namespace, name=name)


def create_run(run_id: str, run_facets: dict[str, RunFacet] | None) -> Run:
    return Run(runId=run_id, facets=run_facets)


# pylint: disable=too-many-arguments
def create_event(
    event_type: RunState,
    run_id: str,
    run_facets: dict[str, RunFacet] | None,
    name: str,
    namespace,
    inputs: list[InputDataset] = None,
    outputs: list[OutputDataset] = None,
):
    return RunEvent(
        eventType=event_type,
        eventTime=datetime.now(UTC).isoformat(),
        run=create_run(run_id=run_id, run_facets=run_facets),
        job=create_job(namespace, name),
        producer=name,
        inputs=inputs,
        outputs=outputs,
    )
