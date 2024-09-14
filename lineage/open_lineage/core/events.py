from datetime import UTC, datetime

from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.generated.base import RunFacet


def create_job(namespace: str, name: str) -> Job:
    return Job(namespace=namespace, name=name)


def create_run(run_id: str, run_facets: dict[str, RunFacet] | None) -> Run:
    return Run(runId=run_id, facets=run_facets)


def create_event(
    event_type: RunState,
    run_id: str,
    run_facets: dict[str, RunFacet] | None,
    name: str,
    namespace,
):
    return RunEvent(
        eventType=event_type,
        eventTime=datetime.now(UTC).isoformat(),
        run=create_run(run_id=run_id, run_facets=run_facets),
        job=create_job(namespace, name),
        producer=name,
    )
