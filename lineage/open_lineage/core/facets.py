from openlineage.client.event_v2 import Job, Run
from openlineage.client.generated.error_message_run import ErrorMessageRunFacet
from openlineage.client.generated.parent_run import ParentRunFacet


def create_error_facet(message, stack_trace) -> ErrorMessageRunFacet:
    return ErrorMessageRunFacet(
        message=message, stackTrace=stack_trace, programmingLanguage="python"
    )


def create_parent_run_facet(
    parent_run_id: str,
    parent_namespace: str,
    parent_job_name: str,
):
    return ParentRunFacet(
        run=Run(runId=parent_run_id),
        job=Job(name=parent_job_name, namespace=parent_namespace),
    )
