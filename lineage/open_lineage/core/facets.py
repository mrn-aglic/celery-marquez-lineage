from openlineage.client.generated.error_message_run import ErrorMessageRunFacet


def create_error_facet(message, stack_trace) -> ErrorMessageRunFacet:
    return ErrorMessageRunFacet(
        message=message, stackTrace=stack_trace, programmingLanguage="python"
    )
