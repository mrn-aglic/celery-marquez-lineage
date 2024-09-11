from openlineage.client.generated.base import InputDataset, OutputDataset


def create_input_dataset(name: str, namespace: str) -> InputDataset:
    return InputDataset(name=name, namespace=namespace)


def create_output_dataset(name: str, namespace: str) -> OutputDataset:
    return OutputDataset(
        name=name,
        namespace=namespace,
    )
