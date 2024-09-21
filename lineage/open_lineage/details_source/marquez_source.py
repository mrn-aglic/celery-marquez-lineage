import requests
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from lineage.app import config

_MARQUEZ_URL = config.MARQUEZ_URL


def _job_version_available(response) -> bool:
    return response.json().get("jobVersion") is not None


@retry(
    retry=retry_if_exception_type(ValueError),
    stop=stop_after_delay(5),
    wait=wait_fixed(1),
)
def get_job_details(job_id: str) -> dict:
    endpoint = f"{_MARQUEZ_URL}/api/v1/jobs/runs/{job_id}"

    response = requests.get(endpoint)

    if not _job_version_available(response):
        raise ValueError("JobVersion field not populated yet.")

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:

        if err.response.status_code == 404:
            return {}

        raise err

    return response.json()["jobVersion"]
