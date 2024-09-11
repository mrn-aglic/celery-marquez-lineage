import requests

from lineage.app import config

_MARQUEZ_URL = config.MARQUEZ_URL


def get_job_details(job_id: str) -> dict:
    endpoint = f"{_MARQUEZ_URL}/api/v1/jobs/runs/{job_id}"

    response = requests.get(endpoint)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 404:
            return {}

        raise err

    return response.json()["jobVersion"]
