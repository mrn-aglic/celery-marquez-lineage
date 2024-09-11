from os import environ

ENVIRONMENT = environ.get("ENVIRONMENT", "").upper()
DEV_NAME = "DEV"

if ENVIRONMENT not in ("PROD", DEV_NAME):
    raise ValueError("Please set the environment variable to either DEV or PROD")


def is_scheduler():
    return environ.get("instance", "").upper() == "SCHEDULER"


def is_dev():
    return ENVIRONMENT == DEV_NAME


MARQUEZ_HOST = environ.get("MARQUEZ_HOST", "marquez")
MARQUEZ_PORT = environ.get("MARQUEZ_PORT", 9000)

MARQUEZ_URL = f"http://{MARQUEZ_HOST}:{MARQUEZ_PORT}"
