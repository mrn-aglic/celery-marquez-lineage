from celery.utils.log import get_task_logger

from lineage.app import config
from lineage.celeryapp import app

logger = get_task_logger(__name__)

if config.is_dev():
    import logging

    logging.basicConfig(level=logging.INFO)


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    try:
        logger.info("SETUP PERIODIC TASK")

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
