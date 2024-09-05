from celery.utils.log import get_task_logger

from lineage.app import config
from lineage.celeryapp import app

logger = get_task_logger(__name__)


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    try:
        if config.is_scheduler():
            return

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
