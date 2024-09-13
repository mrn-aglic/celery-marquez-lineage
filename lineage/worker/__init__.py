from celery.utils.log import get_task_logger

from lineage.app import config
from lineage.celeryapp import app
from lineage.worker.test_tasks import simple_pipeline

logger = get_task_logger(__name__)


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    try:
        if config.is_scheduler():
            return

        simple_pipeline.lineage_pipeline.s().apply_async(countdown=35)
        # failling_pipeline.pipeline.s().apply_async(countdown=35)

    except Exception as e:
        logger.error(f"An exception occurred: {e}")
