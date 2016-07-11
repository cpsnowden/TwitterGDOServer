import logging

from Celery import app
from src.AnalyticsService.AnalyticsEngine import AnalyticsEngine
from src.api.Objects.MetaData import AnalyticsMeta

logger = logging.getLogger(__name__)

@app.task
def get_analytics(analytics_id):

    logger.info("Attempting to get analytics")

    analytics_meta = AnalyticsMeta.objects.get(id=analytics_id)

    try:
        result = AnalyticsEngine.get_analytics(analytics_meta)
        print result
    except Exception as e:
        logger.exception()
        result = False

    print result
    if not result:
        logging.exception("Error executing analytics task %s", analytics_meta.dataset_id)
        analytics_meta.status = "ERROR"
        analytics_meta.save()