import logging

from Celery import app
from AnalyticsService.AnalyticsEngine import AnalyticsEngine
from api.Objects.MetaData import AnalyticsMeta

logger = logging.getLogger(__name__)

@app.task
def get_analytics(analytics_id):

    logger.info("Attempting to get analytics")

    analytics_meta = AnalyticsMeta.objects.get(id=analytics_id)

    # try:
    result = AnalyticsEngine.get(analytics_meta)

    # except Exception as e:
    #     logger.exception()
    #     result = False


    if not result:
        logger.exception("Error executing analytics task %s", analytics_meta.id)
        analytics_meta.status = "ERROR"
        analytics_meta.save()
    else:
        logger.info("Analytics task %s successful", analytics_meta.id)

