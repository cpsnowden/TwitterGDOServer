import logging

from Celery import app
from src.AnalyticsService.Graphing.Graph import Graph
from src.api.Objects.MetaData import AnalyticsMeta

logger = logging.getLogger(__name__)

@app.task
def debug(x,y):
    print "Hello World"
    return x+y


@app.task
def generate_graph(analytics_id):

    logger.info("Attempting to do grph task")

    analytics_meta = AnalyticsMeta.objects.get(id=analytics_id)

    try:
       Graph.get_graph(analytics_meta)
    except Exception as e:
        logging.exception("Error getting cursor for dataset %s", analytics_meta.dataset_id)
        analytics_meta.status = "ERROR"
        analytics_meta.save()
        return

