import logging

from src.AnalyticsService.BasicAnalytics.Analytics import BasicAnalytics
from src.AnalyticsService.Graphing.Graph import Graph


class AnalyticsEngine(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_analytics(cls, analytics_meta):
        cls._logger.info("Attempting to run task %s", analytics_meta.type)

        analytics_options = {
            "Mention_Time_Graph": Graph.get_mention_time_graph,
            "Retweet_Time_Graph": Graph.get_retweet_time_graph,
            "Time_Distribution": BasicAnalytics.get_time_distribution,
            "Top_Users":BasicAnalytics.get_top_users,
            "Basic_Stats":BasicAnalytics.get_basic_stats,
            "Retweet_Community_Graph":Graph.get_community_retweet_graph
        }

        analytics_constructor = analytics_options[analytics_meta.type]
        return analytics_constructor(analytics_meta)


