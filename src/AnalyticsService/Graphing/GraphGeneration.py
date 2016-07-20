import logging

from AnalyticsService.Graphing.Community.RetweetCommunityGraph import RetweetCommunityGraph
from AnalyticsService.Graphing.Time.HashtagGraph import HashtagGraph
from AnalyticsService.Graphing.Time.HashtagGraphNEW import HashtagGraphRetweetNEW
from AnalyticsService.Graphing.Time.MentionTimeGraph import MentionTimeGraph
from AnalyticsService.Graphing.Time.RetweetTimeGraph import RetweetTimeGraph
from AnalyticsService.Graphing.Time.RetweetTimeGraphHistory import RetweetTimeGraphHistory
from AnalyticsService.Graphing.Time.HashtagGraphRetweetv2 import HashtagGraphRetweetv2
from AnalyticsService.Graphing.Time.HashtagGraphRetweet import HashtagGraphRetweet
from AnalyticsService.AnalysisGeneration import AnalysisGeneration
from AnalyticsService.Graphing.Community.CommunityGraphClassification import CommunityGraphClassification

class GraphGeneration(AnalysisGeneration):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_options(cls):
        return [HashtagGraph,
                HashtagGraphRetweet,
                HashtagGraphRetweetv2,
                HashtagGraphRetweetNEW,
                CommunityGraphClassification]

    @classmethod
    def get_classification(cls):
        return "Graph"