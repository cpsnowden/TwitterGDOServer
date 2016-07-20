import logging
from AnalyticsService.AnalysisGeneration import AnalysisGeneration
from AnalyticsService.Analytics.BasicStats import BasicStats
from AnalyticsService.Analytics.TimeStats import TimeDistribution
from AnalyticsService.Analytics.TopUsers import TopUsers


class AnalyticsGeneration(AnalysisGeneration):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_options(cls):
        return [BasicStats,
                TimeDistribution,
                TopUsers]

    @classmethod
    def get_classification(cls):
        return "Analytics"