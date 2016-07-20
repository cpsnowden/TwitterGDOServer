import logging

from AnalyticsService.AnalysisTemplate import AnalysisTemplate


class Analytics(AnalysisTemplate):
    _logging = logging.getLogger(__name__)

    __arguments = []

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(Analytics, cls).get_args()
