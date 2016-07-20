import logging

from AnalyticsService.Analytics.AnalyticsGeneration import AnalyticsGeneration
from AnalyticsService.Graphing.GraphGeneration import GraphGeneration


class AnalyticsEngine(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get(cls, analytics_meta):
        classification = analytics_meta.classification
        cls._logger.info("Attempting to run class: %s", analytics_meta.classification)

        func = cls.get_options_dict().get(classification,
                                        lambda x: cls._logger.error("Unknown type specified %s", analytics_meta.type))

        return func(analytics_meta)

    @classmethod
    def get_options(cls):
        return [AnalyticsGeneration,
                GraphGeneration]

    @classmethod
    def get_details(cls):

        return map(lambda option: {"classification": option.get_classification(),
                                   "types": option.get_option_details()}, cls.get_options())

    @classmethod
    def get_options_dict(cls):
        return dict([(o.get_classification(), o.get) for o in cls.get_options()])


#Check registered analysis options
if __name__ == '__main__':
    import pprint
    pprint.pprint(AnalyticsEngine.get_details())