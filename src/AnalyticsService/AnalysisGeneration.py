import logging


class AnalysisGeneration(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get(cls, analytics_meta):
        graph_type = analytics_meta.type

        func = cls.get_options_dict().get(graph_type,
                                          lambda x: cls._logger.error("Unknown type specified %s", analytics_meta))

        cls._logger.info("Attempting construction of type %s", graph_type)

        return func(analytics_meta)

    @classmethod
    def get_option_details(cls):

        return map(lambda option:{"type":option.get_type(), "args":option.get_args()}, cls.get_options())

    @classmethod
    def get_details(cls):
        return {"classification": cls.get_classification(),
                "options": cls.get_option_details()}

    @classmethod
    def get_options_dict(cls):
        return dict([(o.get_type(), o.get) for o in cls.get_options()])

    @classmethod
    def get_options(cls):
        return []

    @classmethod
    def get_classification(cls):
        return "Unspecified"