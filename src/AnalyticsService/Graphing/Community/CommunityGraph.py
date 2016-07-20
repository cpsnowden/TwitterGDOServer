import logging
from AnalyticsService.Graphing.Graph import Graph
import abc

class CommunityGraph(Graph):

    _logger = logging.getLogger(__name__)
    __arguments = []

    @classmethod
    def get_arguments(cls):
        return cls.__arguments + super(CommunityGraph, cls).get_args()