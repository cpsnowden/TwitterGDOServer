import logging
from AnalyticsService.Graphing.Graph import Graph
from dateutil import parser
import numpy as np
from datetime import timedelta

class TimeGraph(Graph):

    _logger = logging.getLogger(__name__)
    __arguments = [{"name": "timeLabelInterval", "prettyName": "Time between time indicator labels (hrs)",
                    "type": "integer", "default": 1.0},
                   {"name": "timeInterval", "prettyName": "Time interval to classify source of gravity (s)",
                    "type": "integer", "default": 1.0}
                   ]

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(TimeGraph, cls).get_args()

    @staticmethod
    def get_time_step(date, start, interval):
        return int(divmod((date - start).total_seconds(), interval)[0])

    @staticmethod
    def link_node_to_history(graph, history, node_id, user_id):

        past_user_node_id = TimeGraph.get_last_node(user_id, history)
        if past_user_node_id is not None:
            graph.add_edge(past_user_node_id, node_id, type="user")
        history[user_id] = node_id

    @staticmethod
    def get_last_node(common_id, history):
        if common_id in history:
            return history[common_id]
        else:
            return None

    @classmethod
    def add_time_indicator_nodes(cls, graph, interval, maximum, minimum):

        start_date = parser.parse(graph.graph["start_date"])
        max_time_step = graph.graph["max_step"]

        if interval > 0:
            for i in np.arange(0.0, max_time_step, interval):
                graph.add_node("TimeInd_T:" + str(start_date + timedelta(seconds=i)), type="TimeIndicator",
                               gravity_x=float(i), gravity_x_strength=float(100),
                               gravity_y=float(maximum), gravity_y_strength=float(100))

                graph.add_node("TimeInd_B:" + str(start_date + timedelta(seconds=i)), type="TimeIndicator",
                               gravity_x=float(i), gravity_x_strength=float(100),
                               gravity_y=float(minimum), gravity_y_strength=float(100))

        return graph

    @classmethod
    def add_time_indicator_nodes_after_layout(cls, graph, start_date, max_time_step, max_y, min_y):
        pass