import logging
from src.AnalyticsService.Graphing.GraphUtils import GraphUtils
from src.AnalyticsService.TwitterObj import Status


class RetweetGraph(object):
    _logger = logging.getLogger(__name__)

    __retweet_nodes = ("type", {"tweet": "blue", "source": "red", "target": "lime"})
    __retweet_edges = ("type", {"source": "gold", "target": "turquoise"})
    color = (__retweet_nodes, __retweet_edges)

    @classmethod
    def get_graph(cls, graph, cursor, schema_id, quantum_s):

        history = {}

        start_date = Status(cursor[0], schema_id).get_created_at()

        for status_json in cursor:
            status = Status(status_json, schema_id)

            time_step = int(divmod((status.get_created_at() - start_date).total_seconds(), quantum_s)[0])

            # Add tweet node
            tweet_id = str(status.get_id())
            graph.add_node(tweet_id,
                           label=status.get_text().replace("\n", " "),
                           type="tweet",
                           date=str(status.get_created_at()),
                           gravity_x=float(time_step),
                           gravity_y=float(0))

            source_user = status.get_user()
            RetweetGraph.add_user_node(graph, source_user, time_step, tweet_id, "source", history)

            target_user = status.get_retweet_status().get_user()
            RetweetGraph.add_user_node(graph, target_user, time_step, tweet_id, "target", history)

        return graph

    @staticmethod
    def add_user_node(graph, target_user, time_step, tweet_id, type, history):
        user_id = target_user.get_id()
        node_id = str(user_id) + ":" + tweet_id
        user_name = target_user.get_name()
        graph.add_node(node_id,
                       label="usr:" + user_name,
                       type=type,
                       gravity_x=float(time_step),
                       gravity_y=RetweetGraph.get_gravity_y("user", user_name))

        graph.add_edge(node_id, tweet_id, type=type)
        GraphUtils.link_node_to_history(graph, history, node_id, user_id)

    @staticmethod
    def get_gravity_y(node_type, tweet=None):

        if node_type == "user":
            if tweet == "StrongerIn":
                return float(-70)
            elif tweet == "vote_leave":
                return float(70)
            return float(0)
