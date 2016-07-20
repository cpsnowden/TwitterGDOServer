import logging

from AnalyticsService.Junk.GraphUtils import GraphUtils
from src.AnalyticsService.TweetClassifier import TweetClassifier
from src.AnalyticsService.TwitterObj import Status


class MentionGraph(object):
    _logger = logging.getLogger(__name__)

    __mention_nodes = ("type", {"tweet": "blue", "source": "red", "target": "lime"})
    __mention_edges = ("type", {"input": "gold", "output": "turquoise"})
    color = (__mention_nodes, __mention_edges)

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
                           gravity_y=MentionGraph.get_gravity_y("tweet", status),
                           gravity_y_strength=MentionGraph.get_gravity_y_strength("tweet"),
                           gravity_x_strength = float(10.0))


            # Add the source user
            source_user = status.get_user()
            source_user_id = source_user.get_id()
            source_node_id = str(source_user_id) + ":" + tweet_id
            graph.add_node(source_node_id,
                           label="usr:" + source_user.get_name(),
                           type="source",
                           gravity_x=float(time_step),
                           gravity_y=MentionGraph.get_gravity_y("user", source_user.get_name()),
                           gravity_y_strength=MentionGraph.get_gravity_y_strength("user"),
                           gravity_x_strength=float(10.0))

            # Add edge from source user thread to tweet node
            graph.add_edge(source_node_id, tweet_id, type="input")

            GraphUtils.link_node_to_history(graph, history, source_node_id, source_user_id)

            for mention in status.get_mentions():
                # Add mention node

                target_user_id = str(mention.get_user_id())
                target_user_name = mention.get_user_name()
                target_node_id = str(target_user_id) + ":" + str(tweet_id)
                graph.add_node(target_node_id,
                               label=target_user_name,
                               type="target",
                               gravity_x=float(time_step),
                               gravity_y=MentionGraph.get_gravity_y("user", target_user_name),
                               gravity_y_strength=MentionGraph.get_gravity_y_strength("user"),
                               gravity_x_strength=float(10.0))

                # Add edge from tweet to mention
                graph.add_edge(tweet_id, target_node_id, type="output")

                GraphUtils.link_node_to_history(graph, history, target_node_id, target_user_id)

        return graph

    @staticmethod
    def get_gravity_y_strength(node_type, tweet=None):

        if node_type == "user":
            return float(0.01)
        else:
            return float(10.0)

    @staticmethod
    def get_gravity_y(node_type, tweet=None):
        if node_type == "tweet":
            return 10.0 * TweetClassifier.classify_tweet_by_hashtag(tweet, tags)
        else:
            return float(0.0)


tags = {
    "brexit": 0,
    "no2eu": 2,
    "notoeu": 2,
    "betteroffout": 2,
    "voteout": 2,
    "eureform": 2,
    "britainout": 2,
    "leaveeu": 2,
    "voteleave": 2,
    "beleave": 2,
    "loveeuropeleaveeu": 2,
    "yes2eu": -2,
    "yestoeu": -2,
    "betteroffin": -2,
    "votein": -2,
    "ukineu": -2,
    "bremain": -2,
    "strongerin": -2,
    "leadnotleave": -2,
    "voteremain": -2,
}
