import logging

from src.AnalyticsService.TweetClassifier import TweetClassifier
from src.AnalyticsService.TwitterObj import Status
from src.AnalyticsService.Graphing.GraphUtils import GraphUtils


class MentionGraphSimple(object):
    _logger = logging.getLogger(__name__)

    __mention_nodes = ("type", {"tweet": "blue", "source": "red", "target": "lime"})
    __mention_edges = ("type", {"input": "gold", "tweet": "turquoise"})
    color = (__mention_nodes, __mention_edges)

    @classmethod
    def get_graph(cls, graph, cursor, schema_id, quantum_s):

        history = {}

        start_date = Status(cursor[0], schema_id).get_created_at()

        for status_json in cursor:

            status = Status(status_json, schema_id)

            time_step = int(divmod((status.get_created_at() - start_date).total_seconds(), quantum_s)[0])

            # # Add tweet node
            tweet_id = str(status.get_id())

            # Add the source user
            source_user = status.get_user()
            source_user_id = source_user.get_id()
            source_node_id = str(source_user_id) + ":" + tweet_id

            step_score = TweetClassifier.classify_tweet_by_hashtag(status, tags)
            previous_node_id = GraphUtils.get_last_node(source_user_id, history)
            if previous_node_id is None:
                score = step_score
            else:
                previous_score = graph.node[previous_node_id]["score"]
                score = 0.7 * step_score + 0.3 * previous_score

            graph.add_node(source_node_id,
                           label="usr:" + source_user.get_name() + " txt: " + status.get_text().replace("\n", " "),
                           type="source",
                           tweet = status.get_text().replace("\n", " "),
                           date=str(status.get_created_at()),
                           gravity_x=float(time_step),
                           gravity_y=float(10*score),
                           gravity_y_strength=float(100),
                           gravity_x_strength=float(10.0),
                           score = float(score))

            GraphUtils.link_node_to_history(graph, history, source_node_id, source_user_id)

            for mention in status.get_mentions():
                # Add mention node

                target_user_id = str(mention.get_user_id())
                target_user_name = mention.get_user_name()
                target_node_id = str(target_user_id) + ":" + str(tweet_id)

                previous_node_id = GraphUtils.get_last_node(target_user_id, history)
                if previous_node_id is None:
                    score = 0
                else:
                    score = graph.node[previous_node_id]["score"]


                graph.add_node(target_node_id,
                               label=target_user_name,
                               type="target",
                               gravity_x=float(time_step),
                               gravity_y=float(0.0),
                               gravity_y_strength=float(0.01),
                               gravity_x_strength=float(10.0),
                               score=float(score))

                # Add edge from tweet to mention
                graph.add_edge(source_node_id, target_node_id, type="tweet")

                GraphUtils.link_node_to_history(graph, history, target_node_id, target_user_id)

        return graph

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
