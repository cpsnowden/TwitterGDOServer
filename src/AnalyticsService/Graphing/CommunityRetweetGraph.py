import logging
from sets import Set
from src.AnalyticsService.TwitterObj import Status


class CommunityRetweetGraph(object):
    _logger = logging.getLogger(__name__)

    __edge = ("type", {"retweet": "blue"})
    __node = ("type", {"retweeted": "red", "retweeter": "lime", "both":"blueviolet"})
    color = (__node, __edge)

    @classmethod
    def get_graph(cls, graph, cursor, schema_id):

        retweeters = Set()
        retweeted = Set()

        for status_json in cursor:

            status = Status(status_json, schema_id)

            # Add the source user
            source_user = status.get_user()
            source_user_id = str(source_user.get_id())

            if source_user_id not in retweeted and source_user_id not in retweeters:

                graph.add_node(source_user_id,
                               label=source_user.get_name(),
                               type="retweeter")

            retweeters.add(source_user_id)

            if source_user_id in retweeted:
                graph.node[source_user_id]["type"] = "both"

            target_user = status.get_retweet_status().get_user()
            target_user_id = str(target_user.get_id())

            if target_user_id not in retweeted and target_user_id not in retweeters:

                graph.add_node(target_user_id,
                               label=target_user.get_name(),
                               type="retweeted")

            if target_user_id in retweeters:
                graph.node[target_user_id]["type"] = "both"

            retweeted.add(target_user_id)

            graph.add_edge(source_user_id, target_user_id, type="retweet", tweet = status.get_text())

        return graph
