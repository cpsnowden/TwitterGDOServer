import logging

import networkx as nx
from AnalyticsService.Graphing.Community.CommunityGraph import CommunityGraph
from AnalyticsService.TwitterObj import Status
from dateutil import parser


class RetweetCommunityGraph(CommunityGraph):
    _logger = logging.getLogger(__name__)

    __type_name = "Retweet_Community_Graph"
    __arguments = []

    _edges_color = ("type", {"retweet": "blue"})
    _node_color = ("type", {"retweeted": "red", "retweeter": "lime", "both": "blueviolet"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(CommunityGraph, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    @classmethod
    def get(cls, analytics_meta):

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        tweet_limit = args["tweetLimit"]
        start_date_co = parser.parse(args["startDateCutOff"])
        end_date_co = parser.parse(args["endDateCutOff"])

        query = {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None},
                 Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date_co, "$lte": end_date_co}}

        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()
            return

        graph = cls.build_graph(cursor, schema_id)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        cls.finalise_graph(graph, gridfs, analytics_meta, cls.get_color())

        return True

    @classmethod
    def build_graph(cls, cursor, schema_id):

        graph = nx.DiGraph()

        retweeters = set()
        retweeted = set()

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

            graph.add_edge(source_user_id, target_user_id, type="retweet", tweet=status.get_text())

        return graph
