import logging

import networkx as nx
from AnalyticsService.Graphing.Time.TimeGraph import TimeGraph
from AnalyticsService.TwitterObj import Status, User
from dateutil import parser


class RetweetTimeGraph(TimeGraph):
    _logger = logging.getLogger(__name__)

    __type_name = "Retweet_Time_Graph"
    __arguments = [{"name": "limitRetweeters", "prettyName": "Limit number of top retweeters", "type": "integer",
                    "default": 100},
                   {"name": "topPolarisingUser", "prettyName": "Top polarising user", "type": "string",
                    "default": "vote_leave"},
                   {"name": "bottomPolarisingUser", "prettyName": "Bottom polarising user", "type": "string",
                    "default": "StrongerIn"}]

    _node_color = ("type", {"status": "blue", "source": "red", "target": "lime"})
    _edges_color = ("type", {"source": "gold", "target": "turquoise"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(RetweetTimeGraph, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    topPolarisingUser = None
    bottomPolarisingUser = None

    @classmethod
    def get(cls, analytics_meta):

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        time_interval = args["timeInterval"]
        tweet_limit = args["tweetLimit"]
        start_date_co = parser.parse(args["startDateCutOff"])
        end_date_co = parser.parse(args["endDateCutOff"])
        limit_retweeters = args["limitRetweeters"]
        cls.topPolarisingUser = args["topPolarisingUser"]
        cls.bottomPolarisingUser = args["bottomPolarisingUser"]

        user_ids = cls.get_top_retweeter_ids(db_col, limit_retweeters, schema_id)

        query = {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None},
                 Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date_co, "$lte": end_date_co},
                 cls.join_keys(Status.SCHEMA_MAP[schema_id]["user"], User.SCHEMA_MAP[schema_id]["id"]):
                     {"$in": user_ids}}

        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()
            return

        graph = cls.build_graph(cursor, schema_id, time_interval)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        cls.finalise_graph(graph, gridfs, analytics_meta, cls.get_color())

        return True

    @classmethod
    def get_top_retweeter_ids(cls, db_col, retweeter_limit, schema_id):

        user_id_key = cls.join_keys(Status.SCHEMA_MAP[schema_id]["user"],
                                    User.SCHEMA_MAP[schema_id]["id"])
        cls._logger.info("Using user id key '%s'", user_id_key)
        query_top_retweeters = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {'id': '$' + user_id_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": retweeter_limit}
        ]
        cls._logger.info("Query for top retweeting users %s", query_top_retweeters)

        return [user["_id"]["id"] for user in db_col.aggregate(query_top_retweeters, allowDiskUse=True)]

    @classmethod
    def build_graph(cls, cursor, schema_id, time_interval):

        history = {}
        graph = nx.DiGraph()

        start_date = Status(cursor[0], schema_id).get_created_at()

        for status_json in cursor:
            status = Status(status_json, schema_id)

            time_step = cls.get_time_step(status.get_created_at(), start_date, time_interval)

            status_id = str(status.get_id())
            graph.add_node(status_id,
                           label=status.get_text().replace("\n", " "),
                           type="status",
                           date=str(status.get_created_at()),
                           gravity_x=RetweetTimeGraph.get_gravity_x(time_step),
                           gravity_y=RetweetTimeGraph.get_gravity_y("status"),
                           gravity_x_strength=RetweetTimeGraph.get_gravity_x_strength("status"),
                           gravity_y_strength=RetweetTimeGraph.get_gravity_y_strength("status"))

            source_user = status.get_user()
            target_user = status.get_retweet_status().get_user()

            cls.add_user_node(graph, source_user, time_step, status_id, "source", history)
            cls.add_user_node(graph, target_user, time_step, status_id, "target", history)

        return graph

    @staticmethod
    def add_user_node(graph, user, time_step, status_id, node_type, history):

        user_id = user.get_id()
        node_id = str(user_id) + ":" + str(status_id)

        graph.add_node(node_id,
                       label="usr:" + user.get_name(),
                       type=node_type,
                       gravity_x=RetweetTimeGraph.get_gravity_x(time_step),
                       gravity_y=RetweetTimeGraph.get_gravity_y(node_type, user),
                       gravity_x_strength=RetweetTimeGraph.get_gravity_x_strength(node_type),
                       gravity_y_strength=RetweetTimeGraph.get_gravity_y_strength(node_type, user))

        if node_type == "source":
            graph.add_edge(node_id, status_id, type=node_type)
        else:
            graph.add_edge(status_id, node_id, type=node_type)

        TimeGraph.link_node_to_history(graph, history, node_id, user_id)

    @classmethod
    def get_gravity_x(cls, time_step):
        return float(time_step)

    @classmethod
    def get_gravity_y(cls, node_type, c=None):
        if node_type in ["source", "target"]:
            return {cls.bottomPolarisingUser: -100.0, cls.topPolarisingUser: 100.0}.get(c.get_name(), 0.0)
        else:
            return 0.0

    @classmethod
    def get_gravity_x_strength(cls, node_type):
        return 10.0

    @classmethod
    def get_gravity_y_strength(cls, node_type, c=None):
        if node_type in ["source", "target"]:
            return {cls.bottomPolarisingUser: 400.0, cls.topPolarisingUser: 400.0}.get(c.get_name(), 0.01)
        else:
            return 0.001
