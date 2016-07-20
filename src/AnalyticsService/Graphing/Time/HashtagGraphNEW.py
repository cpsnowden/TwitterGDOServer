import logging

import networkx as nx
from AnalyticsService.Graphing.Classification.Classification import TwitterUserModel
from AnalyticsService.Graphing.Classification.Brexit_Tags import leave,remain
from AnalyticsService.Graphing.Time.TimeGraph import TimeGraph
from AnalyticsService.TwitterObj import Status, User
from dateutil import parser


class HashtagGraphRetweetNEW(TimeGraph):
    _logger = logging.getLogger(__name__)

    __type_name = "HashtagGraphRetweetNEW"
    __arguments = [{"name": "userLimit", "prettyName": "Number of users", "type": "integer", "default": 1000},
                   {"name": "tweetScoring", "prettyName": "Tweet Scoring System", "type": "enum", "options":
                     TwitterUserModel.scoring_options.keys()}]

    _node_color = ("type", {"status": "blue", "source": "red", "target": "lime"})
    _edges_color = ("type", {"source": "gold", "target": "turquoise"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(HashtagGraphRetweetNEW, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    @classmethod
    def get(cls, analytics_meta):
        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        time_interval = args["timeInterval"]
        tweet_limit = args["tweetLimit"]
        start_date_co = parser.parse(args["startDateCutOff"])
        end_date_co = parser.parse(args["endDateCutOff"])
        limit = args["userLimit"]

        user_ids = cls.get_top_users(db_col, limit, schema_id)

        query = {Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date_co, "$lte": end_date_co},
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
    def get_top_users(cls, db_col, limit, schema_id):

        user_id_key = cls.join_keys(Status.SCHEMA_MAP[schema_id]["user"],
                                    User.SCHEMA_MAP[schema_id]["id"])

        cls._logger.info("Using user id key '%s'", user_id_key)
        query_top_retweeters = [
            {"$group": {"_id": {'id': '$' + user_id_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        cls._logger.info("Query for top users %s", query_top_retweeters)

        return [user["_id"]["id"] for user in db_col.aggregate(query_top_retweeters, allowDiskUse=True)]

    @classmethod
    def build_graph(cls, cursor, schema_id, time_interval):

        history = {}
        users = {}
        graph = nx.DiGraph()

        start_date = Status(cursor[0], schema_id).get_created_at()

        for status_json in cursor:
            status = Status(status_json, schema_id)

            date = status.get_created_at()
            time_step = cls.get_time_step(date, start_date, time_interval)

            status_id = str(status.get_id())

            source_user = status.get_user()
            source_id = str(source_user.get_id())

            if source_id not in users:
                users[source_id] = TwitterUserModel((leave,remain))
            user_model = users[source_id]
            current_score = user_model.said_these(status.get_hashtags())

            graph.add_node(status_id,
                           label="twt:" + status.get_text().replace("\n", " "),
                           type="status",
                           date=str(status.get_created_at()),
                           gravity_x=float(time_step),
                           gravity_y=float(0.0),
                           gravity_x_strength=float(10),
                           gravity_y_strength=float(0.001))

            user = status.get_user()
            source_user_id = user.get_id()
            node_id = str(source_user_id) + ":" + str(status_id)

            graph.add_node(node_id,
                           label="usr:" + user.get_name(),
                           type="source",
                           date=str(date),
                           gravity_x=float(time_step),
                           gravity_y=float(current_score),
                           gravity_x_strength=float(10),
                           gravity_y_strength=float(10))

            TimeGraph.link_node_to_history(graph, history, node_id, source_user_id)
            graph.add_edge(node_id, status_id, type = "source")

        return graph
