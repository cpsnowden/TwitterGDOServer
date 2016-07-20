class CUser(object):
    POSITIVE = 40
    NEGATIVE = -40

    def __init__(self, scores):
        self.scores = scores
        self.time_constant = 5.0
        self.partisan = None
        self.score = 0.0
        self.scaler = 1.0

    def said_these(self, hastags, time_step):

        score = 0
        for h in hastags:
            score += self.scores.get(h.lower(), 0.0)

        if score > 0:
            self.add_positive(score, time_step)
        elif score < 0:
            self.add_negative(score, time_step)

        return self.score

    def add_positive(self, score, time_step):

        if self.partisan is None or self.partisan == "Negative":
            if self.partisan == "Negative":
                self.scaler = 2.0
            self.partisan = "Positive"

        self.score += self.scaler * score * (self.POSITIVE - self.score) / 100

    def add_negative(self, score, time_step):

        if self.partisan is None or self.partisan == "Positive":
            if self.partisan == "Positive":
                self.scaler = 2.0
            self.partisan = "Negative"

        self.score += self.scaler * abs(score) * (self.NEGATIVE - self.score) / 100


leave = {
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
}

remain = {
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

tags = dict(leave.items() + remain.items())

from AnalyticsService.Graphing.Time.TimeGraph import TimeGraph
import logging
from dateutil import parser
from AnalyticsService.TwitterObj import Status, User
import networkx as nx


class HashtagGraph(TimeGraph):
    _logger = logging.getLogger(__name__)

    __type_name = "HashtagGraph"
    __arguments = [{"name": "userLimit", "prettyName": "Number of users", "type": "integer", "default": 1000}]

    _node_color = ("type", {"status": "blue", "source": "red", "target": "lime"})
    _edges_color = ("type", {"source": "gold", "target": "turquoise"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(HashtagGraph, cls).get_args()

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

            time_step = cls.get_time_step(status.get_created_at(), start_date, time_interval)

            status_id = str(status.get_id())

            source_user = status.get_user()
            source_id = str(source_user.get_id())

            if source_id not in users:
                users[source_id] = CUser(tags)
            cuser = users[source_id]
            current_score = cuser.said_these(status.get_hashtags(), time_step)

            cls.add_user_node(graph, status, time_step, status_id, "source", history, current_score)

        return graph

    @staticmethod
    def add_user_node(graph, status, time_step, status_id, node_type, history, score):
        user = status.get_user()
        user_id = user.get_id()
        node_id = str(user_id) + ":" + str(status_id)

        graph.add_node(node_id,
                       label="usr:" + user.get_name() + ":" + status.get_text().replace("\n", " "),
                       type=node_type,
                       date=str(status.get_created_at()),
                       gravity_x=float(time_step),
                       gravity_y=float(score),
                       gravity_x_strength=float(10),
                       gravity_y_strength=float(10))

        TimeGraph.link_node_to_history(graph, history, node_id, user_id)
