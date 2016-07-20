import logging

import networkx as nx
from AnalyticsService.Graphing.Time.TimeGraph import TimeGraph
from AnalyticsService.TwitterObj import Status, User
from dateutil import parser
import numpy as np


class RetweetTimeGraphHistory(TimeGraph):
    _logger = logging.getLogger(__name__)
    __type_name = "Retweet_Time_Graph_with_History"
    __arguments = [{"name": "limitRetweeters", "prettyName": "Limit number of top retweeters", "type": "integer",
                    "default": 100},
                   {"name": "alphaS", "prettyName": "Weight on source's previous score", "type": "integer",
                    "default": 0.7},
                   {"name": "betaS", "prettyName": "Weight of target's previous score on source", "type": "integer",
                    "default": 0.3},
                   {"name": "alphaT", "prettyName": "Weight on target's previous score", "type": "integer",
                    "default": 0.7},
                   {"name": "betaT", "prettyName": "Weight of source's previous score on target", "type": "integer",
                    "default": 0.3},
                   {"name": "scoreToGravityY", "prettyName": "Scaling of score to gravity Y", "type": "integer",
                    "default": 10},
                   {"name": "exponent", "prettyName": "Exponent", "type": "integer", "default": 2},
                   {"name": "denomenator", "prettyName": "Denomenator", "type": "integer", "default": 1}]

    _node_color = ("type", {"status": "blue", "source": "red", "target": "lime"})
    _edges_color = ("type", {"source": "gold", "target": "turquoise"})

    @classmethod
    def get_color(cls):
        return cls._node_color, cls._edges_color

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(RetweetTimeGraphHistory, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    beta_s = 0.3
    alpha_s = 0.7
    beta_t = 0.1
    alpha_t = 0.9
    score_to_gravity_y = 10.0
    denomenator = 1
    exponent = 2

    @classmethod
    def get(cls, analytics_meta):

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        time_interval = args["timeInterval"]
        tweet_limit = args["tweetLimit"]
        start_date_co = parser.parse(args["startDateCutOff"])
        end_date_co = parser.parse(args["endDateCutOff"])
        limit_retweeters = args["limitRetweeters"]

        cls.alpha_s = args["alphaS"]
        cls.alpha_t = args["alphaT"]
        cls.beta_s = args["betaS"]
        cls.beta_t = args["betaT"]
        cls.score_to_gravity_y = args["scoreToGravityY"]
        cls.denomenator = args["denomenator"]
        cls.exponent = args["exponent"]

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
        scores = {}
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
                           gravity_x=RetweetTimeGraphHistory.get_gravity_x(time_step),
                           gravity_y=RetweetTimeGraphHistory.get_gravity_y("status"),
                           gravity_x_strength=RetweetTimeGraphHistory.get_gravity_x_strength("status"),
                           gravity_y_strength=RetweetTimeGraphHistory.get_gravity_y_strength("status"))

            source_user = status.get_user()
            target_user = status.get_retweet_status().get_user()

            source_score, target_score = RetweetTimeGraphHistory.get_scores(source_user.get_name(),
                                                                            target_user.get_name(),
                                                                            scores, ("vote_leave", "StrongerIn"))

            cls.add_user_node(graph, source_user, time_step, status_id, "source", history, source_score)
            cls.add_user_node(graph, target_user, time_step, status_id, "target", history, target_score)

        return graph

    @classmethod
    def get_scores(cls, node1, node2, scores, pinned):

        top_pin, bottom_pin = pinned

        DEFAULT = 0.0
        OFFSET = 100.0
        # print node1, node2, pinned
        if node1 not in scores:
            if node1 == top_pin:
                scores[node1] = OFFSET
            elif node1 == bottom_pin:
                scores[node1] = -OFFSET
            else:
                scores[node1] = DEFAULT
        if node2 not in scores:
            if node2 == top_pin:
                scores[node2] = OFFSET
            elif node2 == bottom_pin:
                scores[node2] = -OFFSET
            else:
                scores[node2] = DEFAULT

        if node1 == top_pin:
            node1_score = OFFSET
            # print "Top pin"
        elif node1 == bottom_pin:
            node1_score = -OFFSET
            # print "Bottom pin"
        else:
            node1_score = (cls.beta_s * scores[node2] + cls.alpha_s * scores[node1]) / 2.0

        if node2 == bottom_pin:
            node2_score = -OFFSET
            # print "Top pin"
        elif node2 == top_pin:
            node2_score = OFFSET
            # print "Buttom pin"
        else:
            node2_score = (cls.beta_t * scores[node1] + cls.alpha_t * scores[node2]) / 2.0

        scores[node1] = node1_score
        scores[node2] = node2_score

        return node1_score, node2_score

    @staticmethod
    def add_user_node(graph, user, time_step, status_id, node_type, history, score):

        user_id = user.get_id()
        node_id = str(user_id) + ":" + str(status_id)

        graph.add_node(node_id,
                       label="usr:" + user.get_name(),
                       type=node_type,
                       score=score,
                       gravity_x=RetweetTimeGraphHistory.get_gravity_x(time_step),
                       gravity_y=RetweetTimeGraphHistory.get_gravity_y(node_type, user, score),
                       gravity_x_strength=RetweetTimeGraphHistory.get_gravity_x_strength(node_type, user),
                       gravity_y_strength=RetweetTimeGraphHistory.get_gravity_y_strength(node_type, user, score))

        if node_type == "source":
            graph.add_edge(node_id, status_id, type=node_type)
        else:
            graph.add_edge(status_id, node_id, type=node_type)

        TimeGraph.link_node_to_history(graph, history, node_id, user_id)

    @classmethod
    def get_gravity_x(cls, time_step):
        return float(time_step)

    @classmethod
    def get_gravity_y(cls, node_type, c=None, score=None):
        if node_type in ["source", "target"]:
            return score * cls.score_to_gravity_y
            # return {"StrongerIn": -100.0, "vote_leave": 100.0}.get(c.get_name(), 0.0)
        else:
            return 0.0

    @classmethod
    def get_gravity_x_strength(cls, node_type, c=None):
        return 10.0

    @classmethod
    def get_gravity_y_strength(cls, node_type, c=None, score=None):
        if node_type in ["source", "target"]:
            return {"StrongerIn": 400.0, "vote_leave": 400.0}.get(c.get_name(),
                                                                  float(max([np.power(score / cls.denomenator,
                                                                                      cls.exponent),
                                                                             0.1])))
        else:
            return 0.001
