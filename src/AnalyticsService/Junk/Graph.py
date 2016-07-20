import logging
from datetime import datetime

from src.AnalyticsService.Graphing.CommunityRetweetGraph import CommunityRetweetGraph

import networkx as nx
import pymongo
from AnalyticsService.Junk.AnalyticsUtils import AnalyticsUtils
from AnalyticsService.Junk.MentionGraph import MentionGraph
from AnalyticsService.Junk.RetweetGraph import RetweetGraph
from dateutil import parser
from src.AnalyticsService.Graphing.GraphUtils import GraphColor
from src.AnalyticsService.Graphing.GraphUtils import GraphUtils
from src.AnalyticsService.TwitterObj import Status, User


class Graph(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_community_retweet_graph(cls, analytics_meta):

        cls._logger.info("Attempting retweet community graph")

        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        tweet_limit = args["Tweet_Limit"]
        start_date = parser.parse(args["Start_Date_Lim"])
        end_date = parser.parse(args["End_Date_Lim"])

        query = {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None},
                 Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date, "$lte": end_date}}
        cls._logger.info("Getting cursor")
        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()

        graph = nx.DiGraph()
        cls._logger.info("Getting graph")
        CommunityRetweetGraph.get_graph(graph, cursor, schema_id)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        # Layout Call

        GraphColor.color_graph(graph, CommunityRetweetGraph.color)
        cls._logger.info("Colored graph %s", analytics_meta.id)
        analytics_meta.status = "COLORED"
        analytics_meta.save()

        AnalyticsUtils.export(analytics_meta, gridfs, graph, cls.save_graphml)

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)

        cls._logger.info("Saved GDO fomatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True


    @classmethod
    def get_retweet_time_graph(cls, analytics_meta):

        cls._logger.info("Attempting retweet time graph")

        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        quantum_s = args["Time_Quantum_s"]
        tweet_limit = args["Tweet_Limit"]
        start_date = parser.parse(args["Start_Date_Lim"])
        end_date = parser.parse(args["End_Date_Lim"])
        limit_top_source = args["Limit_Sources"]

        user_id_key = Status.SCHEMA_MAP[schema_id]["user"] + "." + User.SCHEMA_MAP[schema_id]["id"]
        cls._logger.info("User id key %s", user_id_key)

        top_user_query  = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {'id': '$' + user_id_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit_top_source}
        ]

        cls._logger.info("Query for top retweeting users %s", top_user_query)

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        source_users = [user["_id"]["id"] for user in top_users]

        query = {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None},
                 Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date, "$lte": end_date},
                 user_id_key: {"$in": source_users}}

        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()

        graph = nx.Graph()
        RetweetGraph.get_graph(graph, cursor, schema_id, quantum_s)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        # Layout Call

        GraphColor.color_graph(graph, RetweetGraph.color)
        cls._logger.info("Colored graph %s", analytics_meta.id)
        analytics_meta.status = "COLORED"
        analytics_meta.save()

        AnalyticsUtils.export(analytics_meta, gridfs, graph, cls.save_graphml)

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)

        cls._logger.info("Saved GDO fomatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True

    @classmethod
    def get_mention_time_graph(cls, analytics_meta):

        cls._logger.info("Attempting mention time graph")

        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        quantum_s = args["Time_Quantum_s"]
        tweet_limit = args["Tweet_Limit"]
        start_date = parser.parse(args["Start_Date_Lim"])
        end_date = parser.parse(args["End_Date_Lim"])
        limit_top_source = args["Limit_Sources"]

        user_id_key = Status.SCHEMA_MAP[schema_id]["user"] + "." + User.SCHEMA_MAP[schema_id]["id"]
        cls._logger.info("User id key %s", user_id_key)

        top_user_query = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": False},
                        Status.SCHEMA_MAP[schema_id]["mentions"]: {"$gt": []}}},
            {"$group": {"_id": {'id': '$' + user_id_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit_top_source}
        ]

        cls._logger.info("Query for top mentioning users %s", top_user_query)

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        source_users = [user["_id"]["id"] for user in top_users]

        query = {Status.SCHEMA_MAP[schema_id]["mentions"]: {"$gt": []},
                 Status.SCHEMA_MAP[schema_id]["retweeted_status"]: None,
                 Status.SCHEMA_MAP[schema_id]["ISO_date"]: {"$gte": start_date, "$lte": end_date},
                 user_id_key: {"$in": source_users}}

        cursor = cls.get_cursor(db_col, query, schema_id, tweet_limit)
        if cursor is None:
            analytics_meta.status = "NO DATA IN RANGE"
            analytics_meta.save()

        graph = nx.Graph()
        MentionGraph.get_graph(graph, cursor, schema_id, quantum_s)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        # Layout Call

        GraphColor.color_graph(graph, MentionGraph.color)
        cls._logger.info("Colored graph %s", analytics_meta.id)
        analytics_meta.status = "COLORED"
        analytics_meta.save()

        AnalyticsUtils.export(analytics_meta, gridfs, graph, cls.save_graphml)

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)

        cls._logger.info("Saved GDO fomatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True

    @classmethod
    def save_graphml(cls, graph, name, gridfs):
        with gridfs.new_file(filename=name, content_type="text/xml") as f:
            nx.write_graphml(graph, f)

    @classmethod
    def get_cursor(cls, db_col, query, schema_id, tweet_limit):

        cls._logger.info("Querying DATA database, collection %s with query %s", db_col.name, query)

        cursor = db_col.find(query) \
            .limit(tweet_limit) \
            .sort(Status.SCHEMA_MAP[schema_id]["ISO_date"], pymongo.ASCENDING)

        cusor_size = cursor.count(with_limit_and_skip=True)

        cls._logger.info("Cursor size %d", cusor_size)
        if cusor_size == 0:
            logging.warning("Cursor has no data !!")
            return None

        return cursor

    @classmethod
    def save_to_path(cls, graph, name):
        nx.write_graphml(graph, name)

