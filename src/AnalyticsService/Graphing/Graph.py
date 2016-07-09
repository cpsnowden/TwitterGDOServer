import logging
from datetime import datetime

import networkx as nx
import pymongo
from src.AnalyticsService.Graphing.GraphUtils import GraphColor
from src.AnalyticsService.Graphing.MentionGraph import MentionGraph
from src.AnalyticsService.Graphing.RetweetGraph import RetweetGraph
from src.AnalyticsService.TwitterObj import Status
from dateutil import parser
from src.AnalyticsService.Graphing.GraphUtils import GraphUtils
from src.Database.Persistence import DatabaseManager
from src.api.Objects.MetaData import DatasetMeta


def get_schema_id(db_col):
    T4J_cols = [
        "DS_2db92824-be5d-47ee-b746-a3a2ddda1863",
        "DS_df6fd789-6728-4903-8f1a-b29c12ea4928"
    ]

    if db_col in T4J_cols:
        return "T4J"
    else:
        return "RAW"


class Graph(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_graph(cls, analytics_meta):
        graph_options = {
        "Mention_Time_Graph": Graph.get_mention_time_graph,
        "Retweet_Time_Graph": Graph.get_retweet_time_graph
        }

        graph_constructor = graph_options[analytics_meta.type]
        return graph_constructor(analytics_meta)

    @classmethod
    def get_retweet_time_graph(cls, analytics_meta):

        cls._logger.info("Attempting retweet time graph")

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        quantum_s = args["Time_Quantum_s"]
        tweet_limit = args["Tweet_Limit"]
        start_date = parser.parse(args["Start_Date_Lim"])
        end_date = parser.parse(args["End_Date_Lim"])

        query = {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists":True,"$ne":None},
                 Status.SCHEMA_MAP[schema_id]["created_at"]: {"$gte": start_date, "$lte": end_date}}

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

        cls.export(analytics_meta, gridfs, graph)


    @classmethod
    def get_mention_time_graph(cls, analytics_meta):

        cls._logger.info("Attempting mention time graph")

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        quantum_s = args["Time_Quantum_s"]
        tweet_limit = args["Tweet_Limit"]
        start_date = parser.parse(args["Start_Date_Lim"])
        end_date = parser.parse(args["End_Date_Lim"])

        query = {Status.SCHEMA_MAP[schema_id]["mentions"]: {"$gt": []},
                 Status.SCHEMA_MAP[schema_id]["retweeted_status"]: None,
                 Status.SCHEMA_MAP[schema_id]["created_at"]: {"$gte": start_date, "$lte": end_date}}

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

        cls.export(analytics_meta, gridfs, graph)

    @classmethod
    def setup(cls, analytics_meta):

        dataset_meta = DatasetMeta.objects.get(id=analytics_meta.dataset_id)
        schema_id = get_schema_id(analytics_meta.dataset_id)

        dbm = DatabaseManager()
        db_col = dbm.data_db.get_collection(dataset_meta.db_col)

        args = dict([(k, v["value"]) for k, v in analytics_meta.specialised_args.items()])

        cls._logger.info("Found arguments %s", str(args))
        cls._logger.info("Using schema: %s", schema_id)

        return dbm.gridfs, db_col, args, schema_id

    @classmethod
    def export(cls, analytics_meta, gridfs, graph):

        Graph.save_to_gridfs(graph, analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved networkx formatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED UNFORMATTED"
        analytics_meta.save()

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved GDO fomatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

    @classmethod
    def get_cursor(cls, db_col, query, schema_id, tweet_limit):

        cls._logger.info("Querying DATA database, collection %s with query %s", db_col.name, query)

        cursor = db_col.find(query) \
            .limit(tweet_limit) \
            .sort(Status.SCHEMA_MAP[schema_id]["created_at"], pymongo.ASCENDING)

        cusor_size = cursor.count(with_limit_and_skip=True)

        cls._logger.info("Cursor size %d", cusor_size)
        if cusor_size == 0:
            logging.warning("Cursor has no data !!")
            return None

        return cursor


    @classmethod
    def save_to_path(cls, graph, name):
        nx.write_graphml(graph, name)

    @classmethod
    def save_to_gridfs(cls, graph, name, gridfs):
        with gridfs.new_file(filename=name, content_type="text/xml") as f:
            nx.write_graphml(graph, f)



