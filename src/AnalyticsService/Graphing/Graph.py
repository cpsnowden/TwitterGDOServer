import logging
import json
import networkx as nx
from datetime import datetime

from AnalyticsService.Graphing.GraphUtils import GraphColor, GraphUtils
from AnalyticsService.TwitterObj import Status
from AnalyticsService.AnalysisTemplate import AnalysisTemplate
from pymongo import ASCENDING
from networkx import write_graphml
from AnalyticsService.Graphing.GephiRPC.GephiRPC import GephiRpcClient

class Graph(AnalysisTemplate):
    _logging = logging.getLogger(__name__)


    __arguments = [{"name": "tweetLimit", "prettyName": "Tweet limit", "type": "integer", "default": -1},
                   {"name": "startDateCutOff", "prettyName": "Start date cut-off", "type": "datetime",
                    "default_dataset_field": "start_time"},
                   {"name": "endDateCutOff", "prettyName": "End date cut-off", "type": "datetime",
                    "default_dataset_field": "end_time"},
                   {"name": "layoutIterations", "prettyName": "Layout Iterations", "type": "integer",
                    "default": -1},
                   {"name": "LAYOUT_ALGO", "prettyName": "Layout Algorithm", "type": "enum", "options":
                       ["FA2MS"], "default" : "FA2MS"}
                   ]
    @classmethod
    def get_args(cls):
        return cls.__arguments + super(Graph, cls).get_args()

    ####################################################################################################################

    @classmethod
    def get_cursor(cls, db_col, query, schema_id, tweet_limit):
        cls._logger.info("Querying DATA database, collection %s with query %s", db_col.name, query)

        if tweet_limit > 0:
            cursor = db_col.find(query) \
                .limit(tweet_limit) \
                .sort(Status.SCHEMA_MAP[schema_id]["ISO_date"], ASCENDING)
        else:
            cursor = db_col.find(query) \
                .sort(Status.SCHEMA_MAP[schema_id]["ISO_date"], ASCENDING)

        cusor_size = cursor.count(with_limit_and_skip=True)

        cls._logger.info("Cursor size %d", cusor_size)
        if cusor_size == 0:
            logging.warning("Cursor has no data !!")
            return None

        return cursor

    @classmethod
    def finalise_graph(cls, graph, gridfs, analytics_meta, color_scheme):
        GraphColor.color_graph(graph, color_scheme)
        cls._logger.info("Colored graph %s", analytics_meta.id)
        analytics_meta.status = "COLORED"
        analytics_meta.save()

        cls.export_to_gridfs(graph, analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved analytics %s", analytics_meta.db_ref)
        analytics_meta.status = "EXPORTED"
        analytics_meta.save()

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved GDO fomatted graph %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

    @classmethod
    def export_to_gridfs(cls, graph, name, gridfs):
        with gridfs.new_file(filename=name, content_type="text/xml") as f:
            write_graphml(graph, f)

    @classmethod
    def layout(cls, graph, analytics_meta, gridfs, args):

        n_iterations = args["layoutIterations"]
        if n_iterations <= 0:
            cls._logging.info("Not laying out as number of iterations <=0")
            return graph

        cls.export_to_gridfs(graph, analytics_meta.db_ref, gridfs)
        cls._logger.info("Exported graph ready for layout %s", analytics_meta.db_ref)
        analytics_meta.status = "EXPORTED RAW"
        analytics_meta.save()

        GraphUtils.fix_graphml_format(analytics_meta.db_ref, gridfs)
        cls._logger.info("Reformatted graph ready for layout %s", analytics_meta.db_ref)
        analytics_meta.status = "REFORMAT COMPLETE"
        analytics_meta.save()

        params = {"LAYOUT_ITERATIONS": args["layoutIterations"],
                  "LAYOUT_ALGO": args["LAYOUT_ALGO"]}

        client = GephiRpcClient()
        result = json.loads(client.call(json.dumps({"GephiParameters": params, "fileName": analytics_meta.db_ref})))
        cls._logger.info("Result: %s", result)

        cls._logger.info("Build graph %s", analytics_meta.id)
        analytics_meta.status = "LAYOUT COMPLETE"
        analytics_meta.save()

        with gridfs.get_last_version(analytics_meta.db_ref) as f:
            return nx.read_graphml(f)