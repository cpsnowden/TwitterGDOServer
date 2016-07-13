import logging

from src.Database.Persistence import DatabaseManager
from src.api.Objects.MetaData import DatasetMeta


def get_schema_id(db_col):
    T4J_cols = [
        "DS_2db92824-be5d-47ee-b746-a3a2ddda1863",
        "DS_df6fd789-6728-4903-8f1a-b29c12ea4928",
        "DS_a565c2c6-e6dd-4560-90b3-4427723082f4"
    ]

    if db_col in T4J_cols:
        return "T4J"
    else:
        return "RAW"


class AnalyticsUtils(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def setup(cls, analytics_meta):
        dataset_meta = DatasetMeta.objects.get(id=analytics_meta.dataset_id)
        schema_id = dataset_meta.schema
        dbm = DatabaseManager()
        db_col = dbm.data_db.get_collection(dataset_meta.db_col)

        args = dict([(k, v["value"]) for k, v in analytics_meta.specialised_args.items()])

        cls._logger.info("Found arguments %s", str(args))

        return dbm.gridfs, db_col, args, schema_id

    @classmethod
    def export(cls, analytics_meta, gridfs, graph, save_function):
        save_function(graph, analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved analytics %s", analytics_meta.db_ref)
        analytics_meta.status = "EXPORTED"
        analytics_meta.save()

    @classmethod
    def write_json(cls, data, name, gridfs):
        with gridfs.new_file(filename=name, content_type="text/json") as f:
            f.write(data)
