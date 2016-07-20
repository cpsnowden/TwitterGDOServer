from Database.Persistence import DatabaseManager
from api.Objects.MetaData import DatasetMeta
import logging


class AnalysisTemplate(object):
    _logger = logging.getLogger(__name__)

    __arguments = []

    @classmethod
    def get_args(cls):
        return cls.__arguments

    @classmethod
    def export_json(cls, analytics_meta, json, gridfs):
        cls.write_json(json, analytics_meta.db_ref, gridfs)
        cls._logger.info("Saved analytics %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.save()

    @classmethod
    def write_json(cls, data, name, gridfs):
        with gridfs.new_file(filename=name, content_type="text/json") as f:
            f.write(data)

    @classmethod
    def setup(cls, analytics_meta):
        dataset_meta = DatasetMeta.objects.get(id=analytics_meta.dataset_id)
        dbm = DatabaseManager()
        db_col = dbm.data_db.get_collection(dataset_meta.db_col)

        args = analytics_meta.specialised_args

        cls._logger.info("Found arguments %s", args)

        return dbm.gridfs, db_col, args, dataset_meta.schema

    @classmethod
    def join_keys(cls, *keys):
        return ".".join(keys)

    @classmethod
    def dollar_join_keys(cls, *keys):
        return "$" + cls.join_keys(keys)
