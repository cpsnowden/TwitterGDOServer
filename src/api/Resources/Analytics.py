import logging

import gridfs
import api.Utils.MetaID as MetaID
from flask import make_response
from flask_restful import reqparse, marshal_with, fields, abort
from mongoengine.queryset import DoesNotExist
from AnalyticsService.AnalyticsTasks import get_analytics
from AnalyticsService.AnalyticsEngine import AnalyticsEngine
from api.Auth import Resource
from api.Objects.MetaData import AnalyticsMeta, DatasetMeta, DictionaryWrap

analytics_meta_fields = {
    "type": fields.String,
    "id": fields.String(attribute="id"),
    "datasetId": fields.String(attribute="dataset_id"),
    "start_time": fields.DateTime(attribute="start_time"),
    "end_time": fields.DateTime(attribute="end_time"),
    "status": fields.String,
    "db_ref": fields.String,
    "specialised_args": DictionaryWrap(attribute="specialised_args"),
    "classification": fields.String,
    "description": fields.String
}


class AnalyticsList(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('type', type=str, help="Type of analytics")
        self.parser.add_argument('specialised_args', type=dict, help="Specialised Arguments")
        self.parser.add_argument('classification', type=str, help="Classification of analytics")
        self.parser.add_argument('description', type=str, help="Description of analytics")

    @marshal_with(analytics_meta_fields)
    def get(self, dataset_id):
        return list(AnalyticsMeta.objects(dataset_id=dataset_id))

    @marshal_with(analytics_meta_fields)
    def post(self, dataset_id):
        args = self.parser.parse_args()

        dataset_meta = DatasetMeta.objects.get(id=dataset_id)

        short_id, long_id = MetaID.get_id(dataset_meta.db_col, prefix="A_")

        analytics_meta = AnalyticsMeta(classification=args["classification"],
                                       type=args["type"],
                                       description=args["description"],
                                       dataset_id=dataset_id,
                                       id=long_id,
                                       db_ref=short_id,
                                       specialised_args=args["specialised_args"])

        analytics_meta.save()

        get_analytics.delay(analytics_meta.id)

        return analytics_meta


class Analytics(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):

        self.dbm = kwargs["dbm"]

    @marshal_with(analytics_meta_fields)
    def get(self, dataset_id, analytics_id):

        try:
            return AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

    def delete(self, dataset_id, analytics_id):

        try:
            found = AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)

        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

        try:
            f = self.dbm.gridfs.get_last_version(found.db_ref)
            self.dbm.gridfs.delete(f._id)
        except gridfs.NoFile:
            self.logger.warning("No file associated with analytics %s", found.id)

        found.delete()

        return "", 204


class AnalyticsData(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):

        self.dbm = kwargs["dbm"]

    def get(self, dataset_id, analytics_id):

        try:
            found = AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

        f = self.dbm.gridfs.get_last_version(found.db_ref)

        response = make_response(f.read())

        response.headers["Content-Disposition"] = "attachment"
        response.headers["mimetype"] = "text/plain"
        return response

    def delete(self, dataset_id, analytics_id):

        self.logger.info("Attempting to delete data for analytics %s", analytics_id)

        try:
            found = AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

        try:
            f = self.dbm.gridfs.get_last_version(found.db_ref)
            self.dbm.gridfs.delete(f._id)
        except gridfs.NoFile:
            self.logger.warning("No file associated with analytics %s", found.id)

        return "", 204


class AnalyticsOptions(Resource):
    logging = logging.getLogger(__name__)

    def get(self):
        return AnalyticsEngine.get_details()
