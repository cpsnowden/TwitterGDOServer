import api.Utils.MetaID as MetaID
from api.Auth import Resource
from api.Objects.MetaData import AnalyticsMeta, DatasetMeta, DictionaryWrap
from flask_restful import reqparse, marshal_with, fields, abort, marshal
from flask import Response, make_response, redirect, url_for
from mongoengine.queryset import DoesNotExist
import requests
from src.AnalyticsService.AnalyticsTasks import generate_graph
import logging
import gridfs


analytics_meta_fields = {
    "type": fields.String,
    "id": fields.String(attribute="id"),
    "datasetId": fields.String(attribute="dataset_id"),
    "startDate": fields.DateTime(attribute="start_time"),
    "endDate": fields.DateTime(attribute="end_time"),
    "status": fields.String,
    "db_ref": fields.String,
    "specialised_args": DictionaryWrap(attribute="specialised_args")
}


class AnalyticsList(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('type', type=str, help="Type of analytics")
        self.parser.add_argument('specialised_args', type=dict, help="Type of analytics")

    @marshal_with(analytics_meta_fields)
    def get(self, dataset_id):

        return list(AnalyticsMeta.objects(dataset_id=dataset_id))

    @marshal_with(analytics_meta_fields)
    def post(self, dataset_id):
        args = self.parser.parse_args()

        dataset_meta = DatasetMeta.objects.get(id=dataset_id)

        short_id, long_id = MetaID.get_id(dataset_meta.db_col, prefix="A_")

        analytics_meta = AnalyticsMeta(type=args["type"],
                                       dataset_id=dataset_id,
                                       id=long_id,
                                       db_ref=short_id,
                                       specialised_args=args["specialised_args"])

        analytics_meta.save()

        generate_graph.delay(analytics_meta.id)

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
            self.logger.warning("No file associated with analytics %s",found.id)

        found.delete()

        return "", 204

class AnalyticsData(Resource):

    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):

        self.dbm = kwargs["dbm"]

    def get(self, dataset_id, analytics_id):

        try:
            found =  AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

        f = self.dbm.gridfs.get_last_version(found.db_ref)

        response = make_response(f.read())
        response.headers["Content-Disposition"] = "attachment; filename=books.csv"

        return response

    def delete(self, dataset_id, analytics_id):

        self.logger.info("Attempting to delete data for analytics %s", analytics_id)

        try:
            found =  AnalyticsMeta.objects.get(id=analytics_id, dataset_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Analytics {} does not exist".format(analytics_id))

        try:
            f = self.dbm.gridfs.get_last_version(found.db_ref)
            self.dbm.gridfs.delete(f._id)
        except gridfs.NoFile:
            self.logger.warning("No file associated with analytics %s",found.id)

        return "", 204