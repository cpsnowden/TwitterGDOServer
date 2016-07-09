from api.Auth import Resource
from api.Objects.MetaData import DatasetMeta
from api.Utils import MetaID
from flask_restful import reqparse, marshal_with, fields, abort
from mongoengine.queryset import DoesNotExist
import logging

dataset_meta_fields = {
    "description": fields.String,
    "tags": fields.List(fields.String),
    "type": fields.String,
    "id": fields.String(attribute="id"),
    "startDate": fields.DateTime(attribute="start_time"),
    "endDate": fields.DateTime(attribute="end_time"),
    "status": fields.String,
    "db_col": fields.String,
    "filterSize": fields.Integer(attribute="collection_size")
}

status_meta_fields = {
    "status": fields.String
}


class Dataset(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        self.data_service = kwargs["data_service"]

    @marshal_with(dataset_meta_fields)
    def get(self, dataset_id):

        try:
            return DatasetMeta.objects.get(consumer_id=dataset_id)
        except DoesNotExist:
            abort(404, message="Dataset {} does not exist".format(dataset_id))

    def delete(self, dataset_id):

        try:
            found = DatasetMeta.objects.get(consumer_id=dataset_id)
            self.data_service.delete(found)
            found.delete()
        except DoesNotExist:
            abort(404, message="Dataset {} does not exist".format(dataset_id))

        return "", 204


class DatasetStatus(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        self.data_service = kwargs["data_service"]
        self.parser = reqparse.RequestParser()
        allowable_status = ["STOPPED", "ORDERED"]
        self.parser.add_argument("status", type=str, choices=allowable_status, help="Status of dataset must be one "
                                                                                    "of " + str(allowable_status))

    @marshal_with(status_meta_fields)
    def get(self, dataset_id):
        try:
            return DatasetMeta.objects.get(id=dataset_id)
        except DoesNotExist:
            abort(404, message="Dataset {} does not exist".format(dataset_id))

    @marshal_with(status_meta_fields)
    def put(self, dataset_id):
        args = self.parser.parse_args()
        status = args["status"]

        try:
            found = DatasetMeta.objects.get(id=dataset_id)
            self.data_service.handle_status(found, status)
            return found, 201
        except DoesNotExist:
            abort(404, message="Dataset {} does not exist".format(dataset_id))


class DatasetList(Resource):
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('type', type=str, help="Type of dataset")
        self.parser.add_argument('tags', type=str, action="append", help="Tags for dataset")
        self.parser.add_argument('description', type=str, help="Description of dataset")

        self.data_service = kwargs["data_service"]

    @marshal_with(dataset_meta_fields)
    def get(self):
        return list(DatasetMeta.objects)

    @marshal_with(dataset_meta_fields)
    def post(self):
        args = self.parser.parse_args()

        description = args["description"]
        short_id, long_id = MetaID.get_cammel_id(description)
        dataset_meta = DatasetMeta(description=args["description"],
                                   tags=args["tags"],
                                   type=args["type"],
                                   id=long_id,
                                   db_col=short_id)

        dataset_meta.save()

        self.data_service.add(dataset_meta)

        return dataset_meta
