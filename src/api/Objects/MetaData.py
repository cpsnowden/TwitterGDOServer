import datetime

from mongoengine import connect, Document, StringField, ListField, DateTimeField, LongField, DictField
from flask_restful import fields
import json

connect("Meta", alias="meta_data_db")


class DatasetMeta(Document):
    description = StringField(required=True)
    tags = ListField(StringField())
    type = StringField(required=True)
    id = StringField(primary_key=True)
    db_col = StringField(required=True)
    status = StringField(required=True, default="ORDERED")
    start_time = DateTimeField(required=True, default=datetime.datetime.now())
    end_time = DateTimeField()
    collection_size = LongField(default=0)
    schema = StringField(default="RAW", choices=["RAW","T4J"])

    meta = {"db_alias": "meta_data_db"}


class AnalyticsMeta(Document):
    description = StringField()
    classification = StringField(required=True)
    type = StringField(required=True)
    db_ref = StringField(required=True)
    status = StringField(required=True, default="ORDERED")
    dataset_id = StringField(required=True)
    id = StringField(primary_key=True)
    start_time = DateTimeField(required=True, default=datetime.datetime.now())
    end_time = DateTimeField()
    meta = {"db_alias": "meta_data_db"}
    specialised_args = DictField()



class DictionaryWrap(fields.Raw):
    def format(self, value):
        return json.dumps(value)
