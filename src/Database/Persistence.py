from pymongo import MongoClient
import gridfs


class DatabaseManager(object):
    def __init__(self, url=None):
        self.client = MongoClient(url, connect=False)
        self.data_db = self.client.get_database("DATA")
        self.gridfs = gridfs.GridFS(self.client.get_database("FILE_DATA"))

class StatusDAO(object):
    def __init__(self, collection):
        self.collection = collection

    def insert(self, status_json):
        self.collection.insert(status_json)

    def get_cursor(self, query, limit):
        return self.collection.find(query).limit(limit)
