import logging

import Resources.Analytics
import flask_restful
import yaml
from DataService.DataService import DataService
from Database.Persistence import DatabaseManager
from Resources.Dataset import Dataset, DatasetList, DatasetStatus
from Resources.TwitterConsumer import TwitterConsumer, TwitterConsumerList
from flask import Flask

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
api = flask_restful.Api(app)

with open("config.yml", 'r') as config_file:
    cfg = yaml.load(config_file)

dbm = DatabaseManager()
data_service = DataService(dbm, cfg["data_service_cfg"])

api.add_resource(Dataset, '/API/dataset/<dataset_id>', resource_class_kwargs={'data_service': data_service})
api.add_resource(DatasetList, '/API/dataset', resource_class_kwargs={'data_service': data_service})
api.add_resource(DatasetStatus, '/API/dataset/<dataset_id>/status', resource_class_kwargs={'data_service': data_service})
api.add_resource(TwitterConsumer, '/API/twitter_consumer/<consumer_id>', resource_class_kwargs={
    'twitter_service': data_service.twitter_service}, endpoint="consumer")
api.add_resource(TwitterConsumerList, '/API/twitter_consumer', resource_class_kwargs={
    'twitter_service': data_service.twitter_service})

api.add_resource(Resources.Analytics.AnalyticsList, '/API/dataset/<dataset_id>/analytics')
api.add_resource(Resources.Analytics.Analytics, '/API/dataset/<dataset_id>/analytics/<analytics_id>',
                 resource_class_kwargs = {'dbm': dbm})
api.add_resource(Resources.Analytics.AnalyticsData, '/API/dataset/<dataset_id>/analytics/<analytics_id>/data',
                 resource_class_kwargs = {'dbm': dbm}, endpoint = "AnalyticsData")

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
