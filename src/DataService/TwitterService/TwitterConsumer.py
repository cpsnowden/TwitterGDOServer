import json
import logging
import multiprocessing
from dateutil import parser
import os
import pika
from pymongo.errors import DuplicateKeyError
from src.AnalyticsService.TwitterObj import Status

class RouterManager(object):
    logger = logging.getLogger(__name__)

    def __init__(self, dbm, n_routers_init):
        self.dbm = dbm
        self.manager = multiprocessing.Manager()
        self.routerItems = self.manager.dict()
        self.n_filters = 0
        self.router_c = 0
        self.routers = {}

        for _ in range(n_routers_init):
            self.spin_up()

    def spin_up(self):

        self.logger.info("Spinning up a consumer")

        router_id = self.router_c
        self.router_c += 1
        stop_event = self.manager.Event()

        consumer = multiprocessing.Process(target=Router, args=(router_id, self.routerItems, stop_event, self.dbm))
        consumer.daemon = True
        consumer.start()
        self.routers[router_id] = (stop_event, consumer)

        return router_id, consumer

    def add_filter(self, dataset_info):

        self.logger.info("Adding filter desc: %s id: %s", dataset_info.description, dataset_info.id)

        filter_request = FilterRequest(dataset_info.id,
                                       dataset_info.description,
                                       [t.lstrip("#") for t in dataset_info.tags],
                                       dataset_info.db_col,
                                       dataset_info.schema)

        self.routerItems[dataset_info.id] = filter_request
        self.n_filters += 1

    def remove_filter(self, dataset_info):

        del self.routerItems[dataset_info.id]
        self.n_filters -= 1

    def spin_down(self, router_id):

        logging.info("Attempting to spin down %d", router_id)

        (stop_event, consumer) = self.routers[router_id]
        stop_event.set()
        del self.routers[router_id]

    def auto_spin_down(self):
        if len(self.routers) > 0:
            self.spin_down(self.routers.keys()[-1])

    @property
    def p_counter(self):

        return len(self.routers)

    def __str__(self):
        return str(self.routers)


class FilterRequest(object):
    def __init__(self, filter_id, description, keys, db_col, schema):
        self.keys = [k.lower() for k in keys]
        self.description = description
        self.id = filter_id
        self.db_col = db_col
        self.schema = schema

    def __repr__(self):
        return self.keys, self.description

    def __str__(self):
        return self.__repr__()


class Router(object):
    logger = logging.getLogger(__name__)

    def __init__(self, router_id, filter_list, stop_event, dbm):

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

        self.channel = connection.channel()
        self.channel.queue_declare(queue="twitter_stream")
        self.channel.basic_consume(self.consume, queue="twitter_stream")
        self.dbm = dbm
        self.id = router_id
        self.filter_list = filter_list
        self.stop_event = stop_event

        self.logger.info("Constructed router instance with id %s", self.id)

        self.run()

    def run(self):
        self.logger.warning("Trying to start router with pid: %d ", os.getpid())

        self.channel.start_consuming()

    def consume(self, ch, method, properties, body):

        json_status = json.loads(body)
        for f in self.filter_list.values():

            hashtags = [h["text"].lower() for h in json_status["entities"]["hashtags"]]

            if any(ht in hashtags for ht in f.keys):
                created_date_string = json_status[Status.SCHEMA_MAP[f.schema]["created_at"]]
                json_status["ISO_created_at"] = parser.parse(created_date_string)
                try:
                    self.dbm.data_db.get_collection(f.db_col).insert(json_status)
                except DuplicateKeyError:
                    self.logger.exception("Duplicate tweet id ignoring")

                self.logger.info("id: %d descr: %s user: %s text: %s", self.id, f.description,
                                 json_status["user"]["screen_name"],
                                 json_status["text"].replace("\n", ""))

        ch.basic_ack(delivery_tag=method.delivery_tag)

        if self.stop_event.is_set():
            self.logger.info("Stopping router id: %d pid: %d", self.id, os.getpid())
            self.channel.stop_consuming()
