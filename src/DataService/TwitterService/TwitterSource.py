from DataService.TwitterService.TwitterStreamer import TwitterStreamProvider
from DataService.TwitterService.TwitterConsumer import RouterManager
import datetime
from pymongo import ASCENDING
import logging


class TwitterSource(object):
    logger = logging.getLogger(__name__)

    def __init__(self, dbm, cfg, n_routers_init=2):
        self.streamer = TwitterStreamProvider(cfg["auth"])
        self.router = RouterManager(dbm, n_routers_init)
        self.pipes = []
        self.dbm = dbm

    def delete(self, dataset_meta):

        db_col = dataset_meta.db_col
        if db_col in self.dbm.data_db.collection_names():
            self.dbm.data_db.get_collection(db_col).drop()

    def setup_collection(self, name):

        self.dbm.data_db.get_collection(name).create_index([("ISO_created_at",ASCENDING)])
        self.dbm.data_db.get_collection(name).create_index([("id", ASCENDING)], unique=True)

    def get_status(self):

        return self.streamer.status.get()

    def add(self, dataset_info):

        db_col = dataset_info.db_col
        logging.info("Adding twitter sourced dataset %s", dataset_info.db_col)
        if db_col not in self.dbm.data_db.collection_names():
            self.logger.info("Setting up new collection for %s", db_col)
            self.setup_collection(db_col)


        self.pipes.append(dataset_info)
        self.streamer.run(self.get_tracking_terms())
        self.router.add_filter(dataset_info)

        logging.info("Filters: %d Consumers %d", self.router.n_filters, self.router.p_counter)

        dataset_info.status = "RUNNING"
        dataset_info.save()

    def stop(self, dataset_info):

        logging.info("Stopping twitter sourced dataset %s", dataset_info.description)

        try:

            self.pipes.remove(dataset_info)

        except ValueError:

            logging.info("Attempting to stop twitter sourced dataset %s that does not exist", dataset_info.description)
            dataset_info.status = "STOPPED"
            dataset_info.collection_size = self.dbm.data_db.get_collection(dataset_info.db_col).count()
            dataset_info.end_time = datetime.datetime.now()
            dataset_info.save()
            return

        self.router.remove_filter(dataset_info)

        if len(self.pipes) == 0:
            self.streamer.stop()
        else:
            self.streamer.run(self.get_tracking_terms())

        dataset_info.status = "STOPPED"
        dataset_info.collection_size = self.dbm.data_db.get_collection(dataset_info.db_col).count()
        dataset_info.end_time = datetime.datetime.now()
        dataset_info.save()

    def get_tracking_terms(self):

        terms = []
        for pipe in self.pipes:
            terms += pipe.tags

        return list(set(terms))
