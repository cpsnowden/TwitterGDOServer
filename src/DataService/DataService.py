from TwitterService.TwitterSource import TwitterSource


class DataService(object):
    def __init__(self, dbm, data_service_cfg):
        self.dbm = dbm

        self.twitter_service = TwitterSource(self.dbm, data_service_cfg["twitter"])

        self.switch = {
            "Twitter_Stream": self.twitter_service
        }

    def delete(self, dataset_meta):

        service = self.switch.get(dataset_meta.type)
        service.delete(dataset_meta)

    def add(self, dataset_meta):

        service = self.switch.get(dataset_meta.type)
        service.add(dataset_meta)

    def stop(self, dataset_meta):

        service = self.switch.get(dataset_meta.type)
        service.stop(dataset_meta)

    def handle_status(self, dataset_meta, status):

        if status == "ORDERED":
            self.add(dataset_meta)

        if status == "STOPPED" and dataset_meta.status != "STOPPED":
            self.stop(dataset_meta)
