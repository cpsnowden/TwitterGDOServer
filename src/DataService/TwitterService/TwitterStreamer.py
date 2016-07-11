import json
import logging

import pika
import tweepy


class TwitterStreamProvider(object):
    logger = logging.getLogger(__name__)

    def __init__(self, auth):
        self.auth = tweepy.OAuthHandler(auth["consumer_key"], auth["consumer_secret"])
        self.auth.set_access_token(auth["access_token"], auth["access_token_secret"])
        self.listener = MongoStreamListener()
        self.stream = None

    def run(self, tracking_terms):
        if self.stream is not None:
            self.stop()

        self.logger.info("Running twitter stream with terms %s", str(tracking_terms))

        self.stream = tweepy.Stream(self.auth, self.listener)
        self.stream.filter(track=tracking_terms, async=True)

    def stop(self):
        self.logger.info("Shutting down twitter stream as last filter removed")
        self.stream.disconnect()
        self.stream = None


class MongoStreamListener(tweepy.StreamListener):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super(MongoStreamListener, self).__init__()
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

        self.channel = connection.channel()
        self.channel.queue_declare(queue='twitter_stream')

    def on_status(self, status):
        self.channel.basic_publish(exchange="", routing_key="twitter_stream", body=json.dumps(status._json))

    def on_error(self, status_code):
        if status_code == 420:
            self.logger.error("Too many connections stopping listener")
            return False
        else:
            self.logger.error("Tweepy exception %d stopping listener", status_code)
        return super(MongoStreamListener, self).on_error(status_code)
