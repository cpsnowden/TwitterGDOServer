from api.Auth import Resource
from flask_restful import marshal_with, fields, abort

twitter_consumer_meta = {
    "id": fields.Integer(attribute="consumer_id"),
    "pid": fields.Integer,
    "alive": fields.Boolean,
    "url": fields.Url("consumer")
}


class TwitterConsumerMeta(object):
    def __init__(self, consumer_id, pid, alive):
        self.consumer_id = consumer_id
        self.alive = alive
        self.pid = pid


class TwitterConsumerList(Resource):
    def __init__(self, **kwargs):
        self.twitter_service = kwargs["twitter_service"]

    @marshal_with(twitter_consumer_meta)
    def post(self):
        consumer_id, c = self.twitter_service.router.spin_up()

        return TwitterConsumerMeta(consumer_id, c.ident, c.is_alive())

    @marshal_with(twitter_consumer_meta)
    def get(self):
        consumers = []

        for c_key in self.twitter_service.router.routers.keys():
            _, c = self.twitter_service.router.routers[c_key]
            consumers.append(TwitterConsumerMeta(c_key, c.ident, c.is_alive()))

        return consumers


class TwitterConsumer(Resource):
    def __init__(self, **kwargs):
        self.twitter_service = kwargs["twitter_service"]

    @marshal_with(twitter_consumer_meta)
    def get(self, consumer_id):

        try:
            _, c = self.twitter_service.router.routers[int(consumer_id)]
            return TwitterConsumerMeta(consumer_id, c.ident, c.is_alive())
        except KeyError:
            abort(404, message="Consumer {} does not exist".format(consumer_id))

    def delete(self, consumer_id):

        self.twitter_service.router.spin_down(int(consumer_id))

        return "", 204
