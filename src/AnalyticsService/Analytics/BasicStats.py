import json
import logging

from AnalyticsService.TwitterObj import Status
from AnalyticsService.Analytics.Analytics import Analytics

class BasicStats(Analytics):
    _logger = logging.getLogger(__name__)

    __type_name = "Basic_Stats"
    __arguments = [{"name":"topHashtagLimit","prettyName":"Number of top hashtags","type":"integer"}]

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(BasicStats, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    @classmethod
    def get(cls, analytics_meta):
        data = {}

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        hashtag_limit = args["topHashtagLimit"]

        if hashtag_limit > 0:
            name, d = cls.get_top_hashtags(schema_id, hashtag_limit, db_col)
            data[name] = d
        name, d = cls.get_languages(schema_id, db_col)
        data[name] = d
        name, d = cls.get_retweet_authored_dist(schema_id, db_col)
        data[name] = d

        cls.export_json(analytics_meta, json.dumps(data), gridfs)

        return True

    @classmethod
    def get_languages(cls, schema_id, db_col):
        cls._logger.info("Attempting to get languages")

        lang_key = Status.SCHEMA_MAP[schema_id]["language"]

        query = [
            {"$group": {"_id": {'id': '$' + lang_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}]

        top_hastags = db_col.aggregate(query, allowDiskUse=True)

        return "top_languages", list(top_hastags)

    @classmethod
    def get_retweet_authored_dist(cls, schema_id, db_col):
        cls._logger.info("Attempting to get retweet distribution")

        total = db_col.count()
        retweeted = db_col.find({Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": False}}).count()

        return "type_dist", {"retweets": retweeted, "non_retweets": total - retweeted}

    @classmethod
    def get_top_hashtags(cls, schema_id, limit, db_col):
        cls._logger.info("Attempting to get top hashtags")

        mention_key = Status.SCHEMA_MAP[schema_id]["hashtags"]

        top_user_query = [
            {"$unwind": "$" + mention_key},
            {"$group": {"_id": {'id': '$' + mention_key + '.' + 'text'}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}]

        top_hastags = db_col.aggregate(top_user_query, allowDiskUse=True)

        return "top_hashtags", list(top_hastags)