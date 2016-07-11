import logging
from datetime import datetime
import json
import numpy
import pandas as pd
from src.AnalyticsService.AnalyticsUtils import AnalyticsUtils
from src.AnalyticsService.TwitterObj import Status, User, UserMention


class BasicAnalytics(object):
    _logger = logging.getLogger(__name__)

    @classmethod
    def get_basic_stats(cls, analytics_meta):

        data = {}

        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        hashtag_limit = args["Limit_Top_Hashtags"]

        if hashtag_limit > 0:
            name, d = BasicAnalytics.get_top_hashtags(schema_id, hashtag_limit, db_col)
            data[name] = d

        AnalyticsUtils.export(analytics_meta, gridfs, json.dumps(data), AnalyticsUtils.write_json)

        cls._logger.info("Saved %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True

    @classmethod
    def get_top_hashtags(cls, schema_id, limit, db_col):

        cls._logger.info("Attempting to get top hashtags")

        mention_key = Status.SCHEMA_MAP[schema_id]["hashtags"]

        top_user_query = [
            {"$unwind": "$" + mention_key},
            {"$group": {"_id": {'id': '$' + mention_key + '.' + 'text'}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        top_hastags = db_col.aggregate(top_user_query, allowDiskUse=True)

        return ("top_hashtags", list(top_hastags))

    @classmethod
    def get_top_users(cls, analytics_meta):

        cls._logger.info("Attempting to get top users")

        data = {}
        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        retweeted_limit = args["Limit_Top_Retweeted_Users"]
        retweeter_limit = args["Limit_Top_Retweeters"]
        mention_limit = args["Limit_Top_Mentioned"]
        original_author_limit = args["Limit_Top_Original_Authors"]

        if retweeted_limit > 0:
            name, d = BasicAnalytics.get_top_retweeted(schema_id, retweeted_limit, db_col)
            data[name] = d
        if retweeter_limit > 0:
            name, d = BasicAnalytics.get_top_retweeters(schema_id, retweeter_limit, db_col)
            data[name] = d
        if mention_limit > 0:
            name, d = BasicAnalytics.get_top_mentioned(schema_id, mention_limit, db_col)
            data[name] = d
        if original_author_limit > 0:
            name, d = BasicAnalytics.get_top_authors(schema_id, mention_limit, db_col)
            data[name] = d

        AnalyticsUtils.export(analytics_meta, gridfs, json.dumps(data), AnalyticsUtils.write_json)

        cls._logger.info("Saved %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True

    @classmethod
    def get_top_authors(cls, schema_id, limit, db_col):

        cls._logger.info("Attempting to get top authoring users")

        user_key = Status.SCHEMA_MAP[schema_id]["user"]
        user_id_key = user_key + "." + User.SCHEMA_MAP[schema_id]["id"]
        user_name_key = user_key + "." + User.SCHEMA_MAP[schema_id]["name"]

        top_user_query = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": False}}},
            {"$group": {"_id": {'id': '$' + user_id_key, 'name': '$' + user_name_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return ("top_original_authors", list(top_users))


    @classmethod
    def get_top_mentioned(cls, schema_id, limit, db_col):

        cls._logger.info("Attempting to get top mentioned users")

        mention_key = Status.SCHEMA_MAP[schema_id]["mentions"]
        user_id_key = mention_key + "." + UserMention.SCHEMA_MAP[schema_id]["id"]
        user_name_key = mention_key + "." + UserMention.SCHEMA_MAP[schema_id]["name"]

        top_user_query = [
            {"$unwind": "$" + mention_key},
            {"$group": {"_id": {'id': '$' + user_id_key, 'name': '$' + user_name_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return ("top_mentioned", list(top_users))

    @classmethod
    def get_top_retweeted(cls, schema_id, limit, db_col):

        cls._logger.info("Attempting to get top retweeted users")

        retweet_key = Status.SCHEMA_MAP[schema_id]["retweeted_status"]
        user_key = Status.SCHEMA_MAP[schema_id]["user"]
        user_id_key = retweet_key + "." + user_key + "." + User.SCHEMA_MAP[schema_id]["id"]
        user_name_key = retweet_key + "." + user_key + "." + User.SCHEMA_MAP[schema_id]["name"]

        top_user_query = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {'id': '$' + user_id_key, 'name': '$' + user_name_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return ("top_retweeted", list(top_users))

    @classmethod
    def get_top_retweeters(cls, schema_id, limit, db_col):

        cls._logger.info("Attempting to get top retweeters")

        user_key = Status.SCHEMA_MAP[schema_id]["user"]
        user_id_key = user_key + "." + User.SCHEMA_MAP[schema_id]["id"]
        user_name_key = user_key + "." + User.SCHEMA_MAP[schema_id]["name"]

        top_user_query = [
            {"$match": {Status.SCHEMA_MAP[schema_id]["retweeted_status"]: {"$exists": True, "$ne": None}}},
            {"$group": {"_id": {'id': '$' + user_id_key, 'name': '$' + user_name_key}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return ("top_retweeters", list(top_users))

    @classmethod
    def get_time_distribution(cls, analytics_meta):

        cls._logger.info("Attempting to get time distribution")

        gridfs, db_col, args, schema_id = AnalyticsUtils.setup(analytics_meta)

        time_quantum = args["Time_Quantum"]
        options = {
            "Minute": "min",
            "Hour": "H",
            "Day": "D",
            "Week": "W",
            "Month": "M"
        }

        try:
            time_quantum = options[time_quantum]
        except KeyError:
            cls._logger.exception("Wrong time quantum given")
            return False

        dates = [Status(c, schema_id).get_created_at()
                 for c in db_col.find({},
                                      {Status.SCHEMA_MAP[schema_id]["created_at"]: 1})]

        index = pd.DatetimeIndex(dates)

        df = pd.DataFrame(numpy.ones(len(index)), index=index, columns=["Count"])
        gb = df.groupby(pd.TimeGrouper(freq=time_quantum)).count()

        data = gb.to_json(date_format='iso')

        cls._logger.info("Built analytics %s", analytics_meta.id)
        analytics_meta.status = "BUILT"
        analytics_meta.save()

        AnalyticsUtils.export(analytics_meta, gridfs, data, AnalyticsUtils.write_json)

        cls._logger.info("Saved %s", analytics_meta.db_ref)
        analytics_meta.status = "SAVED"
        analytics_meta.end_time = datetime.now()
        analytics_meta.save()

        return True
