import json
import logging

from AnalyticsService.Analytics.Analytics import Analytics
from AnalyticsService.TwitterObj import Status, User, UserMention


class TopUsers(Analytics):
    _logger = logging.getLogger(__name__)

    __type_name = "Top_Users"
    __arguments = [{"name":"topRetweetedLimit","prettyName":"Number of top retweeted users", "type": "integer"},
                 {"name":"topRetweeterLimit","prettyName":"Number of top retweeting users","type": "integer"},
                 {"name":"topMentionedLimit","prettyName":"Number of top mentioned users","type": "integer"},
                 {"name":"topOriginalAuthorLimit","prettyName":"Number of top non-retweeting users","type": "integer"}]

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(TopUsers, cls).get_args()

    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    @classmethod
    def get(cls, analytics_meta):

        data = {}
        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        retweeted_limit = args["topRetweetedLimit"]
        retweeter_limit = args["topRetweeterLimit"]
        mention_limit = args["topMentionedLimit"]
        original_author_limit = args["topOriginalAuthorLimit"]

        if retweeted_limit > 0:
            name, d = cls.get_top_retweeted(schema_id, retweeted_limit, db_col)
            data[name] = d
        if retweeter_limit > 0:
            name, d = cls.get_top_retweeters(schema_id, retweeter_limit, db_col)
            data[name] = d
        if mention_limit > 0:
            name, d = cls.get_top_mentioned(schema_id, mention_limit, db_col)
            data[name] = d
        if original_author_limit > 0:
            name, d = cls.get_top_authors(schema_id, original_author_limit, db_col)
            data[name] = d

        cls.export_json(analytics_meta, json.dumps(data), gridfs)

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
            {"$limit": limit}]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return "top_original_authors", list(top_users)

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
            {"$limit": limit}]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return "top_mentioned", list(top_users)

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
            {"$limit": limit}]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return "top_retweeted", list(top_users)

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
            {"$limit": limit}]

        top_users = db_col.aggregate(top_user_query, allowDiskUse=True)

        return "top_retweeters", list(top_users)