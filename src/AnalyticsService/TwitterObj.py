from dateutil import parser


class Status(object):
    T4J = dict(hashtags="hashtagEntities", mentions="userMentionEntities", user="user", text="text",
               created_at="createdAt", id="id", retweeted_status="retweetedStatus", language="lang", ISO_date = "createdAt")

    RAW = dict(hashtags="entities.hashtags", mentions="entities.user_mentions", user="user", text="text",
               created_at="created_at", id="id", retweeted_status="retweeted_status", language="lang", ISO_date = "ISO_created_at")

    SCHEMA_MAP = {
        "T4J": T4J,
        "RAW": RAW
    }

    def __init__(self, json, schema_id):
        self.item = DictionaryWrapper(json)
        self.SCHEMA = self.SCHEMA_MAP[schema_id]
        self.SCHEMA_ID = schema_id

    def get(self, key):
        return self.item.get(self.SCHEMA[key])

    def get_hashtags(self):
        hashtag_list = self.get("hashtags")
        return [h_tag["text"] for h_tag in hashtag_list]

    def get_mentions(self):
        mention_list = self.get("mentions")
        return [UserMention(json, self.SCHEMA_ID) for json in mention_list]

    def get_user(self):
        return User(self.get("user"), self.SCHEMA_ID)

    def get_text(self):
        return self.get("text")

    def get_created_at(self):
        return TwitterDate(self.get("created_at"), self.SCHEMA_ID).get_date_time()

    def get_id(self):
        return self.get("id")

    def get_retweet_status(self):
        return Status(self.get("retweeted_status"), self.SCHEMA_ID)


def conv_dt(raw):
    return raw


def conv_json_dt(raw):
    return parser.parse(raw)
    # return datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")


class TwitterDate(object):
    SCHEMA_MAP = {
        "T4J": conv_dt,
        "RAW": conv_json_dt
    }

    def __init__(self, raw, schema_id):
        self.SCHEMA_ID = schema_id
        self.raw = raw

    def get_date_time(self):
        return self.SCHEMA_MAP[self.SCHEMA_ID](self.raw)


class User(object):
    T4J = dict(id="id", name="screenName", follower_count="followersCount", friends_count="friendsCount", lang="lang")
    RAW = dict(id="id", name="screen_name", follower_count="followers_count", friends_count="friends_count",
               lang="lang")
    SCHEMA_MAP = {
        "T4J": T4J,
        "RAW": RAW
    }

    def __init__(self, json, schema_id):
        self.item = DictionaryWrapper(json)
        self.SCHEMA = self.SCHEMA_MAP[schema_id]
        self.SCHEMA_ID = schema_id

    def get(self, key):
        return self.item.get(self.SCHEMA[key])

    def get_name(self):
        return self.get("name")

    def get_id(self):
        return self.get("id")

    def get_follower_count(self):
        return self.get("follower_count")

    def get_friends_count(self):
        return self.get("friends_count")

    def get_lang(self):
        return self.get("lang")


class UserMention(object):
    T4J = dict(id="id", name="screenName")
    RAW = dict(id="id", name="screen_name")

    SCHEMA_MAP = {
        "T4J": T4J,
        "RAW": RAW
    }

    def __init__(self, json, schema_id):
        self.item = DictionaryWrapper(json)
        self.SCHEMA = self.SCHEMA_MAP[schema_id]
        self.SCHEMA_ID = schema_id

    def get(self, key):
        return self.item.get(self.SCHEMA[key])

    def get_user_id(self):
        return self.get("id")

    def get_user_name(self):
        return self.item.get(self.SCHEMA["name"])


class DictionaryWrapper(object):
    def __init__(self, json):
        self.item = json

    def get(self, item):
        return self.__getitem__(item)

    def put(self, key, value):
        return self.__setitem__(key, value)

    def __getitem__(self, item):

        return DictionaryWrapper.rec_get(self.item, item)

    def __setitem__(self, key, value):

        return DictionaryWrapper.rec_put(self.item, key, value)

    @staticmethod
    def rec_put(d, keys, item):
        if "." in keys:
            key, rest = keys.split(".", 1)
            if key not in d:
                d[key] = {}
            DictionaryWrapper.rec_put(d[key], rest, item)
        else:
            d[keys] = item

    @staticmethod
    def rec_get(d, keys):
        if "." in keys:
            key, rest = keys.split(".", 1)
            return DictionaryWrapper.rec_get(d[key], rest)
        else:
            return d[keys]
