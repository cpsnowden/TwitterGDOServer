import numpy as np
import operator
class CommunityUser(object):

    UNCLASSIFIED = "Unclassified"

    def __init__(self, classification_scores):
        self.classifications = dict([(c,0.0) for c in classification_scores.keys()])
        self.classification_scores = classification_scores
        self.status_ids = []
        self.number_of_times_retweets = 0.0
        self.number_of_times_mentioned = 0.0

    @property
    def get_classification(self):

        max_score = max(self.classifications.iteritems(), key = operator.itemgetter(1))[1]
        # print max_score
        maxes = [c for (c,s) in self.classifications.items() if s == max_score]
        if len(maxes) > 1:
            return self.UNCLASSIFIED
        return maxes[0]

    def said(self, status):

        sid = str(status.get_id())
        if sid in self.status_ids:
            return self.get_classification

        self.status_ids.append(sid)

        hashtags = status.get_hashtags()

        for classification in self.classifications.keys():
            self.classifications[classification] += \
                self.get_score(hashtags, self.classification_scores[classification])
        return self.get_classification

    def retweeted_by(self, user):

        u_cls = user.get_classification
        if u_cls is not self.UNCLASSIFIED:
            self.classifications[u_cls] += 3
        self.number_of_times_retweets += 1
        return self.get_classification

    def get_number_statuses(self):
        return len(self.status_ids)

    def mentioned(self):
        self.number_of_times_mentioned +=1

    @staticmethod
    def get_score(hashtags, scores):
         return reduce(lambda x,y:x+y,map(lambda h: 1.0 if h.lower() in scores else 0.0, hashtags), 0.0)


class TweetScoring(object):

    @staticmethod
    def get_exp_score(a_score, b_score, MAX_TWEET_SCORE, time_constant = 2.0, ):
        return MAX_TWEET_SCORE * (- np.exp(-a_score / time_constant) + np.exp(-b_score / time_constant))

    @staticmethod
    def get_binary_score(a_score, b_score, MAX_TWEET_SCORE):

        if a_score > b_score:
            return MAX_TWEET_SCORE
        elif b_score > a_score:
            return -MAX_TWEET_SCORE
        else:
            return 0.0


class TwitterUserModel(object):

    MAX_USER_SCORE = 100

    scoring_options = {
        "Exponential": TweetScoring.get_exp_score,
        "HardBinary": TweetScoring.get_binary_score
    }

    def __init__(self, hashtag_scores, n = 10, scoring_system = "Exponential"):

        self.A, self.B = hashtag_scores

        self.scoring_function = self.scoring_options.get(scoring_system)

        self.score = 0.0
        self.history = []
        self.n = n
        self.MAX_TWEET_SCORE = self.MAX_USER_SCORE / float(self.n)

    def said_these(self, hashtags):
        tweet_score = self.get_tweet_score(hashtags)
        self.score = self.get_user_score(tweet_score)
        return self.score

    def get_user_score(self, new_score):
        self.history.append(new_score)
        self.history = self.history[-self.n:]
        return sum(self.history)

    def get_tweet_score(self, hashtags):
        A_score = reduce(lambda x, y: x + y, map(lambda h: float(self.A.get(h.lower(), 0.0)), hashtags), 0.0)
        B_score = reduce(lambda x, y: x + y, map(lambda h: float(self.B.get(h.lower(), 0.0)), hashtags), 0.0)

        return self.scoring_function(A_score, B_score, self.MAX_TWEET_SCORE)