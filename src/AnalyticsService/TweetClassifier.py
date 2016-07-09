class TweetClassifier(object):
    @staticmethod
    def classify_tweet_by_hashtag(tweet, h_tag_scores):

        score = 0
        for h_tag in tweet.get_hashtags():
            if h_tag.lower() in h_tag_scores:
                score += h_tag_scores[h_tag.lower()]
        return score