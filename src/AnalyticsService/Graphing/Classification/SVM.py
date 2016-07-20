import csv
import sys

import nltk
import re
import numpy as np
from nltk.corpus import stopwords
from nltk.corpus import wordnet as wn
from nltk.tokenize import TweetTokenizer
from sklearn import svm

reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')


class TweetPreprocessing(object):
    LABELS = {
        "leave": 0,
        "remain": 1
    }
    INV_LAVEL = dict([(LABELS[i],i) for i in LABELS.keys()])

    def __init__(self):
        self.tknzr = TweetTokenizer(preserve_case=False)
        self.url_regex = re.compile(r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+')
        self.punct = re.compile('.*[A-Za-z].*')
        self.tweets = []
        self.clf = svm.SVC()
        self.lemmatizer = nltk.WordNetLemmatizer()

    def tokenize_tweet(self, tweet_text, remove_single_punct=True):
        # print tweet_text
        tokens = [i for i in self.tknzr.tokenize(tweet_text) if i not in stopwords.words("english")]
        if remove_single_punct:
            tokens = [i for i in tokens if self.punct.match(i)]
        tokens = [i for i in tokens if not self.url_regex.match(i)]

        t = []
        for token, tag in nltk.pos_tag(tokens):
            lemma = self.lemmantize(token, tag)
            # yield lemma
            t.append(lemma)

        return t

    def get_feature_list_and_labels(self, tweets, featureList):
        sfl = sorted(featureList)
        map = {}
        fv = []
        labels = []

        for tweet, label in tweets:
            for w in sfl:
                map[w] = 0
            for word in tweet:
                if word.lower() in map:
                    map[word.lower()] = 1
            fv.append(map.values())
            labels.append(self.LABELS.get(label))
        return fv, labels

    def get_feature_vector(self, tweet, featureList):
        sfl = sorted(featureList)
        map = {}

        for w in sfl:
            map[w] = 0
        for word in self.tokenize_tweet(tweet):
            if word.lower() in map:
                map[word.lower()] = 1
        return map.values()

    def load_training(self, path):

        data = csv.reader(open(path, 'rb'), delimiter=',', quotechar='|')
        for i,j in enumerate(data):
            classif = j[0]
            txt = j[1]
            # if i > 200:
            #     break
            self.tweets.append((self.tokenize_tweet(txt), classif))

        self.taining, self.labels = self.get_feature_list_and_labels(self.tweets, self.feature_list)
        # print self.labels

    def train(self):
        self.clf.fit(self.taining, self.labels)

    def load_feature_list(self, path):

        data = csv.reader(open(path, 'rb'), delimiter=',')
        self.feature_list = [d[0] for d in data]
        self.feature_list = self.feature_list[:100]

    def predict(self, tweet):

        feature_vector = self.get_feature_vector(tweet, self.feature_list)
        prediction = self.clf.predict(np.array(feature_vector).reshape(1,-1))
        certain = self.clf.decision_function(np.array(feature_vector).reshape(1,-1))
        return prediction, certain

    def get_feature_list(self, tweet_texts):
        return nltk.FreqDist(reduce(lambda x, y: x + y, map(self.tokenize_tweet, tweet_texts), []))

    def lemmantize(self, token, tag):
        tag = {
            'N': wn.NOUN,
            'V': wn.VERB,
            'R': wn.ADV,
            'J': wn.ADJ
        }.get(tag[0], wn.NOUN)
        return self.lemmatizer.lemmatize(token, tag)

    def setup(self, feature_list_path, training_path):
        self.load_feature_list(feature_list_path)
        self.load_training(training_path)
        self.train()


tp = TweetPreprocessing()
# data = csv.reader(open("training_data.txt", 'rb'), delimiter=',', quotechar='|')
# feature_list = tp.get_feature_list(d[1] for d in data)
# print "Getting most common"
# most_common = feature_list.most_common(2000)
#
# with open("common_terms_from_trainingv2.txt", "w") as f:
#     f.write(('\n'.join('%s,%s' % x for x in most_common)))
tp.load_feature_list("common_terms_from_trainingv2.txt")
tp.load_training("TRAINING_DATA.csv")
tp.train()
# print tp.get_feature_vector("#brexit", tp.feature_list)

from sklearn import metrics

true_labels = []
pre_labels = []
print "Trained"
def test(path, tb):
    data = csv.reader(open(path, 'rb'), delimiter=',', quotechar='|')
    for row in data:
        # i = raw_input()
        # if i == "EXIT":
        #     break
        pre_label = row[0]
        text = row[1]
        post_label_n, conf = tb.predict(text)

        pst_label = tb.INV_LAVEL.get(post_label_n[0])
        if abs(conf) < 0.5:
            pst_label = "Unknown"
            post_label_n[0] = 2
        if pst_label != pre_label:
            print pst_label, pre_label, conf, text
        true_labels.append(tb.LABELS.get(pre_label))
        pre_labels.append(post_label_n[0])
    # print true_labels, pre_labels
    print metrics.classification_report(true_labels, pre_labels, target_names=tb.LABELS.keys() + ["Unknown"])


test("TEST_DATA.csv", tp)
#
# tp.setup()
# while (True):
#     text = raw_input('Enter txt: ')
#     p, c = tp.predict(text)
#     print p,tp.LABELS.get(p[0]), c
# # #
# # #
# dbcol = pymongo.MongoClient().get_database("DATA").get_collection("Brexit_old")
# c = dbcol.find({"retweetedStatus":{"$exists":False}})
#
# words = []
#
# import random
# #
# data = csv.reader(open("training_data.txt", 'rb'), delimiter=',', quotechar='|')
#
# datav = []
# for i in data:
#     datav.append(i)
#
# random.shuffle(datav)
# train_data = datav[:20000]
# test_data = datav[20000:]
# with open("TRAINING_DATA.csv","w") as csvfile:
#     wr = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_ALL)
#     for row in train_data:
#         wr.writerow(row)
# with open("TEST_DATA.csv", "w") as csvfile:
#     wr = csv.writer(csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_ALL)
#     for row in test_data:
#         wr.writerow(row)

# print c.count()
# bar = progressbar.ProgressBar()
# for tweet in bar(c):
#     tokens = tp.tokenize_tweet(tweet["text"])
#     words.extend(tokens)
#
# dist = nltk.FreqDist(words)
# print
# with open("common_terms.txt","w") as f:
#     f.write(('\n'.join('%s,%s' % x for x in dist.most_common(2000))))


# exit()
#
#
# negative = "This sucks"
# positive = "Tom is a boss"
#
# training_tweet = [(negative, 0),
#                   (positive, 1)]
#
# feature_list = ["sucks", "boss"]
#
#
# feature_vector, labels = get_feature_list_and_labels(training_tweet, feature_list)
#
# clf = svm.SVC()
# print clf.fit(feature_vector, labels)
#
# fv = get_feature_vector("why in the world would boss", feature_list)
# print clf.predict(fv)
