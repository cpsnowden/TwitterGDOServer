# from sklearn import svm
# X = [[0, 0], [1, 1]]
# y = [0, 1]
# clf = svm.SVC()
# clf.fit(X, y)
import sys
reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')
import progressbar


# # print clf.predict([[0,0]])
# # print clf.decision_function([[1,0]])
import pymongo

dbcol = pymongo.MongoClient().get_database("DATA").get_collection("Brexit_old")
c = dbcol.find({"retweetedStatus":{"$exists":False},"lang":"en"})
    # .limit(1000)

leave = {"no2eu", "notoeu", "betteroffout", "voteout", "eureform", "britainout", "leaveeu", "voteleave", "beleave",
         "loveeuropeleaveeu","takecontrol","leave"}

remain = {
    "yes2eu",
    "yestoeu",
    "betteroffin",
    "votein",
    "ukineu",
    "bremain",
    "strongerin",
    "leadnotleave",
    "voteremain",
}

# leave = {"voteleave"}
# remain = {"voteremain"}

from AnalyticsService.TwitterObj import Status
import random
# print c.count()
leave_c = 0
remain_c = 0
t= progressbar.FormatCustomText("(Leave %(le)d, Remain %(re)d)", dict(le=0,re=0))

widgets = [t,":::", progressbar.Counter(),":::", progressbar.RotatingMarker()]
bar = progressbar.ProgressBar(widgets=widgets)

with open("training_data.txt","w") as f:

    for s in bar(c):
        if random.random() > 1.0/5:
            continue

        status = Status(s, "T4J")
        hastags = set([s.lower() for s in status.get_hashtags()])

        if hastags.intersection(leave) and not hastags.intersection(remain):
            label = "leave"
            leave_c +=1
            t.update_mapping(le=leave_c)
        elif not hastags.intersection(leave) and hastags.intersection(remain):
            label = "remain"
            remain_c +=1
            t.update_mapping(re=remain_c)
        else:
            pass
            continue
            # print status.get_hashtags()

        f.write("|"+label+"|,|"+status.get_text().replace("\n"," ")+"|\n")



