from pymongo import MongoClient
import sys
reload(sys)  # just to be sure
sys.setdefaultencoding('utf-8')
db = MongoClient().get_database("DATA").get_collection("Trump_Clinton_Saunders_old")


with open("out.dat","w") as f:
    for c in db.find().limit(1000):
        f.write(c["text"].replace("\n"," ") + "\n")