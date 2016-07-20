import random

import matplotlib.pyplot as plt


class User(object):
    POSITIVE = 40
    NEGATIVE = -40

    def __init__(self, scores):
        self.scores = scores
        self.time_constant = 5.0
        self.partisan = None
        self.score = 0.0
        self.scaler = 1.0

    def said_these(self, hastags, time_step):

        score = 0
        for h in hastags:
            score += self.scores.get(h,0.0)

        if score > 0:
            self.add_positive(score, time_step)
        elif score < 0:
            self.add_negative(score, time_step)

        return self.score


    def add_positive(self, score, time_step):

        if self.partisan is None or self.partisan == "Negative":
            if self.partisan == "Negative":
                self.scaler = 2.0
            self.partisan = "Positive"

        self.score += self.scaler * score * (self.POSITIVE - self.score) / 100

    def add_negative(self, score, time_step):

        if self.partisan is None or self.partisan == "Positive":
            if self.partisan == "Positive":
                self.scaler = 2.0
            self.partisan = "Negative"

        self.score += self.scaler * abs(score) * (self.NEGATIVE - self.score) / 100

leave = {
    "no2eu": 2,
    "notoeu": 2,
    "betteroffout": 2,
    "voteout": 2,
    "eureform": 2,
    "britainout": 2,
    "leaveeu": 2,
    "voteleave": 2,
    "beleave": 2,
    "loveeuropeleaveeu": 2,
}

remain = {
    "yes2eu": -2,
    "yestoeu": -2,
    "betteroffin": -2,
    "votein": -2,
    "ukineu": -2,
    "bremain": -2,
    "strongerin": -2,
    "leadnotleave": -2,
    "voteremain": -2,
}

def get_tags(side):
    tags = []
    numnber_of_tags = random.randint(0,4)
    for i in xrange(numnber_of_tags):
        tags.append(random.choice(side.keys()))
    return tags


alex = User(dict(leave.items() + remain.items()))
scores = []
pos = []
neg = []
for i in xrange(100):
    tags = get_tags(leave)
    print tags
    s =  alex.said_these(tags, i)
    scores.append(s)

for i in xrange(100,110):
    tags = get_tags(remain)
    print tags
    s =  alex.said_these(tags, i)
    scores.append(s)

for i in xrange(110,120):
    tags = get_tags(leave)
    print tags
    s =  alex.said_these(tags, i)
    scores.append(s)

for i in xrange(120,300):
    tags = get_tags(remain)
    print tags
    s =  alex.said_these(tags, i)
    scores.append(s)



plt.plot(range(len(scores)), scores)

plt.show()