class CUser(object):
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
                score += self.scores.get(h.lower(), 0.0)

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