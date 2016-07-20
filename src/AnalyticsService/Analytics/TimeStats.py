import json
import logging

import numpy
import pandas as pd
from AnalyticsService.Analytics.Analytics import Analytics
from AnalyticsService.TwitterObj import Status


class TimeDistribution(Analytics):
    _logger = logging.getLogger(__name__)

    _options = {
        "Minute": "min",
        "Hour": "H",
        "Day": "D",
        "Week": "W",
        "Month": "M"
    }

    __type_name = "Time_Distribution"
    __arguments = [{"name":"timeInterval","prettyName":"Time interval","type": "enum", "options":_options.keys(),
                    "default":"Hour"}]

    @classmethod
    def get_args(cls):
        return cls.__arguments + super(TimeDistribution, cls).get_args()


    @classmethod
    def get_type(cls):
        return cls.__type_name

    ####################################################################################################################

    @classmethod
    def get(cls, analytics_meta):

        gridfs, db_col, args, schema_id = cls.setup(analytics_meta)

        time_interval = args["timeInterval"]


        try:
            time_quantum = cls._options[time_interval]
        except KeyError:
            cls._logger.exception("Wrong time quantum given")
            return False

        dates = [Status(c, schema_id).get_created_at()
                 for c in db_col.find({}, {Status.SCHEMA_MAP[schema_id]["created_at"]: 1})]

        index = pd.DatetimeIndex(dates)

        df = pd.DataFrame(numpy.ones(len(index)), index=index, columns=["Count"])
        gb = df.groupby(pd.TimeGrouper(freq=time_quantum)).count()

        data = gb.to_json(date_format='iso')

        cls.export_json(analytics_meta, json.dumps(data), gridfs)

        return True