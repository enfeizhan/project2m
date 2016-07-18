from datetime import timedelta
from pandas.tseries.holiday import AbstractHolidayCalendar
from pandas.tseries.holiday import Holiday
from pandas.tseries.holiday import next_monday
from pandas.tseries.holiday import MO
from pandas.tseries.holiday import GoodFriday
from pandas.tseries.holiday import EasterMonday
from pandas.tseries.holiday import DateOffset


def my_next_monday_or_tuesday(dt):
    dow = dt.weekday()
    if dow == 5 or dow == 6:
        return dt + timedelta(2)
    return dt


class ASXTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Year\'s Day', month=1, day=1, observance=next_monday),
        Holiday('Australia Day', month=1, day=26, observance=next_monday),
        GoodFriday,
        EasterMonday,
        Holiday('ANZAC Day', month=4, day=25),
        Holiday('Queen\'s Birthday', month=6, day=1,
                offset=DateOffset(weekday=MO(2))),
        Holiday('Christmas Day', month=12, day=25,
                observance=my_next_monday_or_tuesday),
        Holiday('Boxing Day', month=12, day=26,
                observance=my_next_monday_or_tuesday),
    ]


def str2bool(string):
    return string == 'True'
