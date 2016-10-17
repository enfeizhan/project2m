import logging
from datetime import timedelta
from datetime import datetime
from pandas.tseries.holiday import AbstractHolidayCalendar
from pandas.tseries.holiday import Holiday
from pandas.tseries.holiday import next_monday
from pandas.tseries.holiday import MO
from pandas.tseries.holiday import GoodFriday
from pandas.tseries.holiday import EasterMonday
from pandas.tseries.holiday import DateOffset
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from .settings import DBHOST
from .settings import DBUSER
from .settings import DBPASSWD
from .settings import DBNAME


logger = logging.getLogger(__name__)


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

today = datetime.now()
today_str = today.strftime('%Y%m%d')


def tablename_to_modelname(name):
    name_list = [word.capitalize() for word in name.split('_')]
    return ''.join(name_list)


def make_sa_conn_str(host, user, password, dbname, **options):
    '''
    postgresql+psycopg2://<username>:<password>@<hostname>/<dbname>[?<options>]
    '''
    conn_str_template = (
        'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
    )
    postgresql_details = {
        'host': host,
        'user': user,
        'password': password,
        'dbname': dbname
    }
    conn_str = conn_str_template.format(**postgresql_details)
    for ind, option in enumerate(options):
        if ind == 0:
            conn_str += '?'
        else:
            conn_str += '&'
        conn_str += '{}={}'.format(option, options[option])
    return conn_str

sqlalchemy_url = make_sa_conn_str(
    host=DBHOST,
    user=DBUSER,
    password=DBPASSWD,
    dbname=DBNAME,
)

engine = create_engine(sqlalchemy_url, echo=False)
Session = sessionmaker(bind=engine)


def getCommaSeparatedItemsQuoted(comma_separated_str):
    comma_separated_str = comma_separated_str.replace(',', "','")
    comma_separated_str = "'" + comma_separated_str + "'"
    return comma_separated_str


def getListQuoted(item_list):
    item_str = ','.join(item_list)
    item_str = getCommaSeparatedItemsQuoted(item_str)
    return item_str
