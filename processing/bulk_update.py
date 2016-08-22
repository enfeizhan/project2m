import os
import datetime
import requests_cache
import pandas as pd
import pandas_datareader.data as web
import logging
from pandas.tseries.offsets import CustomBusinessDay, DateOffset
from docopt import docopt
from .utils import ASXTradingCalendar
from .utils import str2bool
from .datasets import SharePriceLoad
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
column_name_change ={'minor': 'asx_code',
         'Date': 'date',
         'Open': 'open_price',
         'High': 'high_price',
         'Low': 'low_price',
         'Close': 'close_price',
         'Volume': 'volume',
         'Adj Close': 'adj_close_price',
}
logger = logging.getLogger(__name__)


def update_company_shares(
        codes=None,
        back_days=None,
        start_date=None,
        end_date=None,
        source=None,
        session=None,
        ):
    if codes is None:
        # read the full company list from asx home page
        # if not found, asx has changed the url
        try:
            asx = pd.read_csv(
                'http://www.asx.com.au/asx/research/ASXListedCompanies.csv',
                skiprows=1
            )
            codes_list = (asx.loc[:, 'ASX code'] + '.AX').tolist()
        except:
            raise SystemError('Share list not found on ASX!')
    else:
        # when given codes, just split them to get a list
        codes_list = codes.split(',')
    logger.info('There are {n} shares to update.'.format(n=len(codes_list)))
    if back_days is not None:
        # given back_days, assume end_date is today
        end_datetime = pd.datetime.now()
        # get the start date
        start_datetime = end_datetime - back_days * asx_dayoffset
    elif start_date and end_date:
        # not given back_days then need to know the start_date and end_date
        start_datetime= pd.to_datetime(start_date)
        end_datetime = pd.to_datetime(end_date)
    else:
        raise SystemError(
            'Needs days backwards or start date and end date'
        )
    # get data through pandas
    res = web.DataReader(
        codes_list,
        source,
        start_datetime,
        end_datetime,
        session=session
    ).to_frame()
    res = res.reset_index()
    res = res.rename(columns=column_name_change)
    res.loc[:, 'is_sector'] = False
    res.loc[:, 'create_date'] = pd.datetime.today()
    logger.info('{n} shares updated.'.format(n=res.asx_code.nunique()))
    to_load = SharePriceLoad.process_dataframe(res)
    to_load.load_dataframe()
    

def update_sectors(
        codes=None,
        back_days=None,
        start_date=None,
        end_date=None,
        source=None,
        session=None,
        ):
    if codes is None:
        asx = pd.read_excel('sector_codes.xlsx')
        codes_list = asx.sector_code.tolist()
    else:
        # when given codes, just split them to get a list
        codes_list = codes.split(',')
    logger.info('There are {n} sectors to update.'.format(n=len(codes_list)))

    if back_days is not None:
        # given back_days, assume end_date is today
        end_datetime = pd.datetime.now()
        # get the start date
        start_datetime = end_datetime - back_days * asx_dayoffset
    elif start_date and end_date:
        # not given back_days then need to know the start_date and end_date
        start_datetime= pd.to_datetime(start_date)
        end_datetime = pd.to_datetime(end_date)
    else:
        raise SystemError(
            'Needs days backwards or start date and end date'
        )
    # get data through pandas
    res = web.DataReader(
        codes_list,
        source,
        start_datetime,
        end_datetime,
        session=session
    ).to_frame()
    res = res.reset_index()
    res = res.rename(columns=column_name_change)
    res.loc[:, 'is_sector'] = True
    res.loc[:, 'create_date'] = pd.datetime.today()
    logger.info('{n} sectors updated.'.format(n=res.asx_code.nunique()))
    to_load = SharePriceLoad.process_dataframe(res)
    to_load.load_dataframe()
