import os
import datetime
import requests_cache
import pandas as pd
import pandas_datareader.data as web
import logging
from pandas.tseries.offsets import CustomBusinessDay, DateOffset
from utils import ASXTradingCalendar
from utils import str2bool
from docopt import docopt
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
flag_col_name = 'is_last_11_day'
csv_back_days = 10

logging.basicConfig(
    filename='bulk_update.log',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

def update_company_shares(
        db_url,
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
    logging.info('There are {n} shares to update.'.format(n=len(codes_list)))
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
    # find the last day for the purpose of finding last 11 days
    last_day = res.Date.max()
    days_ago = last_day - csv_back_days * asx_dayoffset
    # find this year to find the file
    this_year = str(last_day.year)
    res = res.rename(columns={'minor': 'code'})
    logging.info('{n} shares updated.'.format(n=res.code.nunique()))
    try:
        # read this year's data
        yearly_dat = pd.read_csv(
            os.path.join(db_url, this_year) + 'price.csv'
        )
        # parsing the date
        yearly_dat.loc[:, 'Date'] = pd.to_datetime(yearly_dat.Date.values)
        # append data to the file of this year
        # res in the first place so the duplicate from old dataset will
        # be dropped
        res = pd.concat([res, yearly_dat])
        # drop duplicates sometimes
        res = res.drop_duplicates(['Date', 'code'])
        res = res.sort_values(['code', 'Date'])
        res = res.set_index(['Date', 'code'])
        # reset to all zeros
        res.loc[:, flag_col_name] = 0
        # set the last 11 days as one
        res = res.sort_index(level=[0, 1])
        res.loc[days_ago:last_day, flag_col_name] = 1
        res = res.sort_index(level=[1, 0])
        res.to_csv(
            os.path.join(db_url, this_year) + 'price.csv'
        )
    except OSError:
        # beginning of a year can't find the file
        # reset to all one
        res.loc[:, flag_col_name] = 1
        res.to_csv(
            os.path.join(db_url, this_year) + 'price.csv',
            index=False
        )
    

def update_sectors(
        db_url,
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
    logging.info('There are {n} sectors to update.'.format(n=len(codes_list)))

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
    # find the last day for the purpose of finding last 11 days
    last_day = res.Date.max()
    days_ago = last_day - csv_back_days * asx_dayoffset
    # find this year to find the file
    this_year = str(last_day.year)
    res = res.rename(columns={'minor': 'code'})
    logging.info('{n} sectors updated.'.format(n=res.code.nunique()))
    try:
        yearly_dat = pd.read_csv(
            os.path.join(db_url, this_year) + 'sector_price.csv'
        )
        yearly_dat.loc[:, 'Date'] = pd.to_datetime(yearly_dat.Date.values)
        # append data to the file of this year
        res = pd.concat([res, yearly_dat])
        # drop duplicates sometimes
        res = res.drop_duplicates(['Date', 'code'])
        res = res.sort_values(['code', 'Date'])
        res = res.set_index(['Date', 'code'])
        # reset to all zeros
        res.loc[:, flag_col_name] = 0
        # set the last 11 days as one
        res = res.sort_index(level=[0, 1])
        res.loc[days_ago:last_day, flag_col_name] = 1
        res = res.sort_index(level=[1, 0])
        res.to_csv(
            os.path.join(db_url, this_year) + 'sector_price.csv'
        )
    except OSError:
        # beginning of a year can't find the file
        # reset to all one
        res.loc[:, flag_col_name] = 1
        res.to_csv(
            os.path.join(db_url, this_year) + 'sector_price.csv',
            index=False
        )
