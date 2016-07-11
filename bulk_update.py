import datetime
import requests_cache
import pandas as pd
import pandas_datareader.data as web
import logging
from pandas.tseries.offsets import CustomBusinessDay, DateOffset
from utils import ASXTradingCalendar
from docopt import docopt
cmd_doc = '''
    Usage:
      bulk_update share auto [--share-back-days=days] [--business=b] [--share-url=url] [--source=source] [--codes=codes]
      bulk_update share manual <start> <end> [--business=b] [--share-url=url] [--source=source] [--codes=codes]
      bulk_update sector auto [--sector-back-days=days] [--business=b] [--sector-url=url] [--source=source] [--codes=codes]
      bulk_update sector manual <start> <end> [--business=b] [--sector-url=url] [--source=source] [--codes=codes]

    Options:
      -h --help     Show this screen.
      -c --codes=codes  ASX codes separated by comma. Mainly for debugging and testing purposes.
      --share-back-days=days  Days to look backward for shares [default: 0].
      --sector-back-days=days  Days to look backward for sectors [default: 1].
      --business=b  Only look at business days [default: True].
      --share-url=url  URL to find share file [default: ~/Dropbox/Project2M/ASXYearlyCompanyConsolidation/].
      --sector-url=url  URL to find sector file [default: ~/Dropbox/Project2M/ASXYearlySectorConsolidation/].
      --source=source  Data source [default: yahoo].
'''
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
        business=None,
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
        if business:
            start_datetime = end_datetime - back_days * asx_dayoffset
        else:
            start_datetime = end_datetime - back_days * DateOffset()
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
        yearly_dat = pd.read_csv(db_url+this_year+'price.csv')
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
        res.to_csv(db_url+this_year+'price.csv')
    except OSError:
        # beginning of a year can't find the file
        # reset to all one
        res.loc[:, flag_col_name] = 1
        res.to_csv(db_url+this_year+'price.csv', index=False)
    

def update_sectors(
        db_url,
        codes=None,
        back_days=None,
        start_date=None,
        end_date=None,
        source=None,
        business=None,
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
        if business:
            start_datetime = end_datetime - back_days * asx_dayoffset
        else:
            start_datetime = end_datetime - back_days * DateOffset()
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
        yearly_dat = pd.read_csv(db_url+this_year+'sector_price.csv')
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
        res.to_csv(db_url+this_year+'sector_price.csv')
    except OSError:
        # beginning of a year can't find the file
        # reset to all one
        res.loc[:, flag_col_name] = 1
        res.to_csv(db_url+this_year+'sector_price.csv', index=False)


if __name__ == '__main__':
    arguments = docopt(cmd_doc)
    expire_after = datetime.timedelta(hours=3)
    session = requests_cache.CachedSession(
        cache_name='cache',
        backend='sqlite',
        expire_after=expire_after
    )
    if arguments['share']:
        if arguments['auto']:
            if arguments['--business'] == 'True':
                logging.info(
                    'Auto bulk updating shares back ' +
                    '{n} business day(s) from now.'.format(
                        n=arguments['--share-back-days']
                    )
                )
            else:
                logging.info(
                    'Auto bulk updating shares back ' +
                    '{n} day(s) from now.'.format(
                        n=arguments['--share-back-days']
                    )
                )
            update_company_shares(
                db_url=arguments['--share-url'],
                codes=arguments['--codes'],
                back_days=int(arguments['--share-back-days']),
                source=arguments['--source'],
                business=bool(arguments['--business']),
                session=session
            )
        elif arguments['manual']:
            if arguments['--business'] == 'True':
                logging.info(
                    'Manual bulk updating shares between' +
                    ' {start} and {end} (business days only).'.format(
                        start=arguments['<start>'],
                        end=arguments['<end>']
                    )
                )
            else:
                logging.info(
                    'Manual bulk updating shares between' +
                    ' {start} and {end}.'.format(
                        start=arguments['<start>'],
                        end=arguments['<end>']
                    )
                )
            update_company_shares(
                db_url=arguments['--share-url'],
                codes=arguments['--codes'],
                start_date=arguments['<start>'],
                end_date=arguments['<end>'],
                source=arguments['--source'],
                business=bool(arguments['--business']),
                session=session
            )
        else:
            raise SystemError('Wrong command combination.')
    elif arguments['sector']:
        if arguments['auto']:
            if arguments['--business'] == 'True':
                logging.info(
                    'Auto bulk updating sectors back ' +
                    '{n} business day(s) from now'.format(
                        n=arguments['--sector-back-days']
                    )
                )
            else:
                logging.info(
                    'Auto bulk updating sectors back ' +
                    '{n} day(s) from now.'.format(
                        n=arguments['--sector-back-days']
                    )
                )
            update_sectors(
                db_url=arguments['--sector-url'],
                codes=arguments['--codes'],
                back_days=int(arguments['--sector-back-days']),
                source=arguments['--source'],
                business=arguments['--business'],
                session=session
            )
        elif arguments['manual']:
            if arguments['--business'] == 'True':
                logging.info(
                    'Manual bulk updating sectors between' +
                    ' {start} and {end} (business days only).'.format(
                        start=arguments['<start>'],
                        end=arguments['<end>']
                    )
                )
            else:
                logging.info(
                    'Manual bulk updating sectors between' +
                    ' {start} and {end}.'.format(
                        start=arguments['<start>'],
                        end=arguments['<end>']
                    )
                )
            update_sectors(
                db_url=arguments['--sector-url'],
                codes=arguments['--codes'],
                start_date=arguments['<start>'],
                end_date=arguments['<end>'],
                source=arguments['--source'],
                business=arguments['--business'],
                session=session
            )
        else:
            raise SystemError('Wrong command combination.')
    else:
        raise SystemError('Share or sector?')
