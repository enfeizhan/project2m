import os
import time
import datetime
import requests_cache
import logging
import pandas as pd
import pandas_datareader.data as web
from docopt import docopt
from pandas.tseries.offsets import CDay, DateOffset
from utils import ASXTradingCalendar
today = pd.datetime.now()
start = pd.datetime(1995, 1, 1)
asx_dayoffset = CDay(calendar=ASXTradingCalendar())
logging.basicConfig(
    filename='full_history.log',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
cmd_doc = '''
    Usage:
        sep_full_history share [--asx | --codes=c] [--share-url=du] [--source=source] [--silent=s] [--start-id=id]
        sep_full_history sector [--xlsx | --codes=c] [--sector-url=du] [--source=source] [--silent=s] [--start-id=id]

    Options:
        --asx  From ASX homepage.
        --xlsx  From xlsx file.
        --codes=c  Manual input codes, separated by commas.
        --sector-url=du  Where to store sector data [default: ~/Dropbox/Project2M/ASXYearlySectorConsolidation]
        --share-url=du  Where to store share data [default: ~/Dropbox/Project2M/ASXCompanyHistory/]
        --source=source  Where to download the data [default: yahoo].
        --silent=s  Whether output details during progress [default: True].
        --start-id=id  Start ID if start again [default: 1].
'''


def get_full_history(
        company_codes,
        db_url,
        source,
        silent,
        start_id,
        session
        ):
    # get full history for each company one by one
    # and install in separate files
    success = 0
    ncodes = len(company_codes)
    for ncode, code in enumerate(company_codes):
        if ncode < start_id - 1:
            continue
        time.sleep(.1)
        if not silent:
            print(
                '{ncode}/{ncodes}. {code} ...'.format(
                    ncode=ncode+1,
                    ncodes=ncodes,
                    code=code)
                )
        try:
            dataframe = web.DataReader(
                code,
                source,
                start,
                today,
                session=session)
        except OSError:
            logging.info('{} failed!'.format(code))
            print('{} failed!'.format(code))
            continue
        try:
            dataframe.loc[:, 'code'] = code
        except ValueError:
            logging.info('{} failed with empty data!'.format(code))
            print('{} empty data!'.format(code))
            continue
        logging.info('{} downloaded!'.format(code))
        dataframe.to_csv(os.path.join(db_url, code)+'.csv')
        success += 1
    return success


if __name__ == '__main__':
    arguments = docopt(cmd_doc)
    expire_after = datetime.timedelta(hours=3)
    session = requests_cache.CachedSession(
        cache_name='cache',
        backend='sqlite',
        expire_after=expire_after
    )
    if arguments['share']:
        if arguments['--asx']:
            asx = pd.read_csv(
                'http://www.asx.com.au/asx/research/ASXListedCompanies.csv',
                skiprows=1
            )
            codes_list = (asx.loc[:, 'ASX code'] + '.AX').tolist()
            ncodes = len(codes_list)
            logging.info('There are {} shares to download.'.format(ncodes))
            success = get_full_history(
                codes_list,
                arguments['--share-url'],
                arguments['--source'],
                bool(arguments['--silent']),
                int(arguments['--start-id']),
                session
            )
            logging.info(
                'There are {} shares downloaded '.format(success) +
                'successfully. Failed {}.\n'.format(ncodes-success)
            )
        elif arguments['--codes']:
            codes_list = arguments['--codes'].split(',')
            ncodes = len(codes_list)
            logging.info('There are {} shares to download.'.format(ncodes))
            success = get_full_history(
                codes_list,
                arguments['--share-url'],
                arguments['--source'],
                bool(arguments['--silent']),
                int(arguments['--start-id']),
                session
            )
            logging.info(
                'There are {} sectors downloaded '.format(success) +
                'successfully. Failed {}.\n'.format(ncodes-success)
            )
    elif arguments['sector']:
        if arguments['--xlsx']:
            asx = pd.read_excel('sector_codes.xlsx')
            codes_list = asx.loc[:, 'sector_code'].tolist()
            ncodes = len(codes_list)
            logging.info('There are {} sectors to download.'.format(ncodes))
            success = get_full_history(
                codes_list,
                arguments['--sector-url'],
                arguments['--source'],
                bool(arguments['--silent']),
                int(arguments['--start-id']),
                session
            )
            logging.info(
                'There are {} sectors downloaded '.format(success) +
                'successfully. Failed {}.\n'.format(ncodes-success)
            )
        elif arguments['--codes']:
            codes_list = arguments['--codes'].split(',')
            ncodes = len(codes_list)
            logging.info('There are {} shares to download.'.format(ncodes))
            success = get_full_history(
                arguments['--codes'].split(','),
                arguments['--sector-url'],
                arguments['--source'],
                bool(arguments['--silent']),
                int(arguments['--start-id']),
                session
            )
            logging.info(
                'There are {} sectors downloaded '.format(success) +
                'successfully. Failed {}.\n'.format(ncodes-success)
            )
