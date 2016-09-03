import os
import datetime
import requests_cache
import logging
import pandas as pd
from docopt import docopt
from pandas.tseries.offsets import CustomBusinessDay
from processing.bulk_update import update_market
from processing.pre_sentiment import scrape_motley_fool
from processing.pre_sentiment import scrape_hotcopper_forum
from processing.pre_sentiment import run_pre_sentiment
from processing.update_lkp_tables import update_lkp_table
from processing.update_lkp_tables import update_all_lkp_tables
from processing.utils import today
from processing.utils import today_str
from processing.utils import ASXTradingCalendar
asx_trading_calendar = ASXTradingCalendar()
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
last_business_day = today - asx_dayoffset
cmd_doc = '''
    Usage:
      cli auto [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli manual <start> <end> [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli startover [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli update actions --source=SITE [--codes=CODES]
      cli update all-lkp-tables
      cli update --lkp-table=LKP-TABLE
      cli pre-sentiment [--source=SITE]

    Options:
      -h --help     Show this screen.
      -c --codes=CODES  ASX codes separated by comma. Mainly for debugging and testing purposes.
      --overwrite-existing-records  Overwrite on primary key.
      --clear-table-first  Clear the table before inserting.
      --source=SITE  Data source [default: yahoo].
      --country=COUNTRY  Country for the exchange [default: Australia].
      --price-type=PRICE-TYPE  Price type (share/sector) [default: share].
      --lkp-table=LKP-TABLE  Look-up table.
'''
logging.basicConfig(
    filename='project2m.log',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('')


class CommandCombinationError(Exception):
    pass


if __name__ == '__main__':
    arguments = docopt(cmd_doc)
    expire_after = datetime.timedelta(hours=3)
    session = requests_cache.CachedSession(
        cache_name='cache',
        backend='sqlite',
        expire_after=expire_after
    )
    if arguments['auto'] and today_str not in asx_trading_calendar.holidays():
        logger.info(
            'Auto bulk updating today\'s {price_type}s price.'.format(
                price_type=arguments['--price-type']
            )
        )
        if arguments['--price-type'] == 'sector':
            start_date = last_business_day
        else:
            start_date = today
        update_market(
            codes=arguments['--codes'],
            start_date=start_date,
            end_date=today,
            source=arguments['--source'],
            data_type=arguments['--price-type'],
            country=arguments['--country'],
            session=session,
        )
    elif arguments['manual']:
        logger.info(
            'Manual bulk updating {price_type}s between'.format(
                price_type=arguments['--price-type']
            ) +
            ' {start} and {end} (business days only).'.format(
                start=arguments['<start>'],
                end=arguments['<end>']
            )
        )
        update_market(
            codes=arguments['--codes'],
            start_date=pd.to_datetime(arguments['<start>']),
            end_date=pd.to_datetime(arguments['<end>']),
            source=arguments['--source'],
            data_type=arguments['--price-type'],
            country=arguments['--country'],
            session=session,
            overwrite_existing_records=True,
        )
    elif arguments['startover']:
        logger.info(
            '{price_type} prices start over.'.format(
                price_type=arguments['--price-type'].capitalize()
            )
        )
        update_market(
            codes=arguments['--codes'],
            start_date=pd.to_datetime('19900101'),
            end_date=today,
            source=arguments['--source'],
            data_type=arguments['--price_type'],
            country=arguments['--country'],
            session=session,
            clear_table_first=True
        )
    elif arguments['pre-sentiment']:
        if arguments['--source'] == 'MotleyFool':
            scrape_motley_fool()
        elif arguments['--source'] == 'HotCopperForum':
            scrape_hotcopper_forum()
        else:
            run_pre_sentiment()
    elif arguments['update']:
        if arguments['all-lkp-tables']:
            update_all_lkp_tables()
        elif arguments['--lkp-table']:
            update_lkp_table(arguments['--lkp-table'])
        elif arguments['actions'] and arguments['--source'] == 'yahoo-actions':
            update_market(
                codes=arguments['--codes'],
                start_date=pd.to_datetime('19900101'),
                end_date=today,
                source='yahoo-actions',
                data_type='action',
                country=arguments['--country'],
                session=session,
                overwrite_existing_records=True
            )
    else:
        raise CommandCombinationError
