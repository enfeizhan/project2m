import os
import datetime
import requests_cache
import logging
import pandas as pd
from docopt import docopt
from pandas.tseries.offsets import CustomBusinessDay
from processing.bulk_update import update_market
from processing.pre_sentiment import run_pre_sentiment
from processing.update_lkp_tables import update_lkp_table
from processing.update_lkp_tables import update_all_lkp_tables
from processing.utils import today
from processing.utils import ASXTradingCalendar
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
last_business_day = today - asx_dayoffset
cmd_doc = '''
    Usage:
      cli auto [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli manual <start> <end> [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli startover [--price-type=PRICE-TYPE] [--source=SITE] [--country=COUNTRY] [--codes=CODES]
      cli pre-sentiment
      cli update all-lkp-tables
      cli update --lkp-table=LKP-TABLE

    Options:
      -h --help     Show this screen.
      -c --codes=CODES  ASX codes separated by comma. Mainly for debugging and testing purposes.
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
    if arguments['auto']:
        logger.info(
            'Auto bulk updating today\'s {price_type}s price.'.format(
                price_type=arguments['--price-type']
            )
        )
        update_market(
            codes=arguments['--codes'],
            start_date=today,
            end_date=today,
            source=arguments['--source'],
            price_type=arguments['--price-type'],
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
            price_type=arguments['--price-type'],
            country=arguments['--country'],
            session=session,
            overwrite_existing_records=True
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
            price_type=arguments['--price_type'],
            country=arguments['--country'],
            session=session,
            clear_table_first=True
        )
    elif arguments['pre-sentiment']:
        run_pre_sentiment()
    elif arguments['update']:
        if arguments['all-lkp-tables']:
            update_all_lkp_tables()
        elif arguments['--lkp-table']:
            update_lkp_table(arguments['--lkp-table'])
    else:
        raise CommandCombinationError
