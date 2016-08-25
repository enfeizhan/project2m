import os
import datetime
import requests_cache
import logging
import pandas as pd
from docopt import docopt
from pandas.tseries.offsets import CustomBusinessDay
from processing.bulk_update import update_market
from processing.pre_sentiment import run_pre_sentiment
from processing.utils import today
from processing.utils import ASXTradingCalendar
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
last_business_day = today - asx_dayoffset
cmd_doc = '''
    Usage:
      cli share auto [--source=SITE] [--codes=CODES]
      cli share manual <start> <end> [--source=SITE] [--codes=CODES]
      cli share startover [--source=SITE] [--codes=CODES]
      cli sector auto [--source=SITE] [--codes=CODES]
      cli sector manual <start> <end> [--source=SITE] [--codes=CODES]
      cli sector startover [--source=SITE] [--codes=CODES]
      cli pre-sentiment

    Options:
      -h --help     Show this screen.
      -c --codes=CODES  ASX codes separated by comma. Mainly for debugging and testing purposes.
      --source=SITE  Data source [default: yahoo].
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
    if arguments['share']:
        if arguments['auto']:
            logger.info('Auto bulk updating today\'s shares price.')
            update_market(
                codes=arguments['--codes'],
                start_date=today,
                end_date=today,
                source=arguments['--source'],
                price_type='share',
                session=session,
            )
        elif arguments['manual']:
            logger.info(
                'Manual bulk updating shares between' +
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
                price_type='share',
                session=session,
                overwrite_existing_records=True
            )
        elif arguments['startover']:
            logger.info('Share prices start over.')
            update_market(
                codes=arguments['--codes'],
                start_date=pd.to_datetime('19900101'),
                end_date=today,
                source=arguments['--source'],
                price_type='share',
                session=session,
                clear_table_first=True
            )
        else:
            raise CommandCombinationError
    elif arguments['sector']:
        if arguments['auto']:
            logger.info('Auto bulk updating last business day sectors price.')
            update_market(
                codes=arguments['--codes'],
                start_date=last_business_day,
                end_date=today,
                source=arguments['--source'],
                price_type='sector',
                session=session,
            )
        elif arguments['manual']:
            logger.info(
                'Manual bulk updating sectors between' +
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
                price_type='sector',
                session=session,
                overwrite_existing_records=True
            )
        elif arguments['startover']:
            logger.info('Sector prices start over.')
            update_market(
                codes=arguments['--codes'],
                start_date=pd.to_datetime('19900101'),
                end_date=today,
                source=arguments['--source'],
                price_type='sector',
                session=session,
                clear_table_first=True
            )
        else:
            raise CommandCombinationError
    elif arguments['pre-sentiment']:
        run_pre_sentiment()
