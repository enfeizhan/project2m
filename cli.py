import os
import datetime
import requests_cache
import logging
from docopt import docopt
from processing.bulk_update import update_company_shares
from processing.bulk_update import sectors
from processing.pre_sentiment import run_pre_sentiment
cmd_doc = '''
    Usage:
      cli share auto [--share-back-days=DAYS] [--source=SITE] [--codes=CODES]
      cli share manual <start> <end> [--source=SITE] [--codes=CODES]
      cli sector auto [--sector-back-days=DAYS] [--source=SITE] [--codes=CODES]
      cli sector manual <start> <end> [--source=SITE] [--codes=CODES]
      cli pre-sentiment

    Options:
      -h --help     Show this screen.
      -c --codes=CODES  ASX codes separated by comma. Mainly for debugging and testing purposes.
      --share-back-days=DAYS  Days to look backward for shares [default: 0].
      --sector-back-days=DAYS  Days to look backward for sectors [default: 1].
      --source=SITE  Data source [default: yahoo].
'''

logging.basicConfig(
    filename='project2m.log',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger('')



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
            logger.info(
                'Auto bulk updating shares back ' +
                '{n} business day(s) from now.'.format(
                    n=arguments['--share-back-days']
                )
            )
            update_company_shares(
                codes=arguments['--codes'],
                back_days=int(arguments['--share-back-days']),
                source=arguments['--source'],
                session=session
            )
        elif arguments['manual']:
            logger.info(
                'Manual bulk updating shares between' +
                ' {start} and {end} (business days only).'.format(
                    start=arguments['<start>'],
                    end=arguments['<end>']
                )
            )
            update_company_shares(
                codes=arguments['--codes'],
                start_date=arguments['<start>'],
                end_date=arguments['<end>'],
                source=arguments['--source'],
                session=session
            )
        else:
            raise SystemError('Wrong command combination.')
    elif arguments['sector']:
        if arguments['auto']:
            logger.info(
                'Auto bulk updating sectors back ' +
                '{n} business day(s) from now'.format(
                    n=arguments['--sector-back-days']
                )
            )
            update_sectors(
                codes=arguments['--codes'],
                back_days=int(arguments['--sector-back-days']),
                source=arguments['--source'],
                session=session
            )
        elif arguments['manual']:
            logger.info(
                'Manual bulk updating sectors between' +
                ' {start} and {end} (business days only).'.format(
                    start=arguments['<start>'],
                    end=arguments['<end>']
                )
            )
            update_sectors(
                codes=arguments['--codes'],
                start_date=arguments['<start>'],
                end_date=arguments['<end>'],
                source=arguments['--source'],
                session=session
            )
        else:
            raise SystemError('Wrong command combination.')
    elif arguments['pre-sentiment']:
        run_pre_sentiment()
