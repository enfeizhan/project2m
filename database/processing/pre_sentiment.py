import pandas as pd
import logging
from .postgresql_models import PreSentiment
from .etl_tools import LoadChannel
from .etl_tools import CodesCounter
from .etl_tools import TablesDownloader
from .utils import today

logger = logging.getLogger(__name__)
load_channel = LoadChannel(PreSentiment)


def scrape_motley_fool():
    date_pattern = r'.*\| ([a-zA-Z]* \d{1,2}, \d{4})'
    share_pattern = r'\(ASX: ([A-Z0-9]{3})\)'
    index_pattern = r'\(Index: (\^[A-Z0-9]{4})\)'
    motley_fool = CodesCounter(url='http://www.fool.com.au/recent-headlines/')
    motley_fool.download_data(
        article_by='class_name',
        article_identifier='article-list',
        date_by='tag_name',
        date_identifier='h6',
        date_pattern=date_pattern,
        next_button_by='css_selector',
        next_button_identifier='a.next.pagination',
        text_by='id',
        text_identifier='full_content',
        code_list=share_pattern,
        index_list=index_pattern
    )
    col_codes = {
        'source': 'Motley Fool',
        'date': today.date(),
        'country': 'Australia'
    }
    motley_fool.count_codes('code_list')
    if not motley_fool.df.empty:
        motley_fool.attach_infos(col_codes)
        motley_fool.attach_suffix('code', '.AX')
        motley_fool.load_to_db(
            load_channel,
            True,
            False,
            *['source', 'country']
        )
    motley_fool.count_codes('index_list')
    if not motley_fool.df.empty:
        motley_fool.attach_infos(col_codes)
        motley_fool.load_to_db(
            load_channel,
            True,
            False,
            *['source', 'country']
        )
    motley_fool.quit()
    logger.info('Counted codes from Motley Fool.')


def scrape_hotcopper_forum():
    hotcopper_forum = TablesDownloader(
        url='http://hotcopper.com.au/discussions/asx---by-stock/'
    )
    hotcopper_forum.download_data(
        table_by='id',
        table_identifier='most-discussed-stocks',
    )

    def read_table(table_str, columns):
        table_elements = table_str.split('\n')
        code_list = table_elements[0::3]
        count_list = table_elements[2::3]
        return pd.DataFrame({columns[0]: code_list, columns[1]: count_list})

    hotcopper_forum.to_dataframe(
        read_table=read_table,
        columns=['code', 'counts']
    )
    col_codes = {
        'source': 'Hotcopper Forum',
        'date': pd.datetime.today().strftime('%Y%m%d'),
        'country': 'Australia'
    }
    if not hotcopper_forum.df.empty:
        hotcopper_forum.attach_infos(col_codes)
        hotcopper_forum.attach_suffix('code', '.AX')
        hotcopper_forum.load_to_db(
            load_channel,
            True,
            False,
            *('source', 'country')
        )
    logger.info('Scraped hot topic counts from Hotcopper Forum.')


def run_pre_sentiment():
    scrape_motley_fool()
    scrape_hotcopper_forum()
