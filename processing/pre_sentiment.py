import pdb
from selenium import webdriver
import os
import pandas as pd
import numpy as np
import re
import logging
from docopt import docopt
from .postgresql_models import PreSentiment
from .utils import LoadChannel
from .utils import country_codes
from .utils import pre_sentiment_source_codes as source_codes
from .utils import today
from .utils import today_str
from .utils import Extract
from .utils import Transform
from .utils import Load
from .utils import WebScraper

logger = logging.getLogger(__name__)
load_channel = LoadChannel(PreSentiment)


class PreSentimentExtract(Extract):
    def download_data(self, scrape_func, *args):
        self.scrape_res = scrape_func(*args)
        self.df = self.scrape_res


class PreSentimentTransform(Transform):
    def count_codes(self, code_list_name):
        code_list = self.scrape_res[code_list_name]
        self.df = pd.DataFrame({})
        if len(code_list) > 0:
            code_series = pd.Series(code_list)
            code_series = code_series.value_counts()
            code_series.name = 'counts'
            code_series.index.name = 'code'
            self.df = code_series.reset_index()

    def attach_suffix(self, col, code_suffix):
        self.df.loc[:, col] = self.df.loc[:, col] + code_suffix


class PreSentimentETL(PreSentimentExtract, PreSentimentTransform, Load):
    pass


def motley_fool_scrape_func():
    date_pattern = r'.*\| ([a-zA-Z]* \d{1,2}, \d{4})'
    asx_pattern = r'\(ASX: ([A-Z0-9]{3})\)'
    index_pattern = r'\(Index: (\^[A-Z0-9]{4})\)'
    web_scraper = WebScraper(
        url='http://www.fool.com.au/recent-headlines/'
    )
    web_scraper.search_article_urls(
        article_find_method='find_elements_by_class_name',
        article_find_identifier='article-list',
        date_find_method='find_element_by_tag_name',
        date_find_identifier='h6',
        date_pattern=date_pattern,
        next_button_find_method='find_element_by_css_selector',
        next_button_find_identifier='a.next.pagination'
    )
    web_scraper.find_all_pattern(
        text_find_method='find_element_by_id',
        text_find_identifier='full_content',
        code_list=asx_pattern,
        index_list=index_pattern
    )
    web_scraper.quit()
    return {
        'code_list': web_scraper.code_list,
        'index_list': web_scraper.index_list
    }


def scrape_motley_fool():
    motley_fool = PreSentimentETL()
    motley_fool.download_data(motley_fool_scrape_func)
    col_codes = {
        'source': source_codes['Motley Fool'],
        'date': pd.datetime.today().strftime('%Y%m%d'),
        'country': country_codes['Australia']
    }
    motley_fool.count_codes('code_list')
    if not motley_fool.df.empty:
        motley_fool.attach_infos(col_codes)
        motley_fool.attach_suffix('code', '.AX')
        motley_fool.load_to_db(load_channel, overwrite_existing_records=True)
    motley_fool.count_codes('index_list')
    if not motley_fool.df.empty:
        motley_fool.attach_infos(col_codes)
        motley_fool.load_to_db(load_channel, overwrite_existing_records=True)


def hotcopper_forum_scrape_func():
    web_scraper = WebScraper(
        url='http://hotcopper.com.au/discussions/asx---by-stock/'
    )
    most_dis = web_scraper.find_elements(
        'find_element_by_id',
        'most-discussed-stocks'
    ).text
    web_scraper.quit()
    most_dis = most_dis.split('\n')
    code_list = most_dis[0::3]
    count_list = most_dis[2::3]
    return pd.DataFrame({'code': code_list, 'counts': count_list})


def scrape_hotcopper_forum():
    hotcopper_forum = PreSentimentETL()
    hotcopper_forum.download_data(hotcopper_forum_scrape_func)
    col_codes = {
        'source': source_codes['Hotcopper Forum'],
        'date': pd.datetime.today().strftime('%Y%m%d'),
        'country': country_codes['Australia']
    }
    if not hotcopper_forum.df.empty:
        hotcopper_forum.attach_infos(col_codes)
        hotcopper_forum.attach_suffix('code', '.AX')
        hotcopper_forum.load_to_db(
            load_channel,
            overwrite_existing_records=True
        )


def run_pre_sentiment():
    scrape_hotcopper_forum()
    scrape_motley_fool()
