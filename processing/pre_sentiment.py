from selenium import webdriver
import os
import pandas as pd
import numpy as np
import re
import logging
from docopt import docopt
from .load_channels import LoadChannel
from .postgresql_models import PreSentiment
from .utils import country_codes
from .utils import pre_sentiment_source_codes as source_codes
from .utils import today
from .utils import today_str
logger = logging.getLogger(__name__)


def save_counts(code_list, source, date, country):
    code_series = pd.Series(code_list)
    code_counts = code_series.value_counts()
    code_counts = code_counts.reset_index()
    code_counts.loc[:, 'source'] = source_codes[source]
    code_counts.loc[:, 'date'] = date
    code_counts.loc[:, 'country'] = country_codes[country]
    code_counts.columns = ['code', 'counts', 'source', 'date', 'country']
    if country == 'Australia':
        code_counts.loc[:, 'code'] = code_counts.loc[:, 'code'] + '.AX'
    return code_counts


def scrape_motley_fool():
    date_pattern = r'.*\| ([a-zA-Z]* \d{1,2}, \d{4})'
    asx_pattern = r'\(ASX: ([A-Z0-9]{3})\)'
    index_pattern = r'\(Index: (\^[A-Z0-9]{4})\)'
    driver = webdriver.PhantomJS()
    driver.set_window_size(1120, 550)
    driver.get('http://www.fool.com.au/recent-headlines/')
    link_href_list = []
    all_on_today = True
    while all_on_today:
        # collect all article items
        article_list = np.asarray(
            driver.find_elements_by_class_name('article-list')
        )
        # collect dates from below article title
        auth_date_list = [
            article.find_element_by_tag_name('h6').text
            for article in article_list
        ]
        date_list = pd.to_datetime(
            [re.search(date_pattern, date).groups()[0]
             for date in auth_date_list]
        )
        # check if article is from today
        is_today= date_list == today_str
        article_list = article_list[is_today]
        # get today's articles link
        link_href_list += [
            article.find_element_by_tag_name('a').get_attribute('href')
            for article in article_list
        ]
        # if all articles are for today, click to next page to check if more
        # today's article on the next page
        all_on_today = is_today.all()
        if all_on_today:
            # find the "next page" button and click
            driver.find_element_by_css_selector('a.next.pagination').click()
    code_list = []
    index_list = []
    for link in link_href_list:
        driver.get(link)
        art_text = driver.find_element_by_id('full_content').text
        code_list += list(set(re.findall(asx_pattern, art_text)))
        index_list += list(set(re.findall(index_pattern, art_text)))
    if len(code_list) > 0:
        code_counts = save_counts(
            code_list,
            'Motley Fool',
            today,
            'Australia'
        )
        code_load = LoadChannel(PreSentiment)
        code_load.dataframe = code_counts
        code_load.load_dataframe()
    if len(index_list) > 0:
        index_counts = save_counts(
            index_list,
            'Motley Fool',
            today,
            'Australia'
        )
        index_load = LoadChannel(PreSentiment)
        index_load.dataframe = index_counts
        index_load.load_dataframe()
    driver.quit()


def scrape_hotcopper_forum():
    npage = 1
    is_the_day = True
    code_list = []
    driver = webdriver.PhantomJS()
    driver.set_window_size(1120, 550)
    driver.get('http://hotcopper.com.au/discussions/asx---by-stock/')
    most_dis = driver.find_element_by_id('most-discussed-stocks').text
    most_dis = most_dis.split('\n')
    code_list = most_dis[0::3]
    count_list = most_dis[2::3]
    res_df = pd.DataFrame(
        {'code': code_list,
         'counts': count_list}
    )
    res_df.loc[:, 'source'] = source_codes['Hotcopper Forum']
    res_df.loc[:, 'date'] = today
    res_df.loc[:, 'country'] = country_codes['Australia']
    res_df.loc[:, 'code'] = res_df.loc[:, 'code'] + '.AX'
    driver.quit()
    load_res = LoadChannel(PreSentiment)
    load_res.dataframe = res_df
    load_res.load_dataframe()


def run_pre_sentiment():
    scrape_hotcopper_forum()
    scrape_motley_fool()
