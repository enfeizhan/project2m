from selenium import webdriver
import os
import pandas as pd
import numpy as np
import re
import logging
from docopt import docopt
logging.basicConfig(
    filename='pre_sentiment.log',
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)
db_url = os.path.expanduser('~/Dropbox/Project2M/Sentiment')
# cmd_doc = '''
#     Usage:
#       bulk_update share auto [--share-back-days=days] [--business=b] [--share-url=url] [--source=source] [--codes=codes]
#       bulk_update share manual <start> <end> [--business=b] [--share-url=url] [--source=source] [--codes=codes]
#       bulk_update sector auto [--sector-back-days=days] [--business=b] [--sector-url=url] [--source=source] [--codes=codes]
#       bulk_update sector manual <start> <end> [--business=b] [--sector-url=url] [--source=source] [--codes=codes]
# 
#     Options:
#       -h --help     Show this screen.
#       -c --codes=codes  ASX codes separated by comma. Mainly for debugging and testing purposes.
#       --share-back-days=days  Days to look backward for shares [default: 0].
#       --sector-back-days=days  Days to look backward for sectors [default: 1].
#       --business=b  Only look at business days [default: True].
#       --share-url=url  URL to find share file [default: ~/Dropbox/Project2M/ASXYearlyCompanyConsolidation/].
#       --sector-url=url  URL to find sector file [default: ~/Dropbox/Project2M/ASXYearlySectorConsolidation/].
#       --source=source  Data source [default: yahoo].
# '''
today = pd.datetime.today()
year = today.year
month = today.month
day = today.day
today = '{:4d}-{:02d}-{:02d}'.format(year, month, day)


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
        article_list = np.asarray(driver.find_elements_by_class_name('article-list'))
        auth_date_list = [article.find_element_by_tag_name('h6').text for article in article_list]
        date_list = pd.to_datetime([re.search(date_pattern, date).groups()[0] for date in auth_date_list])
        is_today= date_list == today
        article_list = article_list[is_today]
        link_href_list += [article.find_element_by_tag_name('a').get_attribute('href') for article in article_list]
        all_on_today = is_today.all()
        if all_on_today:
            next_button = driver.find_element_by_css_selector('a.next.pagination')
            next_button.click()
    code_list = []
    index_list = []
    for link in link_href_list:
        driver.get(link)
        art_text = driver.find_element_by_id('full_content').text
        code_list += list(set(re.findall(asx_pattern, art_text)))
        index_list += list(set(re.findall(index_pattern, art_text)))
    code_counts = save_counts(
        code_list,
        'Motley Fool',
        today,
    )
    index_counts = save_counts(
        index_list,
        'Motley Fool',
        today,
    )
    driver.quit()
    return code_counts, index_counts


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
        {'asx_code': code_list,
         'counts': count_list}
    )
    res_df.loc[:, 'source'] = 'Hotcopper Forum'
    res_df.loc[:, 'date'] = today
    res_df.to_csv('counts.csv', index=False)
    driver.quit()
    return res_df


def save_counts(
        code_list,
        source,
        date,
        ):
    code_series = pd.Series(code_list)
    code_counts = code_series.value_counts()
    code_counts = code_counts.reset_index()
    code_counts.loc[:, 'source'] = source
    code_counts.loc[:, 'date'] = date
    code_counts.columns = ['asx_code', 'counts', 'source', 'date']
    return code_counts


if __name__ == '__main__':
    # arguments = docopt(cmd_doc)
    code_counts, index_counts = scrape_motley_fool()
    code_counts.to_csv(
        os.path.join(db_url, 'code_counts.csv'),
        index=False,
        mode='a',
        header=None
    )
    index_counts.to_csv(
        os.path.join(db_url, 'index_counts.csv'),
        index=False,
        mode='a',
        header=None
    )
    code_counts = scrape_hotcopper_forum()
    code_counts.to_csv(
        os.path.join(db_url, 'code_counts.csv'),
        index=False,
        mode='a',
        header=None
    )
