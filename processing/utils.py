import logging
import pdb
import re
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from selenium import webdriver
from pandas.tseries.holiday import AbstractHolidayCalendar
from pandas.tseries.holiday import Holiday
from pandas.tseries.holiday import next_monday
from pandas.tseries.holiday import MO
from pandas.tseries.holiday import GoodFriday
from pandas.tseries.holiday import EasterMonday
from pandas.tseries.holiday import DateOffset


logger = logging.getLogger(__name__)


def my_next_monday_or_tuesday(dt):
    dow = dt.weekday()
    if dow == 5 or dow == 6:
        return dt + timedelta(2)
    return dt


class ASXTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Year\'s Day', month=1, day=1, observance=next_monday),
        Holiday('Australia Day', month=1, day=26, observance=next_monday),
        GoodFriday,
        EasterMonday,
        Holiday('ANZAC Day', month=4, day=25),
        Holiday('Queen\'s Birthday', month=6, day=1,
                offset=DateOffset(weekday=MO(2))),
        Holiday('Christmas Day', month=12, day=25,
                observance=my_next_monday_or_tuesday),
        Holiday('Boxing Day', month=12, day=26,
                observance=my_next_monday_or_tuesday),
    ]


def str2bool(string):
    return string == 'True'

today = datetime.now()
today_str = today.strftime('%Y%m%d')
country_codes = {
    'Australia': 0
}
price_type_codes = {
    'share': 0,
    'sector': 1
}
price_source_codes = {
    'yahoo': 0
}
pre_sentiment_source_codes = {
    'Hotcopper Forum': 0,
    'Motley Fool': 1
}
action_source_codes = {
    'yahoo-actions': 0
}
action_type_codes = {
    'dividend': 0,
    'split': 1
}
lkp_tables = [
    'country',
    'price_type',
    'price_source',
    'pre_sentiment_source',
    'action_source',
    'action_type'
]


class LoadChannel(object):
    def __init__(self, model):
        self.model = model
        self._dataframe = None
        self._filename = None

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, df):
        self._dataframe = df

    @property
    def filename(self):
        return self._filename

    @dataframe.setter
    def filename(self, fn):
        self._filename = fn

    def load_dataframe(self, overwrite_existing_records=False,
                       clear_table_first=False):
        self.model.load_from_dataframe(
            dataframe=self.dataframe,
            overwrite_existing_records=overwrite_existing_records,
            clear_table_first=clear_table_first
        )

    def load_csv(self, overwrite_existing_records=False,
                 clear_table_first=False, parse_dates=True,
                 date_format='%Y-%m-%d'):
        self.model.load_from_csv(
            filename=self.filename,
            parse_dates=parse_dates,
            date_format=date_format,
            clear_table_first=clear_table_first,
            overwrite_existing_records=overwrite_existing_records
        )


class Extract(object):
    pass


class Transform(object):
    def reset_index(self):
        self.df = self.df.reset_index()

    def change_column_name(self, column_name_change):
        self.df = self.df.rename(columns=column_name_change)

    def attach_infos(self, col_codes):
        assert isinstance(col_codes, dict), 'col_codes dict, {col: code}'
        for col, code in col_codes.items():
            self.df.loc[:, col] = code


class Load(object):
    def load_to_db(self, load_channel,
                   overwrite_existing_records=False,
                   clear_table_first=False):
        load_channel.dataframe = self.df
        load_channel.load_dataframe(
            overwrite_existing_records=overwrite_existing_records,
            clear_table_first=clear_table_first
        )

    def logging(self, target, target_col):
        logger.info(
            '{n} {target}s updated.'.format(
                n=self.df.loc[:, target_col].nunique(),
                target=target,
            )
        )


class WebScraper(object):
    def __init__(self, url):
        self.driver = webdriver.PhantomJS()
        self.driver.set_window_size(1120, 550)
        self.url = url
        self.driver.get(self.url)

    def find_elements(self, find_method, find_identifier, start=None):
        if start:
            return getattr(start, find_method)(find_identifier)
        else:
            return getattr(self.driver, find_method)(find_identifier)

    def search_article_urls(
            self,
            article_find_method,
            article_find_identifier,
            date_find_method,
            date_find_identifier,
            date_pattern,
            next_button_find_method,
            next_button_find_identifier
        ):
        self.link_href_list = []
        all_on_today = True
        while all_on_today:
            # collect all article items
            article_list = np.asarray(
                self.find_elements(
                    article_find_method,
                    article_find_identifier
                )
            )
            # collect dates from below article title
            auth_date_list = [
                self.find_elements(
                    date_find_method,
                    date_find_identifier,
                    article
                ).text
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
            self.link_href_list += [
                article.find_element_by_tag_name('a').get_attribute('href')
                for article in article_list
            ]
            # if all articles are for today, click to next page to check if more
            # today's article on the next page
            all_on_today = is_today.all()
            if all_on_today:
                # find the "next page" button and click
                self.find_elements(
                    next_button_find_method,
                    next_button_find_identifier
                ).click()

    def find_all_pattern(
            self,
            text_find_method,
            text_find_identifier,
            **patterns
        ):
        for pattern_list_name, pattern in patterns.items():
            setattr(self, pattern_list_name, [])
        for link in self.link_href_list:
            self.driver.get(link)
            art_text = self.find_elements(
                text_find_method,
                text_find_identifier
            ).text
            for pattern_list_name, pattern in patterns.items():
                pattern_list = (
                    getattr(self, pattern_list_name) +
                    list(set(re.findall(pattern, art_text)))
                )
                setattr(
                    self,
                    pattern_list_name,
                    pattern_list
                )

    def quit(self):
        self.driver.quit()
