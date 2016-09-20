import re
from io import StringIO
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from . import postgresql_models
from .app import engine
from .utils import today
from .utils import today_str
from .utils import tablename_to_modelname

logger = logging.getLogger(__name__)


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

    @filename.setter
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


def lookup(func):
    def inner(self, load_channel,
              overwrite_existing_records,
              clear_table_first,
              *args):
        for arg in args:
            lookup_tablename = 'lkp_' + arg
            id_name = arg + '_id'
            query = 'select {code}, {code_id} from {table};'.format(
                code=arg,
                code_id=id_name,
                table=lookup_tablename
            )
            lookup = pd.read_sql(
                query,
                con=engine,
                index_col=arg,
                columns=[id_name]
            )
            lookup = lookup.loc[:, id_name]
            self.df.loc[:, id_name] = self.df.loc[:, arg].map(lookup)
            # find non-existing codes
            not_exist = self.df.loc[:, id_name].isnull()
            if not_exist.any():
                new_codes = set(self.df.loc[not_exist.values, arg].tolist())
                new_codes_df = pd.DataFrame({arg: list(new_codes)})
                new_codes_df.loc[:, 'create_date'] = today.date()
                modelname = 'Lkp' + tablename_to_modelname(arg)
                update_codes = LoadChannel(
                    getattr(postgresql_models, modelname)
                )
                update_codes.dataframe = new_codes_df
                update_codes.load_dataframe()
                logger.info(
                    'Updated look-up table: {}'.format(lookup_tablename)
                )
                lookup = pd.read_sql(
                    query,
                    con=engine,
                    index_col=arg,
                    columns=[id_name]
                )
                lookup = lookup.loc[:, id_name]
                self.df.loc[:, id_name] = self.df.loc[:, arg].map(lookup)
            self.df = self.df.drop(arg, axis=1)
        func(self, load_channel,
             overwrite_existing_records,
             clear_table_first, *args)
    return inner


class Load(object):
    @lookup
    def load_to_db(self, load_channel,
                   overwrite_existing_records,
                   clear_table_first, *args):
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


class WebScraper(Extract, Transform, Load):
    def __init__(self, url):
        self.driver = webdriver.PhantomJS()
        self.driver.set_window_size(1120, 550)
        self.url = url
        self.driver.get(self.url)
        self.Select = Select

    def find_element(self, by, identifier, entry_element=None):
        if entry_element:
            return getattr(
                entry_element,
                'find_element_by_{}'.format(by)
            )(identifier)
        else:
            return getattr(
                self.driver,
                'find_element_by_{}'.format(by)
            )(identifier)

    def find_elements(self, by, identifier, entry_element=None):
        if entry_element:
            return getattr(
                entry_element,
                'find_elements_by_{}'.format(by)
            )(identifier)
        else:
            return getattr(
                self.driver,
                'find_elements_by_{}'.format(by)
            )(identifier)

    def quit(self):
        self.driver.quit()

    def attach_suffix(self, col, code_suffix):
        self.df.loc[:, col] = self.df.loc[:, col] + code_suffix


class CodesCounter(WebScraper):
    def download_data(
            self,
            article_by,
            article_identifier,
            date_by,
            date_identifier,
            date_pattern,
            next_button_by,
            next_button_identifier,
            text_by,
            text_identifier,
            **patterns
            ):
        self.links = []
        all_on_today = True
        articles = []
        articles_tmp = []
        while all_on_today:
            # collect all article items
            while articles == articles_tmp:
                articles_tmp = self.find_elements(
                    article_by,
                    article_identifier
                )
            articles = articles_tmp
            # collect dates from below article title
            auth_dates = [
                self.find_element(date_by, date_identifier, article).text
                for article in articles
            ]
            dates = pd.to_datetime(
                [re.search(date_pattern, date).groups()[0]
                 for date in auth_dates]
            )
            # check if article is from today
            is_todays = dates == today_str
            articles = [
                article for article, is_today in zip(articles, is_todays)
                if is_today
            ]
            # get today's articles link
            self.links += [
                self
                .find_element('tag_name', 'a', article)
                .get_attribute('href')
                for article in articles
            ]
            # if all articles are for today,
            # click to next page to check if more
            # today's article on the next page
            all_on_today = is_todays.all()
            if all_on_today:
                # find the "next page" button and click
                self.find_element(
                    next_button_by,
                    next_button_identifier
                ).click()
        self.find_codes(text_by, text_identifier, **patterns)

    def find_codes(
            self,
            text_by,
            text_identifier,
            **patterns
            ):
        for patterns_name, pattern in patterns.items():
            setattr(self, patterns_name, [])
        for link in self.links:
            self.driver.get(link)
            art_text = self.find_element(
                text_by,
                text_identifier
            ).text
            for patterns_name, pattern in patterns.items():
                matched_patterns = (
                    getattr(self, patterns_name) +
                    list(set(re.findall(pattern, art_text)))
                )
                setattr(
                    self,
                    patterns_name,
                    matched_patterns
                )

    def count_codes(self, codes_name):
        codes = getattr(self, codes_name)
        self.df = pd.DataFrame({})
        if len(codes) > 0:
            code_series = pd.Series(codes)
            code_series = code_series.value_counts()
            code_series.name = 'counts'
            code_series.index.name = 'code'
            self.df = code_series.reset_index()


class TablesDownloader(WebScraper):
    def download_data(
            self,
            table_by,
            table_identifier,
            npages=1,
            next_button_by=None,
            next_button_identifier=None,
            earliest_date=None,
            date_pattern=None,
            ):
        self.table_str = ''
        is_enough = False
        table_content_tmp = ''
        table_content = ''
        while npages > 0 and (not is_enough):
            while table_content_tmp == table_content:
                table_content_tmp = self.find_element(
                    table_by,
                    table_identifier
                ).text
            table_content = table_content_tmp
            # need a way to remove first line
            if len(self.table_str) > 0:
                if self.table_str[-1] == '\n':
                    self.table_str += table_content
                else:
                    self.table_str += '\n' + table_content
            else:
                self.table_str += table_content
            if earliest_date:
                assert date_pattern, 'Provide date_pattern!'
                dates = pd.to_datetime(
                    list(set(re.findall(date_pattern, table_content)))
                )
                is_enough = (dates < earliest_date).any()
            if next_button_by:
                assert next_button_identifier, 'Provide next_button_identifier'
                try:
                    self.find_element(
                        next_button_by,
                        next_button_identifier
                    ).click()
                except:
                    pass
            npages -= 1

    def to_dataframe(
            self,
            sep=' ',
            header=True,
            skiprows=None,
            parse_dates=None,
            dayfirst=False,
            infer_datetime_format=False,
            cols_to_keep=None,
            read_table=None,
            **kwargs
            ):
        if read_table:
            self.df = read_table(self.table_str, **kwargs)
        else:
            table_file = StringIO(self.table_str)
            table_file.seek(0)
            self.df = pd.read_table(
                table_file,
                sep=sep,
                header=header,
                skiprows=skiprows,
                parse_dates=parse_dates,
                dayfirst=dayfirst,
                infer_datetime_format=infer_datetime_format
            )
            if cols_to_keep:
                self.df = self.df.iloc[:, cols_to_keep]
