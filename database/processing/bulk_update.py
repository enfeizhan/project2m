import pandas as pd
import numpy as np
import pandas_datareader.data as web
import logging
from .postgresql_models import SharePrice
from .postgresql_models import Action
from .etl_tools import LoadChannel
from .etl_tools import Extract
from .etl_tools import Transform
from .etl_tools import Load
from .etl_tools import TablesDownloader
from .utils import today
yahoo_price_name_change = {
    'minor': 'code',
    'Date': 'date',
    'Open': 'open_price',
    'High': 'high_price',
    'Low': 'low_price',
    'Close': 'close_price',
    'Volume': 'volume',
    'Adj Close': 'adj_close_price',
}
yahoo_action_name_change = {
    'action': 'action_type',
    'value': 'amount'
}
logger = logging.getLogger(__name__)
asx_company_list_url = (
    'http://www.asx.com.au/asx/research/ASXListedCompanies.csv'
)


class IncompleteDateRangeError(Exception):
    pass


class InternetError(Exception):
    pass


def download_asx_company_list():
    # read the full company list from asx home page
    # if not found, asx has changed the url
    try:
        asx = pd.read_csv(
            asx_company_list_url,
            skiprows=1
        )
        return (asx.loc[:, 'ASX code'] + '.AX').tolist()
    except:
        raise InternetError('Share list not found on ASX!')


class PandasDataReaderExtract(Extract):
    def download_data(self, codes, source,
                      start_date, end_date,
                      session):
        # get data through pandas
        self.df = pd.DataFrame({})
        try:
            self.df = web.DataReader(
                codes,
                source,
                start_date,
                end_date,
                session=session
            ).to_frame()
        except:
            return
        self.reset_index()


class PandasDataReader(PandasDataReaderExtract, Transform, Load):
    pass


class PandasDataReaderActionExtract(Extract):
    def download_data(self, code, source,
                      start_date, end_date,
                      session):
        # get data through pandas
        if not hasattr(self, 'df'):
            self.df = pd.DataFrame({})
        try:
            dat = web.DataReader(
                code,
                source,
                start_date,
                end_date,
                session=session
            )
        except:
            return
        dat.loc[:, 'code'] = code
        self.df = self.df.append(dat)


class PandasDataReaderActionTransform(Transform):
    def reset_index(self):
        self.df.index.name = 'date'
        self.df = self.df.reset_index()

    def fill_ex_div_date(self):
        self.df.loc[:, 'ex_div_date'] = self.df.loc[:, 'date']

    def categorise_action_type(self):
        self.df.loc[:, 'action_type'] = (
            self
            .df
            .loc[:, 'action_type']
            .str
            .lower()
        )


class PandasDataReaderAction(
        PandasDataReaderActionExtract,
        PandasDataReaderActionTransform,
        PandasDataReader,
        ):
    pass


def update_market(
        codes=None,
        start_date=None,
        end_date=None,
        source=None,
        data_type=None,
        country='Australia',
        session=None,
        overwrite_existing_records=False,
        clear_table_first=False
        ):
    if codes is not None:
        codes = codes.split(',')
    if (source == 'yahoo' and data_type == 'share'
       and country == 'Australia'):
        etl = PandasDataReader()
        load_channel = LoadChannel(SharePrice)
        column_name_change = yahoo_price_name_change
        if codes is None:
            codes = download_asx_company_list()
        logger.info('There are {n} shares to update.'.format(n=len(codes)))
        etl.download_data(
            codes,
            source,
            start_date,
            end_date,
            session
        )
        etl.change_column_name(column_name_change)
        col_codes = {
            'price_type': data_type,
            'source': source,
            'country': country,
            'create_date': today.date()
        }
        etl.attach_infos(col_codes)
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first,
            'source',
            'country',
            'price_type'
        )
        etl.logging(data_type, 'code')
    elif (source == 'yahoo' and data_type == 'sector'
          and country == 'Australia'):
        etl = PandasDataReader()
        load_channel = LoadChannel(SharePrice)
        column_name_change = yahoo_price_name_change
        if codes is None:
            asx = pd.read_excel('sector_codes.xlsx')
            codes = asx.sector_code.tolist()
        logger.info('There are {n} sectors to update.'.format(n=len(codes)))
        etl.download_data(
            codes,
            source,
            start_date,
            end_date,
            session
        )
        etl.change_column_name(column_name_change)
        col_codes = {
            'price_type': data_type,
            'source': source,
            'country': country,
            'create_date': today.date()
        }
        etl.attach_infos(col_codes)
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first,
            'price_type',
            'source',
            'country'
        )
        etl.logging(data_type, 'code')
    elif (source == 'yahoo-actions' and data_type == 'action'
          and country == 'Australia'):
        etl = PandasDataReaderAction()
        load_channel = LoadChannel(Action)
        column_name_change = yahoo_action_name_change
        if codes is None:
            codes = download_asx_company_list()
        logger.info('There are {n} shares to update.'.format(n=len(codes)))
        for code in codes:
            etl.download_data(
                code,
                source,
                start_date,
                end_date,
                session
            )
        etl.change_column_name(column_name_change)
        col_codes = {
            'source': source,
            'country': country,
            'div_date': None,
            'pay_date': None,
            'franking': None,
            'create_date': today.date()
        }
        etl.attach_infos(col_codes)
        etl.categorise_action_type()
        etl.reset_index()
        etl.fill_ex_div_date()
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first,
            *('source', 'country', 'action_type')
        )
        etl.logging(data_type, 'code')


def start_over(
        codes=None,
        start_date=None,
        end_date=None,
        source=None,
        data_type=None,
        country='Australia',
        session=None,
        overwrite_existing_records=False,
        clear_table_first=False
        ):
    if codes is not None:
        codes = codes.split(',')
    if (source == 'yahoo' and data_type == 'share'
       and country == 'Australia'):
        etl = PandasDataReader()
        load_channel = LoadChannel(SharePrice)
        column_name_change = yahoo_price_name_change
        if codes is None:
            codes = download_asx_company_list()
        logger.info('There are {n} shares to update.'.format(n=len(codes)))
        for code in codes:
            etl.download_data(
                [code],
                source,
                start_date,
                end_date,
                session
            )
            if etl.df.empty:
                continue
            etl.change_column_name(column_name_change)
            col_codes = {
                'price_type': data_type,
                'source': source,
                'country': country,
                'create_date': today.date()
            }
            etl.attach_infos(col_codes)
            etl.load_to_db(
                load_channel,
                overwrite_existing_records,
                clear_table_first,
                'source',
                'country',
                'price_type'
            )
    elif (source == 'yahoo' and data_type == 'sector'
          and country == 'Australia'):
        etl = PandasDataReader()
        load_channel = LoadChannel(SharePrice)
        column_name_change = yahoo_price_name_change
        if codes is None:
            asx = pd.read_excel('sector_codes.xlsx')
            codes = asx.sector_code.tolist()
        logger.info('There are {n} sectors to update.'.format(n=len(codes)))
        for code in codes:
            etl.download_data(
                [code],
                source,
                start_date,
                end_date,
                session
            )
            if etl.df.empty:
                continue
            etl.change_column_name(column_name_change)
            col_codes = {
                'price_type': data_type,
                'source': source,
                'country': country,
                'create_date': today.date()
            }
            etl.attach_infos(col_codes)
            etl.load_to_db(
                load_channel,
                overwrite_existing_records,
                clear_table_first,
                'price_type',
                'source',
                'country'
            )
    elif (source == 'yahoo-actions' and data_type == 'action'
          and country == 'Australia'):
        etl = PandasDataReaderAction()
        load_channel = LoadChannel(Action)
        column_name_change = yahoo_action_name_change
        if codes is None:
            codes = download_asx_company_list()
        logger.info('There are {n} shares to update.'.format(n=len(codes)))
        for code in codes:
            etl.download_data(
                code,
                source,
                start_date,
                end_date,
                session
            )
        etl.change_column_name(column_name_change)
        col_codes = {
            'source': source,
            'country': country,
            'div_date': None,
            'pay_date': None,
            'franking': None,
            'create_date': today.date()
        }
        etl.attach_infos(col_codes)
        etl.categorise_action_type()
        etl.reset_index()
        etl.fill_ex_div_date()
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first,
            *('source', 'country', 'action_type')
        )
        etl.logging(data_type, 'code')


# intelligent investor dividend
def scrape_intelligent_investor_dividend(npages, earliest_date=None):
    intel = TablesDownloader(
        url='https://www.intelligentinvestor.com.au/companies/dividends'
    )
    intel.download_data(
        'tag_name',
        'tbody',
        npages=npages,
        next_button_by='xpath',
        next_button_identifier='//ul[@class="pagination"]/li[3]/a',
        earliest_date=earliest_date,
        date_pattern=r'\d{2} [A-Z][a-z]{2} \d{4}',
    )
    intel.to_dataframe(
        header=None,
        parse_dates={'ex_div_date': [1, 2, 3], 'pay_date': [4, 5, 6]},
        dayfirst=True,
        infer_datetime_format=True,
    )
    intel.change_column_name(
        column_name_change={0: 'code', 7: 'amount', 8: 'franking'}
    )
    col_codes = {
        'action_type': 'dividend',
        'source': 'IntelligentInvestor',
        'country': 'Australia',
        'create_date': today.date()
    }
    intel.attach_infos(col_codes)
    intel.df = intel.df.loc[(intel.df.code.str.len() == 3).values, :].copy()
    intel.attach_suffix('code', '.AX')
    intel.df.loc[:, 'amount'] = (
        intel
        .df
        .loc[:, 'amount']
        .str[:-1]
        .apply(lambda x: float(x))
    )
    intel.df.loc[:, 'franking'] = (
        intel
        .df
        .loc[:, 'franking']
        .str[:-1]
        .apply(lambda x: float(x)/100)
    )
    intel.df.loc[:, 'date'] = intel.df.loc[:, 'ex_div_date']
    intel.df.loc[:, 'div_date'] = np.nan
    load_channel = LoadChannel(Action)
    intel.load_to_db(
        load_channel,
        overwrite_existing_records=True,
        *['action_type', 'source', 'country']
    )


class IndexmundiCommodity(TablesDownloader):
    def __init__(self, url, commodity_cat, commodity, time_range, currency):
        super(IndexmundiCommodity, self).__init__(url)
        commodity_cat_tab = self.find_element('link_text', commodity_cat)
        commodity_cat_tab.click()
        commodity_tab = self.find_element('link_text', commodity)
        commodity_tab.click()
        time_range_button = self.find_element('link_text', time_range)
        time_range_button.click()
        self.select = self.Select(self.find_element('id', 'listCurrency'))
        self.select.select_by_visible_text(currency)


def scrape_indexmundi_commodity(
        commodity_cat,
        commodity,
        time_range,
        currency,
        ):
    indexmundi = IndexmundiCommodity(
        url='http://www.indexmundi.com/commodities/',
        commodity_cat=commodity_cat,
        commodity=commodity,
        time_range=time_range,
        currency=currency
    )
    indexmundi.download_data('id', 'gvPrices')

    def read_table(table_str, **kwargs):
        table_strs = table_str.split('\n')
        table_row1 = [table_strs[1].split(' ')[:-1]]
        table_rows = [table_str.split(' ')[:3] for table_str in table_strs[2:]]
        table = pd.DataFrame(table_row1 + table_rows)
        table.loc[:, 1] = table.loc[:, 0] + table.loc[:, 1]
        table = table.drop(0, axis=1)
        table.columns = ['date', 'price']
        table.loc[:, 'date'] = pd.to_datetime(
            table.loc[:, 'date'],
            format='%b%Y'
        )
        return table

    indexmundi.to_dataframe(read_table)
