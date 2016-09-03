import pdb
import pandas as pd
import pandas_datareader.data as web
import logging
from .postgresql_models import SharePrice
from .postgresql_models import Action
from .utils import LoadChannel
from .utils import Extract
from .utils import Transform
from .utils import Load
from .utils import country_codes
from .utils import price_type_codes
from .utils import price_source_codes
from .utils import action_source_codes
from .utils import action_type_codes
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
        self.df = web.DataReader(
            codes,
            source,
            start_date,
            end_date,
            session=session
        ).to_frame()
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
        dat = web.DataReader(
            code,
            source,
            start_date,
            end_date,
            session=session
        )
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
            .map(action_type_codes)
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
            'price_type': price_type_codes[data_type],
            'source': price_source_codes[source],
            'country': country_codes[country],
            'create_date': pd.datetime.today()
        }
        etl.attach_infos(col_codes)
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first
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
            'price_type': price_type_codes[data_type],
            'source': price_source_codes[source],
            'country': country_codes[country],
            'create_date': pd.datetime.today()
        }
        etl.attach_infos(col_codes)
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first
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
            'source': action_source_codes[source],
            'country': country_codes[country],
            'div_date': None,
            'pay_date': None,
            'franking': None,
        }
        etl.attach_infos(col_codes)
        etl.categorise_action_type()
        etl.reset_index()
        etl.fill_ex_div_date()
        etl.attach_create_date()
        etl.load_to_db(
            load_channel,
            overwrite_existing_records,
            clear_table_first
        )
        etl.logging(data_type, 'code')
