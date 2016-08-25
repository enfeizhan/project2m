import pandas as pd
import pandas_datareader.data as web
import logging
yahoo_price_name_change ={
    'minor': 'code',
    'Date': 'date',
    'Open': 'open_price',
    'High': 'high_price',
    'Low': 'low_price',
    'Close': 'close_price',
    'Volume': 'volume',
    'Adj Close': 'adj_close_price',
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


class Load(object):
    def __init__(
            self,
            codes,
            source,
            start_date,
            end_date,
            price_type,
            session,
            is_sector,
            load_channel,
            column_name_change,
            overwrite_existing_records,
            clear_table_first
        ):
        self.codes = codes
        self.source = source
        self.start_date = start_date
        self.end_date = end_date
        self.price_type = price_type
        self.session = session
        self.is_sector = is_sector
        self.load_channel = load_channel
        self.column_name_change = column_name_change
        self.overwrite_existing_records = overwrite_existing_records
        self.clear_table_first = clear_table_first

    def download_data(self):
        if not (self.start_date and self.end_date):
            raise IncompleteDateRangeError('Incomplete time range!')
        # get data through pandas
        self.res = web.DataReader(
            self.codes,
            self.source,
            self.start_date,
            self.end_date,
            session=self.session
        ).to_frame()
        self.res = self.res.reset_index()

    def _change_column_name(self):
        if self.column_name_change:
            self.res = self.res.rename(columns=self.column_name_change)

    def load_to_db(self):
        self._change_column_name()
        if self.is_sector is not None:
            self.res.loc[:, 'is_sector'] = self.is_sector 
        self.res.loc[:, 'create_date'] = pd.datetime.today()
        logger.info(
            '{n} {price_type}s updated.'.format(
                n=self.res.code.nunique(),
                price_type=self.price_type,
            )
        )
        to_load = self.load_channel.process_dataframe(self.res)
        to_load.load_dataframe(
            overwrite_existing_records=self.overwrite_existing_records,
            clear_table_first=self.clear_table_first
        )
    

def update_market(
        codes=None,
        start_date=None,
        end_date=None,
        source=None,
        price_type=None,
        session=None,
        overwrite_existing_records=False,
        clear_table_first=False
    ):
    if source == 'yahoo' and price_type == 'share':
        from .datasets import YahooSharePriceLoad as load_channel
        is_sector = False 
        column_name_change = yahoo_price_name_change
        if codes is None:
            codes = download_asx_company_list()
        else:
            # when given codes, just split them to get a list
            codes = codes.split(',')
        logger.info('There are {n} shares to update.'.format(n=len(codes)))
    elif source == 'yahoo' and price_type == 'sector':
        from .datasets import YahooSharePriceLoad as load_channel
        is_sector = True
        column_name_change = yahoo_price_name_change
        if codes is None:
            asx = pd.read_excel('sector_codes.xlsx')
            codes = asx.sector_code.tolist()
        else:
            # when given codes, just split them to get a list
            codes = codes.split(',')
        logger.info('There are {n} sectors to update.'.format(n=len(codes)))
    load = Load(
        codes=codes,
        source=source,
        start_date=start_date,
        end_date=end_date,
        price_type=price_type,
        is_sector=is_sector,
        load_channel=load_channel,
        column_name_change=column_name_change,
        session=session,
        overwrite_existing_records=overwrite_existing_records,
        clear_table_first=clear_table_first
    )
    load.download_data()
    load.load_to_db()
