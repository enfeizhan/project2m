from .models import PreSentiment
from .models import YahooSharePrice


class DataSource:
    def __init__(self, filename=None, dataframe=None):
        self.filename = filename
        self.dataframe = dataframe

    @classmethod
    def process_file(cls, filename):
        data_source = cls(filename=filename)
        data_source.etl()
        return data_source

    @classmethod
    def process_dataframe(cls, dataframe):
        data_source = cls(dataframe=dataframe)
        data_source.etl()
        return data_source

    def etl(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass


class PreSentimentLoad(DataSource):
    def load_dataframe(self, overwrite_existing_records=False,
                       clear_table_first=False):
        PreSentiment.load_from_dataframe(
            dataframe=self.dataframe,
            overwrite_existing_records=overwrite_existing_records,
            clear_table_first=clear_table_first
        )

    def load_csv(self, overwrite_existing_records=False,
                 clear_table_first=False, parse_dates=True,
                 date_format='%Y-%m-%d'):
        PreSentiment.load_from_csv(
            filename=self.filename,
            parse_dates=parse_dates,
            date_format=date_format,
            clear_table_first=clear_table_first,
            overwrite_existing_records=overwrite_existing_records
        )

class YahooSharePriceLoad(DataSource):
    def load_dataframe(self, overwrite_existing_records=False,
                       clear_table_first=False):
        YahooSharePrice.load_from_dataframe(
            dataframe=self.dataframe,
            overwrite_existing_records=overwrite_existing_records,
            clear_table_first=clear_table_first
        )

    def load_csv(self, overwrite_existing_records=False,
                 clear_table_first=False, parse_dates=True,
                 date_format='%Y-%m-%d'):
        YahooSharePrice.load_from_csv(
            filename=self.filename,
            parse_dates=parse_dates,
            date_format=date_format,
            clear_table_first=clear_table_first,
            overwrite_existing_records=overwrite_existing_records
        )
