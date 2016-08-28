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
