import os
import pandas as pd
import datetime

import logging

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from .app import engine

Base = declarative_base()
logger = logging.getLogger(__name__)


class ColumnsMismatchError(Exception):
    pass


class DataframeLoadable:
    @classmethod
    def load_from_dataframe(cls, dataframe,
                            records_per_insert=10000,
                            overwrite_existing_records=False,
                            check_columns_match=True,
                            clear_table_first=False):
        if check_columns_match:
            if not cls.is_compatible(dataframe):
                raise ColumnsMismatchError
        if clear_table_first:
            cls.delete_all()
        elif overwrite_existing_records:
            cls.delete_conflicting_records(dataframe)
        dataframe.to_sql(cls.__tablename__, engine,
                         if_exists='append',
                         chunksize=records_per_insert,
                         index=False)

    @classmethod
    def is_compatible(cls, dataframe):
        csv_columns = set(dataframe.columns)
        table_columns = {col.name for col in cls.__table__.columns}
        primary_key = inspect(cls).primary_key[0].name
        result = csv_columns == table_columns
        if not result:
            if primary_key in ('pk', 'id'):
                table_columns.discard(primary_key)
        result = csv_columns == table_columns
        return result

    @classmethod
    def delete_all(cls):
        cls.__table__.drop(engine, checkfirst=True)
        cls.__table__.create(engine, checkfirst=True)

    @classmethod
    def delete_conflicting_records(cls, dataframe):
        from .app import Session
        session = Session()
        primary_keys = list(map(lambda x: x.name, inspect(cls).primary_key))
        for ind, row in dataframe.iterrows():
            filter_query = (
                getattr(cls, primary_key) == row.loc[primary_key]
                for primary_key in primary_keys
            )
            query = session.query(cls).filter(and_(filter_query))
            query.delete(synchronize_session=False)
        session.commit()


class CSVLoadable(DataframeLoadable):
    @classmethod
    def load_from_csv(cls, filename,
                      clear_table_first=True,
                      overwrite_existing_records=False,
                      records_per_insert=10000,
                      parse_dates=False,
                      date_format=None):
        if filename.endswith('.gz'):
            compression = 'gzip'
        else:
            compression = None
        reader = pd.read_csv(filename,
                             chunksize=records_per_insert,
                             compression=compression)
        for i, data in enumerate(reader):
            if parse_dates:
                for col in cls.datetime_columns():
                    data[col] = pd.to_datetime(data[col], format=date_format)
            logger.info('{}: inserting up to row {}'.format(
                filename, (i + 1) * records_per_insert))

            load_options = {
                'dataframe': data,
                'records_per_insert': records_per_insert,
                'overwrite_existing_records': overwrite_existing_records,
                'check_columns_match': False,
                'clear_table_first': False
            }
            if i == 0:
                load_options['check_columns_match'] = True,
                if clear_table_first:
                    load_options['clear_table_first'] = True
            cls.load_from_dataframe(**load_options)

    @classmethod
    def load_from_csv_orig(cls, filename,
                           clear_table_first=True,
                           overwrite_existing_records=False,
                           records_per_insert=10000,
                           parse_dates=False,
                           date_format=None):
        if filename.endswith('.gz'):
            compression = 'gzip'
        else:
            compression = None
        reader = pd.read_csv(filename,
                             chunksize=records_per_insert,
                             compression=compression)
        for i, data in enumerate(reader):
            if parse_dates:
                for col in cls.datetime_columns():
                    data[col] = pd.to_datetime(data[col], format=date_format)
            logger.info('{}: inserting up to row {}'.format(
                filename, (i + 1) * records_per_insert))

            if i == 0:
                if not cls.is_compatible(data):
                    raise ColumnsMismatchError
                elif clear_table_first:
                    cls.delete_all()
            if overwrite_existing_records:
                cls.delete_conflicting_records(data)
            data.to_sql(cls.__tablename__, engine,
                        if_exists='append',  # chunksize=records_per_insert,
                        index=False)

    @classmethod
    def datetime_columns(cls):
        return [col.name for col in cls.__table__.columns
                if type(col.type) is DateTime]


class PreSentiment(Base, CSVLoadable):
    __tablename__ = 'pre_sentiment'
    code = Column(String(20), primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)
    source = Column(Integer, primary_key=True, nullable=False)
    country = Column(Integer, nullable=False)
    counts = Column(Integer, nullable=False)


class SharePrice(Base, CSVLoadable):
    __tablename__ = 'share_price'
    code = Column(String(20), primary_key=True, nullable=False)
    date = Column(Date, primary_key=True, nullable=False)
    source = Column(Integer, primary_key=True, nullable=False)
    country = Column(Integer, nullable=False)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    adj_close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    price_type = Column(Integer, nullable=False)
    create_date = Column(Date, nullable=False)


class LkpCountry(Base, CSVLoadable):
    __tablename__ = 'lkp_country'
    country_id = Column(Integer, primary_key=True, nullable=False)
    country = Column(String(50), nullable=False)
    create_date = Column(Date)


class LkpPriceType(Base, CSVLoadable):
    __tablename__ = 'lkp_price_type'
    price_type_id = Column(Integer, primary_key=True, nullable=False)
    price_type = Column(String(50), nullable=False)
    create_date = Column(Date)


class LkpPriceSource(Base, CSVLoadable):
    __tablename__ = 'lkp_price_source'
    price_source_id = Column(Integer, primary_key=True, nullable=False)
    price_source = Column(String(50), nullable=False)
    create_date = Column(Date)


class LkpPreSentimentSource(Base, CSVLoadable):
    __tablename__ = 'lkp_pre_sentiment_source'
    pre_sentiment_source_id = Column(Integer, primary_key=True, nullable=False)
    pre_sentiment_source = Column(String(50), nullable=False)
    create_date = Column(Date)


def create_all_tables():
    Base.metadata.create_all(engine)
