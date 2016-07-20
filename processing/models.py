from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy.ext.declarative import declarative_base

from .app import engine

Base = declarative_base()


class ColumnsMismatchError(Exception):
    pass


class PreSentiment(Base):
    __tablename__ = 'pre_sentiment'
    pk = Column(Integer, nullable=False, primary_key=True)
    asx_code = Column(String(255), nullable=False)
    counts = Column(Integer, nullable=False)
    source = Column(String(255), nullable=False)
    date = Column(Date, nullable=False)


def create_all_tables():
    Base.metadata.create_all(engine)
