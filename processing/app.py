from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import secrets


def make_sa_conn_str(host, user, password, dbname, **options):
    pass

sqlalchemy_url = make_sa_conn_str(
    host=secrets.host,
    user=secrets.user,
    password=secrets.password,
    dbname=secrets.dbname,
    charset='utf8'
)

engine = create_engine(sqlalchemy_url, echo=False)
