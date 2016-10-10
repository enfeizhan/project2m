from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from . import secrets


def make_sa_conn_str(host, user, password, dbname, **options):
    '''
    postgresql+psycopg2://<username>:<password>@<hostname>/<dbname>[?<options>]
    '''
    conn_str_template = (
        'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
    )
    postgresql_details = {
        'host': host,
        'user': user,
        'password': password,
        'dbname': dbname
    }
    conn_str = conn_str_template.format(**postgresql_details)
    for ind, option in enumerate(options):
        if ind == 0:
            conn_str += '?'
        else:
            conn_str += '&'
        conn_str += '{}={}'.format(option, options[option])
    return conn_str

sqlalchemy_url = make_sa_conn_str(
    host=secrets.host,
    user=secrets.user,
    password=secrets.password,
    dbname=secrets.dbname,
)

engine = create_engine(sqlalchemy_url, echo=False)
Session = sessionmaker(bind=engine)
