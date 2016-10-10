import pandas as pd
from sqlalchemy import inspect
from .etl_tools import LoadChannel
from . import postgresql_models
from .app import Session
from .app import engine
from .utils import today
from .utils import tablename_to_modelname
from .postgresql_models import Base


def correcting_code(codename, wrong_code, correct_code):
    # get lookup table name
    modelname = 'Lkp' + tablename_to_modelname(codename)
    # get lookup model from table name
    model = getattr(postgresql_models, modelname)
    # find the row to delete
    filter_query = getattr(model, codename) == wrong_code
    session = Session()
    query = session.query(model).filter(filter_query)
    row_to_rm = query.one()
    # find the id associated with wrong code
    idname = codename + '_id'
    wrong_id = getattr(row_to_rm, idname)
    # remove it
    query.delete(synchronize_session=False)
    session.commit()
    # check if the correct code already exists
    txt_query = '''
        select * from {tablename} where {codename} = '{correct_code}';
    '''
    txt_query = txt_query.format(
        tablename='lkp_' + codename,
        codename=codename,
        correct_code=correct_code
    )
    not_exist = pd.read_sql(txt_query, con=engine).empty
    # insert a new row with correct code if not exist
    if not_exist:
        row_to_insert = pd.DataFrame(
            {
                idname: [wrong_id],
                codename: [correct_code],
                'create_date': today.date()
            }
        )
        update_lkp = LoadChannel(model)
        update_lkp.dataframe = row_to_insert
        update_lkp.load_dataframe()
    # get correct id
    filter_query = getattr(model, codename) == correct_code
    session = Session()
    query = session.query(model).filter(filter_query)
    correct_id = getattr(query.one(), idname)
    session.commit()
    # walk through all tables other than lkp tables to correct the id
    tablenames = [
        tbname for tbname in Base.metadata.tables.keys()
        if 'lkp' not in tbname
    ]
    modelnames = [
        tablename_to_modelname(tablename)
        for tablename in tablenames
    ]
    for modelname, tablename in zip(modelnames, tablenames):
        # first check if model has this column
        model = getattr(postgresql_models, modelname)
        columns = [col.name for col in inspect(model).columns]
        if idname in columns:
            # pull out rows with wrong ids
            txt_query = '''
                select * from {tablename} where {idname} = {wrong_id};
            '''
            txt_query = txt_query.format(
                tablename=tablename,
                idname=idname,
                wrong_id=wrong_id
            )
            rows_to_correct = pd.read_sql(
                txt_query,
                con=engine
            )
            if not rows_to_correct.empty:
                rows_to_correct.loc[:, idname] = correct_id
                rows_to_correct.loc[:, 'create_date'] = today.date()
                # delete all these rows before push back the correct rows
                filter_query = getattr(model, idname) == wrong_id
                session = Session()
                query = session.query(model).filter(filter_query)
                query.delete(synchronize_session=False)
                session.commit()
                # push back
                pushback = LoadChannel(model)
                pushback.dataframe = rows_to_correct
                pushback.load_dataframe(overwrite_existing_records=True)
