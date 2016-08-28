import logging
import pandas as pd
from . import utils
from . import postgresql_models
from .load_channels import LoadChannel
from .utils import today

logger = logging.getLogger(__name__)

def update_lkp_table(table_name):
    codes = pd.Series(getattr(utils, table_name+'_codes'))
    codes.index.name = table_name
    codes.name = table_name + '_id'
    codes = codes.reset_index()
    codes.loc[:, 'create_date'] = today.date()
    table_name_list = table_name.split('_')
    table_name_list = list(map(lambda x: x.capitalize(), table_name_list))
    model_name = 'Lkp' + ''.join(table_name_list)
    update_codes = LoadChannel(getattr(postgresql_models, model_name))
    update_codes.dataframe = codes
    update_codes.load_dataframe(overwrite_existing_records=True)
    logger.info('Updated look-up table: {}.'.format(table_name))


def update_all_lkp_tables():
    from .utils import lkp_tables
    for lkp_table in lkp_tables:
        update_lkp_table(lkp_table)
