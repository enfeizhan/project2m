import pandas as pd
from pandas_share_access import get_full_history
asx = pd.read_excel('asx_full.xlsx')
company_codes = (asx.loc[:, 'ASX code'] + '.AX').tolist()
get_full_history(company_codes, source='yahoo-actions', silent=False)
