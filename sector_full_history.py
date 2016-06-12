import pandas as pd
from pandas_share_access import get_full_history
sector_dat = pd.read_excel('sector_codes.xlsx')
sector_codes = sector_dat.sector_code.tolist()
get_full_history(sector_codes, silent=False)
