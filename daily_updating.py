import pandas as pd
from pandas_share_access import get_n_days_backwards
from pandas.tseries.offsets import CustomBusinessDay
from utils import ASXTradingCalendar
asx_dayoffset = CustomBusinessDay(calendar=ASXTradingCalendar())
flag_col_name = 'is_last_11_day'
csv_back_days = 10
db_url = '/home/fei/Dropbox/Project2M/ASXYearlyCompanyConsolidation/'
# read the full company list
asx = pd.read_excel('asx_full.xlsx')
codes = (asx.loc[:, 'ASX code'] + '.AX').tolist()
now = pd.datetime.now()
# now = pd.datetime(2016, 5, 13)
# get data through pandas
res = get_n_days_backwards(
    # ['AMP.AX', 'TLS.AX', 'ANZ.AX'],
    codes,
    source='yahoo',
    back_days=0,
    # end_datetime=pd.datetime(2016, 4, 6),
    end_datetime=now,
    business=True,
    )
with open('sharecounts.txt', 'w') as f:
    f.write(str(res.shape[0]))
res = res.reset_index()
# find the last day for the purpose of finding last 11 days
last_day = res.Date.max()
days_ago = last_day - csv_back_days * asx_dayoffset
# find this year to find the file
this_year = str(last_day.year)
res = res.rename(columns={'minor': 'code'})
try:
    yearly_dat = pd.read_csv(db_url+this_year+'price.csv')
    yearly_dat.loc[:, 'Date'] = pd.to_datetime(yearly_dat.Date.values)
    # append data to the file of this year
    res = pd.concat([res, yearly_dat])
    # drop duplicates sometimes
    res = res.drop_duplicates(['Date', 'code'])
    res = res.sort_values(['code', 'Date'])
    res = res.set_index(['Date', 'code'])
    # reset to all zeros
    res.loc[:, flag_col_name] = 0
    # set the last 11 days as one
    res = res.sort_index(level=[0, 1])
    res.loc[days_ago:last_day, flag_col_name] = 1
    res = res.sort_index(level=[1, 0])
    res.to_csv(db_url+this_year+'price.csv')
except OSError:
    # beginning of a year can't find the file
    # reset to all one
    res.loc[:, flag_col_name] = 1
    res.to_csv(db_url+this_year+'price.csv', index=False)

# now work on sectors
db_url = '/home/fei/Dropbox/Project2M/ASXYearlySectorConsolidation/'
asx = pd.read_excel('sector_codes.xlsx')
codes = asx.sector_code.tolist()
# codes = ['^AXDJ', '^AXEJ']
# get the data through pandas
# sector data usually updated the next day
res = get_n_days_backwards(
    codes,
    source='yahoo',
    back_days=1,
    # end_datetime=pd.datetime(2016, 4, 6),
    end_datetime=now,
    business=True,
    )
res = res.reset_index()
# find the last day for the purpose of finding last 11 days
last_day = res.Date.max()
days_ago = last_day - csv_back_days * asx_dayoffset
# find this year to find the file
this_year = str(last_day.year)
res = res.rename(columns={'minor': 'code'})
try:
    yearly_dat = pd.read_csv(db_url+this_year+'sector_price.csv')
    yearly_dat.loc[:, 'Date'] = pd.to_datetime(yearly_dat.Date.values)
    # append data to the file of this year
    res = pd.concat([res, yearly_dat])
    # drop duplicates sometimes
    res = res.drop_duplicates(['Date', 'code'])
    res = res.sort_values(['code', 'Date'])
    res = res.set_index(['Date', 'code'])
    # reset to all zeros
    res.loc[:, flag_col_name] = 0
    # set the last 11 days as one
    res = res.sort_index(level=[0, 1])
    res.loc[days_ago:last_day, flag_col_name] = 1
    res = res.sort_index(level=[1, 0])
    res.to_csv(db_url+this_year+'sector_price.csv')
except OSError:
    # beginning of a year can't find the file
    # reset to all one
    res.loc[:, flag_col_name] = 1
    res.to_csv(db_url+this_year+'sector_price.csv', index=False)
