import os
import time
import pandas as pd
try:
    import pandas_datareader.data as web
except ImportError:
    print('Using older Pandas version. Stock actions affected!')
    import pandas.io.data as web
from pandas.tseries.offsets import CDay, DateOffset
from utils import ASXTradingCalendar
today = pd.datetime.now()
start = pd.datetime(1995, 1, 1)
asx_dayoffset = CDay(calendar=ASXTradingCalendar())


def get_n_days_backwards(
        company_codes,
        source='yahoo',
        back_days=1,
        end_datetime=today,
        business=True,
        ):
    if business:
        start_datetime = end_datetime - back_days * asx_dayoffset
    else:
        start_datetime = end_datetime - back_days * DateOffset()
    datapanel = web.DataReader(
        company_codes,
        source,
        start_datetime,
        end_datetime)
    return datapanel.to_frame()


def get_full_history(
        company_codes,
        source='yahoo',
        silent=True,
        start_id=1
        ):
    # get full history for each company one by one
    # and install in separate files
    if isinstance(company_codes, str):
        company_codes = [company_codes]
    ncodes = len(company_codes)
    failed_list = []
    for ncode, code in enumerate(company_codes):
        if ncode < start_id - 1:
            continue
        time.sleep(.1)
        if not silent:
            print(
                '{ncode}/{ncodes}. {code} ...'.format(
                    ncode=ncode+1,
                    ncodes=ncodes,
                    code=code)
                )
        try:
            dataframe = web.DataReader(
                code,
                source,
                start,
                today)
        except OSError:
            print('{} failed!'.format(code))
            failed_list.append(code)
            continue
        try:
            dataframe.loc[:, 'code'] = code
        except ValueError:
            print('{} empty data!'.format(code))
            failed_list.append(code)
            continue
        dataframe.to_csv(code+'.csv')
    return failed_list


def update_full_history(
        company_codes,
        url='./',
        source='yahoo',
        silent=True,
        start_id=1,
        ):
    # given string convert to a list
    if isinstance(company_codes, str):
        company_codes = [company_codes]
    ncodes = len(company_codes)
    failed_list = []
    for ncode, code in enumerate(company_codes):
        if ncode < start_id - 1:
            continue
        time.sleep(.1)
        if not silent:
            print(
                '{ncode}/{ncodes}. {code} ...'.format(
                    ncode=ncode+1,
                    ncodes=ncodes,
                    code=code)
                )
        # if file exists, update it
        if os.path.isfile(url+code+'.csv'):
            # get the last date
            old = pd.read_csv(url+code+'.csv')
            old_datetime_str = old.iloc[-1, 0]
            year = int(old_datetime_str[:4])
            month = int(old_datetime_str[5:7])
            day = int(old_datetime_str[8:])
            update_start = pd.datetime(year, month, day) + DateOffset()
            try:
                dataframe = web.DataReader(
                    code,
                    source,
                    update_start,
                    today)
            except OSError:
                print('{} failed!'.format(code))
                failed_list.append(code)
                continue
            try:
                dataframe.loc[:, 'code'] = code
            except ValueError:
                print('{} empty return'.format(code))
                failed_list.append(code)
                continue
            dataframe.to_csv(url+code+'.csv', header=False, mode='a')
        # if file doesn't exist, install in a new
        else:
            try:
                dataframe = web.DataReader(
                    code,
                    source,
                    start,
                    today)
            except OSError:
                print('{} failed!'.format(code))
                failed_list.append(code)
                continue
            if dataframe.empty:
                print('{} empty data!'.format(code))
            else:
                dataframe.to_csv(url+code+'.csv')
    return failed_list


if __name__ == '__main__':
    asx = pd.read_excel('sector_codes.xlsx')
    # company_codes = (asx.loc[:, 'ASX code'] + '.AX').tolist()
    # res = get_n_days_backwards(
    #     company_codes,
    #     source='yahoo',
    #     back_days=11,
    #     end_datetime=today,
    #     business=True,
    #     )
    # res.to_csv('full.csv')
    # failed_list = get_full_history(company_codes, silent=False)
    # update_full_history(['ZYL.AX'], silent=False)
    # res = update_full_history(
    #     company_codes,
    #     url='/Users/feizhan/Dropbox/Project2M/ASXCompanyHistory/',
    #     source='yahoo',
    #     silent=False,
    #     start_id=1
    #     )
    sector_codes = asx.sector_code.tolist()
    res = update_full_history(
        sector_codes,
        url='./',
        source='yahoo',
        silent=False,
        start_id=1
        )
