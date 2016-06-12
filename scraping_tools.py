import time
import os
import pandas as pd
from pandas.tseries.offsets import BDay, DateOffset
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
today = pd.datetime.now()
months = {
    1: 'Jan',
    2: 'Feb',
    3: 'Mar',
    4: 'Apr',
    5: 'May',
    6: 'Jun',
    7: 'Jul',
    8: 'Aug',
    9: 'Sep',
    10: 'Oct',
    11: 'Nov',
    12: 'Dec'
    }
num_months = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'
    }
periods = ['daily', 'weekly', 'monthly', 'divonly']
profile = FirefoxProfile()
profile.set_preference("browser.helperApps.neverAsk.saveToDisk", 'text/csv')
# profile.set_preference(
#     "browser.download.dir",
#     '~/Documents/studies/share_cooperation')
indices = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']


def download_historical_share_prices(
        company_codes,
        start_year,
        start_month,
        start_day,
        end_year=today.year,
        end_month=today.month,
        end_day=today.day,
        chosen_period='daily',
        start_code_id=0,
        operation='download',
        ):
    if isinstance(company_codes, str):
        # convert a string to a list
        company_codes = [company_codes]
    tot_codes = len(company_codes)
    correct_start_year = (
        isinstance(start_year, int) and
        start_year > 1900)
    correct_end_year = (
        isinstance(end_year, int) and
        end_year >= start_year)
    correct_start_month = (
        isinstance(start_month, int) and
        start_month >= 1 and
        start_month <= 12)
    correct_end_month = (
        isinstance(end_month, int) and
        end_month >= 1 and
        end_month <= 12)
    correct_start_day = (
        isinstance(start_day, int) and
        start_day >= 1 and
        start_day <= 31)
    correct_end_day = (
        isinstance(end_day, int) and
        end_day >= 1 and
        end_day <= 31)
    correct_operation = operation in ['download', 'scrape']
    assert correct_start_year, 'Wrong start year!'
    assert correct_end_year, 'Wrong end year!'
    assert correct_start_month, 'Wrong start month!'
    assert correct_end_month, 'Wrong end month!'
    assert correct_start_day, 'Wrong start day!'
    assert correct_end_day, 'Wrong end day!'
    assert correct_operation, 'Operation only \'download\' or \'scrape\''
    if chosen_period not in periods:
        raise SystemError(
            'Chosen period should be one of \'{}\', \'{}\', \'{}\', \'{}\'!'
            .format(*periods)
            )
    driver = webdriver.Firefox(firefox_profile=profile)
    dat = pd.DataFrame({})
    for code_id, company_code in enumerate(company_codes):
        if code_id < start_code_id:
            continue
        print('Scraping page {code_id}/{tot_codes} ...'
              .format(code_id=code_id+1, tot_codes=tot_codes))
        print('Open page ...')
        if company_code == '%5EAXJO':
            driver.get('https://au.finance.yahoo.com/q/hp?s='+company_code)
        else:
            driver.get(
                'https://au.finance.yahoo.com/q/hp?s='+company_code+'.AX')
        print('Select time windows ...')
        time.sleep(1)
        time_controllers = []
        attempt_id = 1
        while len(time_controllers) == 0:
            print('Attempt {} to time ...'.format(attempt_id))
            attempt_id += 1
            time_controllers = (
                driver
                .find_elements_by_xpath("//td[@class='yfnc_formbody1']")
                )
            time.sleep(2)
        sel_start = Select(time_controllers[0].find_element_by_id('selstart'))
        sel_start.select_by_visible_text(months[start_month])
        sel_end = Select(time_controllers[0].find_element_by_id('selend'))
        sel_end.select_by_visible_text(months[end_month])
        startday_field = time_controllers[0].find_element_by_id('startday')
        startday_field.clear()
        startday_field.send_keys(str(start_day))
        endday_field = time_controllers[0].find_element_by_id('endday')
        endday_field.clear()
        endday_field.send_keys(str(end_day))
        startyear_field = time_controllers[0].find_element_by_id('startyear')
        startyear_field.clear()
        startyear_field.send_keys(str(start_year))
        endyear_field = time_controllers[0].find_element_by_id('endyear')
        endyear_field.clear()
        endyear_field.send_keys(str(end_year))
        period_button = time_controllers[1].find_element_by_id(chosen_period)
        period_button.click()
        get_price_button = time_controllers[2].find_element_by_xpath('input')
        get_price_button.click()
        time.sleep(1)
        if operation == 'download':
            print('Download file ...')
            download_button = (
                driver
                .find_element_by_xpath(
                    "//table[@id='yfncsumtab']/tbody/tr[@valign='top']/td/p/a")
                )
            download_button.click()
            while not os.path.isfile('/Users/feizhan/Downloads/table.csv'):
                time.sleep(1)
            print('Rename the file ...')
            os.rename(
                '/Users/feizhan/Downloads/table.csv',
                ('/Users/feizhan/Downloads/'
                 + company_code+'_' + chosen_period + '.csv')
                )
        elif operation == 'scrape':
            print('Scrape table ...')
            prices = []
            attempt_id = 1
            while len(prices) == 0:
                print('Attempt {} to table ...'.format(attempt_id))
                attempt_id += 1
                prices = (
                    driver
                    .find_elements_by_xpath(
                        "//table[@class='yfnc_datamodoutline1']" +
                        "/tbody/tr/td/table/tbody" +
                        "/tr[position() > 1 and position()<last()]")
                    )
                time.sleep(2)
            tmp = map(lambda x: x.text, prices)
            tmp = pd.Series(list(tmp))
            tmp = tmp.str.replace(',', '')
            tmp = tmp.str.split()
            # remove the dividend row
            tmp = tmp.loc[(tmp.apply(len) > 3).values]
            tmp_dat = pd.DataFrame({})
            tmp_dat.loc[:, 'Date'] = (
                tmp.map(lambda x: x[2]) + '-' +
                tmp.map(lambda x: x[1]).map(num_months) + '-' +
                tmp.map(lambda x: x[0]).str.pad(2, fillchar='0'))
            tmp_dat.loc[:, 'Open'] = tmp.map(lambda x: x[3])
            tmp_dat.loc[:, 'High'] = tmp.map(lambda x: x[4])
            tmp_dat.loc[:, 'Low'] = tmp.map(lambda x: x[5])
            tmp_dat.loc[:, 'Close'] = tmp.map(lambda x: x[6])
            tmp_dat.loc[:, 'Volume'] = tmp.map(lambda x: x[7])
            tmp_dat.loc[:, 'Adj Close'] = tmp.map(lambda x: x[8])
            tmp_dat.loc[:, 'Company'] = company_code
            dat = pd.concat([dat, tmp_dat])
    if operation == 'scrape':
        driver.close()
        return dat
    driver.close()


def get_n_days_backwards(
        company_codes,
        back_days=1,
        end_year=today.year,
        end_month=today.month,
        end_day=today.day,
        chosen_period='daily',
        start_code_id=0,
        operation='scrape',
        business=True,
        ):
    if business:
        start_datetime = (
            pd.datetime(end_year, end_month, end_day) -
            back_days * BDay()
            )
    else:
        start_datetime = (
            pd.datetime(end_year, end_month, end_day) -
            back_days * DateOffset()
            )
    start_year = start_datetime.year
    start_month = start_datetime.month
    start_day = start_datetime.day
    if operation == 'scrape':
        dat = download_historical_share_prices(
            company_codes,
            start_year,
            start_month,
            start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            chosen_period=chosen_period,
            start_code_id=start_code_id,
            operation=operation,
            )
        return dat
    elif operation == 'download':
        download_historical_share_prices(
            company_codes,
            start_year,
            start_month,
            start_day,
            end_year=end_year,
            end_month=end_month,
            end_day=end_day,
            chosen_period=chosen_period,
            start_code_id=start_code_id,
            operation=operation,
            )


if __name__ == '__main__':
    # res = download_historical_share_prices(
    #     ['AMP', 'ANZ'],
    #     2016,
    #     2,
    #     25,
    #     end_year=today.year,
    #     end_month=today.month,
    #     end_day=today.day,
    #     chosen_period='daily',
    #     start_code_id=0,
    #     operation='download')
    asx = pd.read_excel('asx.xlsx')
    company_codes = asx.Code.tolist()
    res = get_n_days_backwards(
        company_codes[:20],
        back_days=10,
        end_year=today.year,
        end_month=today.month,
        end_day=today.day,
        chosen_period='daily',
        start_code_id=0,
        operation='scrape',
        business=True,
        )
