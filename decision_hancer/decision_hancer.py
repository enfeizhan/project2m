import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from flask import Flask
from flask import request
from flask import make_response
# from flask import session
# from flask import g
# from flask import redirect
# from flask import url_for
# from flask import abort
from flask import render_template
# from flask import flash
from .app import engine
from .utils import ASXTradingCalendar

asx_trading_calendar = ASXTradingCalendar()
asx_dayoffset = CustomBusinessDay(calendar=asx_trading_calendar)


# create our little application :)
app = Flask(__name__)

# Load default config and override config from an environment variable
app.config.update(
    dict(
        DEBUG=True,
    )
)


# @app.teardown_appcontext
# def close_db(error):
#     """Closes the database again at the end of the request."""
#     if hasattr(g, 'sqlite_db'):
#         g.sqlite_db.close()


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/show_table')
def show_table():
    return render_template('show_table.html')


@app.route('/query', methods=['POST'])
def query_db():
    with open('sql_templates/share_query.sql', 'r') as f:
        query_template = f.read()
    codes = request.form['codes']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    codes = codes.replace(',', "','")
    codes = "'" + codes + "'"
    query = query_template.format(
        codes=codes,
        start_date=start_date,
        end_date=end_date
    )
    res = pd.read_sql(query, engine)
    if 'show_table' in request.form.keys():
        return render_template(
            'show_table.html',
            res_table=res.to_html(index=False)
        )
    elif 'download' in request.form.keys():
        response = make_response(res.to_csv(index=False))
        response.headers["Content-Disposition"] = (
            "attachment; filename=result.csv"
        )
        return response


@app.route('/last_11_business')
def last_11_business():
    return render_template('last_11_business.html')


@app.route('/download_last_11_business', methods=['POST'])
def download_last_11_business():
    with open('sql_templates/share_query.sql', 'r') as f:
        query_template = f.read()
    last_date_query = '''
        select max("date") as max_date from share_price;
    '''
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]
    last_business_day = last_date - 10 * asx_dayoffset
    start_date = last_business_day.strftime('%Y%m%d')
    end_date = last_date.strftime('%Y%m%d')
    codes = request.form['codes']
    if codes == '':
        codes = pd.read_csv(
            'http://www.asx.com.au/asx/research/ASXListedCompanies.csv',
            skiprows=1
        )
        codes = (codes.loc[:, 'ASX code'] + '.AX').tolist()
        codes = '\',\''.join(codes)
    else:
        codes = codes.replace(',', "','")
    codes = "'" + codes + "'"
    query = query_template.format(
        codes=codes,
        start_date=start_date,
        end_date=end_date
    )
    res = pd.read_sql(query, engine)
    res = res.sort_values(by=['code', 'date'])
    if 'show_table' in request.form.keys():
        return render_template(
            'last_11_business.html',
            res_table=res.to_html(index=False)
        )
    elif 'download' in request.form.keys():
        response = make_response(res.to_csv(index=False))
        response.headers["Content-Disposition"] = (
            "attachment; filename=result.csv"
        )
        return response
