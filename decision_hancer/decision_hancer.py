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
from utils.utils import engine
from utils.utils import ASXTradingCalendar
from utils.utils import getListQuoted
from utils.utils import getCommaSeparatedItemsQuoted

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


@app.route('/about_us')
def about_us():
    return render_template('home.html')


@app.route('/custom_time_window')
def custom_time_window():
    return render_template('custom_time_window.html')


@app.route('/custom_time_window_query', methods=['POST'])
def custom_time_window_query():
    with open('sql_templates/share_query.sql', 'r') as f:
        query_template = f.read()
    codes = request.form['codes']
    if codes == '':
        with open('sql_templates/share_code_query.sql', 'r') as f:
            share_code_query = f.read()
        codes = pd.read_sql(share_code_query, engine)
        codes = codes.loc[:, 'share_codes'].tolist()
        codes = getListQuoted(codes)
    else:
        codes = getCommaSeparatedItemsQuoted(codes)
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    query = query_template.format(
        codes=codes,
        start_date=start_date,
        end_date=end_date
    )
    res = pd.read_sql(query, engine)
    res = res.sort_values(by=['code', 'date'])
    if 'show_table' in request.form.keys():
        return render_template(
            'custom_time_window.html',
            res_table=res.to_html(index=False)
        )
    elif 'download' in request.form.keys():
        response = make_response(res.to_csv(index=False))
        response.headers["Content-Disposition"] = (
            "attachment; filename=result.csv"
        )
        return response


@app.route('/quick_recent_price')
def quick_recent_price():
    return render_template('quick_recent_price.html')


@app.route('/quick_recent_price_query', methods=['POST'])
def quick_recent_price_query():
    with open('sql_templates/share_query.sql', 'r') as f:
        query_template = f.read()
    last_date_query = '''
        select max("date") as max_date from share_price;
    '''
    last_date = pd.read_sql(last_date_query, engine).iloc[0, 0]
    last_business_day = last_date - 64 * asx_dayoffset
    start_date = last_business_day.strftime('%Y%m%d')
    end_date = last_date.strftime('%Y%m%d')
    codes = request.form['codes']
    if codes == '':
        with open('sql_templates/share_code_query.sql', 'r') as f:
            share_code_query = f.read()
        codes = pd.read_sql(share_code_query, engine)
        codes = codes.loc[:, 'share_codes'].tolist()
        codes = getListQuoted(codes)
    else:
        codes = getCommaSeparatedItemsQuoted(codes)
    query = query_template.format(
        codes=codes,
        start_date=start_date,
        end_date=end_date
    )
    res = pd.read_sql(query, engine)
    res = res.sort_values(by=['code', 'date'])
    if 'show_table' in request.form.keys():
        return render_template(
            'quick_recent_price.html',
            res_table=res.to_html(index=False)
        )
    elif 'download' in request.form.keys():
        response = make_response(res.to_csv(index=False))
        response.headers["Content-Disposition"] = (
            "attachment; filename=result.csv"
        )
        return response
