import pandas as pd
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
from ..processing.app import engine


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
def dashboard():
    return render_template('welcome.html')


@app.route('/show_table')
def show_table():
    return render_template('show_table.html')


@app.route('/query', methods=['POST'])
def query_db():
    with open('share_query.sql', 'r') as f:
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
