import pandas as pd
import glob
db_url = './'
filenames = glob.glob(db_url + '*.csv')
dat = pd.DataFrame({})
tot = len(filenames)
for ind, filename in enumerate(filenames):
    print('{ind}/{tot}'.format(ind=ind, tot=tot))
    code = filename[2:-4]
    tmp = pd.read_csv(filename)
    tmp.loc[:, 'code'] = code
    # columns = tmp.columns.tolist()
    # multi_cols = pd.MultiIndex.from_product(
    #     [[code], columns],
    #     names=['code', 'price type']
    # )
    # tmp.columns = multi_cols
    dat = pd.concat([dat, tmp])
dat.loc[:, 'Date'] = pd.to_datetime(dat.loc[:, 'Date'])
oldest_year = dat.Date.min().year
recent_year = dat.Date.max().year
dat = dat.sort_values(['code', 'Date'])
dat = dat.set_index('Date')
dat = dat.reindex(
    columns=[
        'code',
        'Open',
        'High',
        'Low',
        'Close',
        'Volume',
        'Adj Close'
    ]
)
print('Consolidation complete!')
for year in range(oldest_year, recent_year+1):
    tmp = dat.loc[str(year)+'-01-01':str(year)+'-12-31']
    tmp.to_csv(str(year)+'sector_price.csv')
