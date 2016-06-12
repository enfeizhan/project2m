import glob
import pandas as pd
filenames = glob.glob('/Users/feizhan/Dropbox/Project2M/ASXDividendHistory/*.csv')
dat = pd.DataFrame({})
for filename in filenames:
    tmp = pd.read_csv(filename, index_col=[0])
    tmp.loc[:, 'code'] = filename[-10:-7]
    dat = dat.append(tmp)
dat.to_csv('dividend_consolidation20160320.csv')
