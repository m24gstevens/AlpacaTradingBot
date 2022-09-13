import sys
sys.path.append("..")

import alpacautil
import datetime
import pandas as pd
import statsmodels.tsa.stattools as ts

def find_pairs(auth):
    sp500 = pd.read_csv('sp500.csv')
    print("asset1,asset2,pval")
    sectors = sp500['Sector'].unique()
    for sector in sectors:
        assets = list(sp500[sp500['Sector'] == sector]['Symbol'])
        by_sector(auth,assets)

def by_sector(auth,assets):
    dfs = alpacautil.recent_stock_dfs(auth,','.join(assets), "8Hours", datetime.timedelta(15*30))
    for i in range(len(assets)):
        for j in range(i+1, len(assets)):
            p_val = coint_test(dfs[assets[i]], dfs[assets[j]])
            print(f"{assets[i]},{assets[j]},{p_val}")


def coint_test(df1,df2):
    combined = pd.merge(df1, df2, how="inner",on=['time'],
    suffixes=('_1','_2'))
    combined = combined[['time','close_1','close_2']]
    test_result = ts.coint(combined['close_1'], combined['close_2'])
    return str(test_result[1])
    

MY_AUTH = {'Apca-Api-Key-Id':'PKE1Z9UEDU05UN64RRFI', 'Apca-Api-Secret-Key':'LoVBhjlwYavuxXuaU8N7zSXWMVpEoLR7pFWFdepQ'}
find_pairs(MY_AUTH)