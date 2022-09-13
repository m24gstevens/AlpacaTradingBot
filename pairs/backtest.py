import sys
sys.path.append("..")

import datetime
import alpacautil
import pandas as pd
import numpy as np

class BackTester:
    def __init__(self, api_key, secret_key):
        self._auth = {'Apca-Api-Key-Id':api_key, 'Apca-Api-Secret-Key':secret_key}

    def _iso_date(self,date):
        if isinstance(date,str):
            date = datetime.datetime.fromisoformat(date)
        return date

    def get_data(self,symb1, symb2, backdays):
        try:
            dfs = alpacautil.recent_stock_dfs(self._auth,f"{symb1},{symb2}",
                                           "1Day", datetime.timedelta(backdays))
        except Exception as e:
            raise Exception("Error getting stock data:",str(e))
        return dfs[symb1], dfs[symb2]

    def z_scores(self, df1, df2):
        """ Backtests the pairs trading strategy based on the close prices. """
        merged = pd.merge(df1, df2, how="inner",on=['time'],
            suffixes=('_1','_2'))
        closes = merged[["time","close_1","close_2"]]
        start_date = closes.iloc[0,0].date()
        closes.insert(0,'day',[(date.date() - start_date).days for date in closes['time']])
        closes.insert(4,'ratio', closes['close_1'].div(closes['close_2']))
        z_scores = []
        for index, row in closes.iterrows():
            day = row['day']
            if day < 50:
                z_scores.append(np.nan)
                continue
            last_50 = closes[(closes['day'] > day - 50) & (closes['day'] < day)]
            mean_ratio = last_50['ratio'].mean()
            stddev = last_50['ratio'].std()
            z_score = (row['ratio'] - mean_ratio)/stddev
            z_scores.append(z_score)
        closes.insert(5,'z_score',z_scores)
        return closes

    def pairs_test(self, df1, df2):
        summary = self.z_scores(df1, df2)
        equity = 100000
        asset1_holdings = 0
        asset2_holdings = 0
        last_bought = None
        for index, row in summary.iterrows():
            z_score = row['z_score']
            if last_bought is None:
                asset1_potential = (equity / 2) / row['close_1']
                asset2_potential = (equity / 2) / row['close_2']
                if z_score > 2 or z_score < -2:
                    multiplier = (1 if z_score < -2 else -1)
                    last_bought = row['day']
                    asset1_holdings = asset1_potential * multiplier
                    asset2_holdings = asset2_potential * -multiplier
            else:
                multiplier = (1 if asset1_holdings > 0 else -1)
                if (z_score * multiplier) > 0 or row['day'] - 50 > last_bought:
                    equity += asset1_holdings * row['close_1']
                    equity += asset2_holdings * row['close_2']
                    last_bought = None

        if last_bought is not None:
            equity += asset1_holdings * summary['close_1'].iat[-1]
            equity += asset2_holdings * summary['close_2'].iat[-1]

        return (equity - 100000)/100000

    def backtest(self):
        df1, df2 = self.get_data(1000)
        return self.pairs_test(df1,df2)


    def backtest_pairs(self):
        sp500 = pd.read_csv('sp500.csv')
        all_assets = list(sp500['Symbol'])
        dfs = {}
        for i in range(0, len(all_assets),25):
            assets = ','.join(all_assets[i:i+25])
            results = alpacautil.recent_stock_dfs(self._auth,assets, "1Day", datetime.timedelta(1000))
            dfs.update(results)
        
        pairs = pd.read_csv('pairs.csv')
        good_pairs = pairs[pairs['pval'] >= 0.80]
        print('asset1','asset2','return')
        for _, row in good_pairs.iterrows():
            roi = self.pairs_test(dfs[row['asset1']],dfs[row['asset2']])
            print(f"{row['asset1']},{row['asset2']},{roi}")
    

bt = BackTester("AAPL","MSFT", 'PKE1Z9UEDU05UN64RRFI', 'LoVBhjlwYavuxXuaU8N7zSXWMVpEoLR7pFWFdepQ')
bt.backtest_pairs()
