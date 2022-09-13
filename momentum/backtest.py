import sys
sys.path.append("..")

import datetime
import alpacautil
import pandas as pd
import numpy as np

class BackTester:
    def __init__(self, api_key, secret_key, asset1, asset2):
        self._auth = {'Apca-Api-Key-Id':api_key, 'Apca-Api-Secret-Key':secret_key}
        self._fee = 0.0025
        self._asset1 = asset1
        self._asset2 = asset2

    def day_data(self,date):
        try:
            dfs = alpacautil.day_crypto_dfs(self._auth, f'{self._asset1},{self._asset2}',"1Min",date)
        except Exception as e:
            raise Exception("Error getting stock data:",str(e))
        return dfs[self._asset1], dfs[self._asset2]

    def recent_summary(self, asset, backdays):
        dfs = alpacautil.recent_crypto_dfs(self._auth,asset,"1Day",
                                             datetime.timedelta(backdays))
        df = dfs[asset]

        mavgs = []
        twoday_price_increase = []
        start_date = dfs.iloc[0]['time']
        for index, row in df.iterrows():
            rowtime = row['time']
            if (row['time'] - start_date).days < 30 or index < 2:
                mavgs.append(np.nan)
                twoday_price_increase.append(np.nan)
            short_term = df[((rowtime - df['time']).days < 25) & ((rowtime - df['time']).days > 0)]
            mavgs.append(short_term['close'].mean())
            price_increase = (df.at[index-1,"close"] - df.at[index-2,"open"])/df.at[index-2,"open"]
            twoday_price_increase.append(price_increase)
        df.insert(1, "short_term_mavg", mavgs)
        df.insert(2, "twoday_price_increase", twoday_price_increase)
        return df



""" Trading strategy: """
    

