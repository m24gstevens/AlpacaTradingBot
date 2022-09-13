import requests
import datetime
import pandas as pd
import numpy as np

def day_rfcs(date):
    """Returns start and end times of a day as RFC 3339 formatted strings"""
    start_day = date.replace(hour=0, minute=0, second=0)
    end_day = date.replace(hour=23, minute=59,second=59)
    return start_day.isoformat()+'Z', end_day.isoformat()+'Z'

def yesterday_rfcs():
    """Returns start and end times of yesterday as RFC 3339 formatted strings"""
    return day_rfcs(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))

def time_ago(timedelta):
    dt = datetime.datetime.now(datetime.timezone.utc).astimezone() - timedelta
    return dt

def fifteen_ago():
    return time_ago(datetime.timedelta(minutes=16))

def get_data(endpoint, auth, symbols, timeframe, start, end, page_token=None):
    """
    Gets raw data for a number of symbols in a given timeframe.
    ---
    auth: dict of {'Apca-Api-Key-Id':... , 'Apca-Api-Secret-Key':...}
    symbols: comma separated string of symbols
    timeframe: String - One of "{x}Min", "{x}Hour", "1Day", "1Week"
    start, end - RFC-3339 dates for inclusive start and end
    """
    params = {
        'symbols': symbols,
        'timeframe': timeframe,
        'start': start,
        'end': end,
        'limit': 100
    }
    if page_token is not None:
        params['page_token']=page_token
    res = requests.get(endpoint, headers=auth, params=params)
    if not res.ok:
        print(res.status_code)
        raise Exception(res.json())

    return res.json()

def get_bars(endpoint, auth, symbols, timeframe, start, end, page_token=None):
    raw_json = get_data(endpoint, auth, symbols, timeframe, start, end, page_token)
    return raw_json.get('next_page_token'), raw_json['bars']

def last_minute_crypto_volume(auth, symbol, minute_end, counted=0, page_token=None):
    params = {
        'symbols': symbol,
        'start': (minute_end - datetime.timedelta(minutes=1)).isoformat() + 'Z',
        'end': minute_end.isoformat() + 'Z',
        'limit': 1000
    }
    if page_token is not None:
        params['page_token']=page_token
    res = requests.get('https://data.alpaca.markets/v1beta2/crypto/trades', headers=auth, params=params)
    if not res.ok:
        print(res.status_code)
        raise Exception(res.json())
    counted += sum(t['s'] for t in res.json()['trades'])
    page_token = res.json().get('next_page_token',None)
    if page_token is None:
        return counted
    return last_minute_crypto_volume(auth, symbol, minute_end, counted, page_token)



def collect_dfs(auth, endpoint, symbols, timeframe, start, end):
    """ Aggregates bar data into a dataframe """
    security_names = symbols.split(',')
    columns = ['t','o','c','h','l','v','vw','n']
    types = {'o':'float32','c':'float32', 'h':'float32','l':'float32','v':'int32','vw':'float32','n':'int32'}
    column_mappings = {'t':'time','o':'open','c':'close', 'h':'high','l':'low','v':'volume','vw':'volume_weighted_average','n':'num_orders'}
    dfs = {name:pd.DataFrame(columns=columns) for name in security_names}

    page_token = None

    while True:
        page_token, bars = get_bars(endpoint, auth, symbols, timeframe, start, end, page_token)
        for security in bars:
            dfs[security]=dfs[security].append(bars[security], ignore_index=True, sort=False)
        if page_token is None:
            break
    
    for name in security_names:
        dfs[name] = dfs[name].astype(types).rename(columns=column_mappings)
        dfs[name]['time'] = pd.to_datetime(dfs[name]['time'], format="%Y-%m-%dT%H:%M:%SZ").sort_values()
    return dfs

def day_stock_dfs(auth, symbols, timeframe,date):
    start, end = day_rfcs(date)
    return collect_dfs(auth,'https://data.alpaca.markets/v2/stocks/bars',symbols, timeframe, start, end)

def day_crypto_dfs(auth, symbols, timeframe, date):
    start, end = day_rfcs(date)
    return collect_dfs(auth,'https://data.alpaca.markets/v1beta2/crypto/bars',symbols, timeframe, start, end)

def yesterday_stock_dfs(auth, symbols, timeframe):
    return day_stock_dfs(auth, symbols, timeframe,datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1))


def recent_crypto_dfs(auth, symbols, timeframe, timedelta_back):
    end_dt = fifteen_ago()
    start_dt = end_dt - timedelta_back
    return collect_dfs(auth,'https://data.alpaca.markets/v1beta2/crypto/bars',symbols, timeframe, start_dt.isoformat(), end_dt.isoformat())

def recent_stock_dfs(auth, symbols, timeframe, timedelta_back):
    end_dt = fifteen_ago()
    start_dt = end_dt - timedelta_back
    return collect_dfs(auth,'https://data.alpaca.markets/v2/stocks/bars',symbols, timeframe, start_dt.isoformat(), end_dt.isoformat())

def get_calendar(auth,date):
    """
    dictionary with string keys of 'date', 'open', 'close', 'session_open', 'session_close'
    """
    if not isinstance(date, str):
        date = date.date().isoformat()
    print(auth)
    res = requests.get('https://paper-api.alpaca.markets/v2/calendar',headers=auth, params={'start':date, 'end':date})
    if not res.ok:
        raise Exception("Couldn't get the calendar", res.json())
    return res.json()[0]

MY_AUTH = {'Apca-Api-Key-Id':'PKE1Z9UEDU05UN64RRFI', 'Apca-Api-Secret-Key':'LoVBhjlwYavuxXuaU8N7zSXWMVpEoLR7pFWFdepQ'}
#nothing = day_stock_dfs(MY_AUTH,"AAPL","5Min",datetime.datetime.fromisoformat("2022-08-21"))
nothing = recent_stock_dfs(MY_AUTH,"AAPL","5Min",datetime.timedelta(hours=8))