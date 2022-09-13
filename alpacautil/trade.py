import json
import requests
import pandas as pd
from .consts import (TRADING_PAPER, TRADING_LIVE, position_datatypes, order_datatypes)


class TradingClient:

    def __init__(self, base_url, api_key, secret_key):
        self._base_url = base_url + '/v2'
        self._auth = {'Apca-Api-Key-Id':api_key, 'Apca-Api-Secret-Key':secret_key}
        self._session = requests.Session()
        self._positions = None
        self._orders = None
        self._init_positions()
        self._init_orders()
        self._get_account()

    def _get_account(self):
        res = self._session.get(self._base_url + f'/account', headers=self._auth)
        if not res.ok:
            raise Exception("Couldn't get account information")
        self.account = res.json()

    def _init_table(self,urlextension,index,tablename,datatypes):
        res = self._session.get(self._base_url + f'/{urlextension}', headers=self._auth)
        if not res.ok:
            raise Exception("Couldn't get positions")
        df = pd.DataFrame(columns=list(datatypes.keys()))
        df = df.astype(datatypes)
        df.set_index(index, inplace=True)
        df.append(res.json(), ignore_index=False,sort=False)

        print(df.columns)

        self.__dict__[tablename] = df

    def _init_positions(self):
        self._init_table('positions','symbol','_positions',position_datatypes)

    def _init_orders(self):
        self._init_table('orders','id','_orders', order_datatypes)


    def _update_position_in_table(self, symbol):
        res = self._session.get(self._base_url + f'/positions/{symbol}', headers=self._auth)
        if not res.ok:
            raise Exception("Couldn't get position with symbol,", symbol)
        self._positions.loc[(self._positions['symbol'] == symbol)] = res.json()

    def _remove_position_from_table(self, symbol):
        self._positions.drop(self._positions[self._positions['symbol'] == symbol].index, inplace=True)

    def close_position(self, symbol,qty=None,percentage=None):
        params = {}
        if qty is not None:
            params["qty"]=qty
        if percentage is not None:
            params["percentage"]=percentage

        if params:
            res = self._session.delete(self._base_url + f'/positions/{symbol}', headers = self._auth,
            params=params)
        else:
            res = self._session.delete(self._base_url + f'/positions/{symbol}', headers = self._auth)

        if not res.ok:
            raise Exception("Failed to close position with symbol,",symbol)

        if params:
            self._update_position_in_table(symbol)
        else:
            self._remove_position_from_table(symbol)

        self._get_account()

    def close_all_positions(self,cancel_orders=False):
        res = self._session.delete(self._base_url + '/positions', headers=self._auth, params={'cancel_orders':cancel_orders})
        if not res.ok:
            raise Exception("Failed to liquidate all positions")
        self._positions.drop(self._positions.index, inplace=True)
        self._get_account()

    def update_order(self,order_id,update_dict):
        res = self._session.patch(self._base_url + f'/orders/{order_id}', headers={'Content-Type':'application/json', **self._auth}, json=update_dict)
        if not res.ok:
            raise Exception("Couldn't update order with ID,",order_id)
        self._orders.loc[order_id] = self._orders.loc[order_id].replace(update_dict)

    def get_order(self,order_id):
        res = self._session.get(self._base_url + f'/orders/{order_id}',headers=self._auth)
        if not res.ok:
            print("Couldn't get order")
        return res.json()

    def cancel_order(self,order_id):
        res = self._session.delete(self._base_url + f'/orders/{order_id}', headers=self._auth)
        if not res.ok:
            if not res.status_code == 422:
                raise Exception("Couldn't delete order with ID,",order_id)
            print("Order with ID ", order_id, " already filled")
        self._orders.drop(self._orders[self._orders['order_id'] == order_id].index,inplace=True)
        self._get_account()

    def cancel_all_orders(self):
        res = self._session.delete(self._base_url + f'/orders', headers=self._auth)
        # High change of a 500 due to a filled order, so fail soft and just reinitialize order table
        self._init_orders()
        self._get_account()

    def request_order(self,order_body):
        """ The order body should be a dictionary with this 'schema':
        symbol - string
        qty - int, can be float for 'day' and 'market' orders
        notional - float (Can't work with qty. Only for 'day' and 'market' orders)
        size - string ('buy' or 'sell')
        type - string ('market', 'limit', 'stop', 'stop_limit' or 'trailing_stop')

        limit_price - float (required on limit-type orders)
        stop_price - float (for stop-type orders)
        trail_price - float (on trailing stop)
        trail_percent - float (alternative to price)
        """
        res = self._session.post(self._base_url + f'/orders', headers={'Content-Type':'application/json', **self._auth}, json=order_body)
        if not res.ok:
            raise Exception("Couldn't create the order with order body,",order_body)
        self._orders.append(res.json(), ignore_index=False,sort=False)
        self._get_account()



