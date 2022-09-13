TRADING_PAPER = "https://paper-api.alpaca.markets"
TRADING_LIVE = "https://api.alpaca.markets"   
    
position_datatypes={
  "asset_id": "string",
  "symbol":"string",
  "exchange": "string",
  "asset_class": "string",
  "avg_entry_price": "float32",
  "qty": "float32",
  "qty_available": "float32",
  "side": "string",
  "market_value": "float32",
  "cost_basis": "float32",
  "unrealized_pl": "float32",
  "unrealized_plpc": "float32",
  "unrealized_intraday_pl": "float32",
  "unrealized_intraday_plpc": "float32",
  "current_price": "float32",
  "lastday_price": "float32",
  "change_today": "float32"
}

order_datatypes={
"id": "string",
  "client_order_id": "string",
  "created_at": "datetime64[ns]",
  "updated_at": "datetime64[ns]",
  "submitted_at": "datetime64[ns]",
  "filled_at": "datetime64[ns]",
  "expired_at": "datetime64[ns]",
  "canceled_at": "datetime64[ns]",
  "failed_at": "datetime64[ns]",
"replaced_at": "datetime64[ns]",
  "replaced_by": "string",
  "replaces": "string",
  "asset_id": "string",
  "symbol": "string",
  "asset_class": "string",
  "notional": "float32",
  "qty": "float32",
  "filled_qty": "int32",
  "filled_avg_price": "float32",
  "order_class": "string",
  "order_type": "string",
  "type": "string",
  "side": "string",
  "time_in_force": "string",
  "limit_price": "float32",
  "stop_price": "float32",
  "status": "string",
  "extended_hours": "boolean",
  "legs": "object",
  "trail_percent": "float32",
  "trail_price": "float32",
  "hwm": "float32"
}

MY_AUTH = {'Apca-Api-Key-Id':'PKE1Z9UEDU05UN64RRFI', 'Apca-Api-Secret-Key':'LoVBhjlwYavuxXuaU8N7zSXWMVpEoLR7pFWFdepQ'}
