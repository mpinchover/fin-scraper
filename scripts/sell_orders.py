from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, OrderSide, TimeInForce

# you need to eb agle to get all the money in the count
# paper=True enables paper trading
trading_client = TradingClient('', '', paper=False)
# acc = trading_client.get_account()
trading_client.close_all_positions(cancel_orders=True)