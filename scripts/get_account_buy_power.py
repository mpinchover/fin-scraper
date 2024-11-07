from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, OrderSide, TimeInForce

# you need to eb agle to get all the money in the count
# paper=True enables paper trading
trading_client = TradingClient('', '', paper=True)
acc = trading_client.get_account()
print("Account is ", acc.cash)

safeguard = 25000
buying_power_as_int = float(acc.cash) 
buying_power_with_safeguard = buying_power_as_int - safeguard
buying_power_in_cents = int(buying_power_with_safeguard * 100)
print("Buying power in cents w safeguard", buying_power_in_cents)
