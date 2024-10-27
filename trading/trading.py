from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest

# you need to eb agle to get all the money in the count
# paper=True enables paper trading
trading_client = TradingClient('api-key', 'secret-key', paper=True)
# preparing orders
market_order_data = MarketOrderRequest(
                    symbol="SPY",
                    qty=0.023,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                    )
# Market order
market_order = trading_client.submit_order(
                order_data=market_order_data
               )


"""
# params to filter orders by
request_params = GetOrdersRequest(
                    status=QueryOrderStatus.OPEN,
                    side=OrderSide.SELL
                 )
"""