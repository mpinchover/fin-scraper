from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, OrderSide, TimeInForce

SAFEGUARD = 25000
class TradingController:
    # pass in trading client
    def __init__(self, trading_client: TradingClient, logger):
        self.trading_client = trading_client
        self.logger = logger

    def sell_shares(self):
        self.trading_client.close_all_positions(cancel_orders=True)

    # return buying power as an int
    def get_buying_power_in_cents(self, safeguard):
        acc = self.trading_client.get_account()
        if not acc:
            raise Exception("account not found")
        if not acc.cash:
            raise Exception("account cash not found")
        
        cash_as_int = float(acc.cash) 
        cash_with_safeguard = cash_as_int - safeguard

        if cash_with_safeguard > 0 and cash_with_safeguard <= 7000:
            cash_in_cents_with_safeguard = int(cash_with_safeguard * 100)
            self.logger.info(f"cash power safeguard is {cash_in_cents_with_safeguard}")
            return cash_in_cents_with_safeguard
        else:
            self.logger.info(f"Buying power with safeguard is too high: {cash_with_safeguard}")
            raise Exception("buying power too high")
            

    def get_allocated_cents_per_stock(self, buy_power, num_stocks):
        return buy_power // num_stocks

    def convert_cents_to_dollar_format(self, cents_per_stock):
        return str(cents_per_stock / 100)

    def build_orders(self, stocks_to_buy):
        if not stocks_to_buy:
            # and log here
            self.logger.info("No stocks provided to build market orders")
            return 
        
        buy_power_cents = self.get_buying_power_in_cents(SAFEGUARD)
        self.logger.info(f"buy power in cents is {buy_power_cents}")
        cents_per_stock = self.get_allocated_cents_per_stock(buy_power_cents, len(stocks_to_buy))
        self.logger.info(f"cents per stock is {cents_per_stock}")
        dollars_per_stock = self.convert_cents_to_dollar_format(cents_per_stock)
        self.logger.info(f"dollars per stock is {dollars_per_stock}")
        
        orders = []
        for stock in stocks_to_buy:
            market_order_data = MarketOrderRequest(
                        symbol=stock.upper(),
                        side=OrderSide.BUY,
                        time_in_force=TimeInForce.DAY,
                        notional=dollars_per_stock,
                    )
            orders.append(market_order_data)
        return orders

    def submit_orders(self, orders):
        if not orders:
            self.logger.info("No orders provided to submit market orders")
            return 
        
        self.logger.info(f"Attempting to submit {len(orders)} market orders...")
        for order in orders:
            market_order = self.trading_client.submit_order(
                order_data=order
               )
        self.logger.info("Submitted market orders")
        # self.logger.u("Made market order ", market_order)

