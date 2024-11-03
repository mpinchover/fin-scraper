import unittest
from alpaca.trading.client import TradingClient
from unittest.mock import Mock, MagicMock, create_autospec
from alpaca.trading.requests import MarketOrderRequest, OrderSide, TimeInForce
from trading.trading import TradingController

class TestTradingController(unittest.TestCase):
    def setUp(self):
        pass
    
    def test_get_buying_power_in_cents(self):
        # trading account
        mock_account = Mock()
        mock_account.buying_power = "1000"

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account

        tc = TradingController(trading_client=mock_trading_client)
        
        buying_power_in_cents = tc.get_buying_power_in_cents()
        self.assertEqual(buying_power_in_cents, 100000)  # 1000 * 100 = 100000 cents

    def test_get_buying_power_in_cents_no_buying_power(self):
        # trading account
        mock_account = Mock()
        mock_account.buying_power = None


        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account
        tc = TradingController(trading_client=mock_trading_client)

        with self.assertRaises(Exception) as context:
            buying_power_in_cents = tc.get_buying_power_in_cents()

        self.assertTrue("buying power not found" in str(context.exception))

    def test_get_buying_power_in_cents_no_acc(self):
        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = None
        tc = TradingController(trading_client=mock_trading_client)

        with self.assertRaises(Exception) as context:
            buying_power_in_cents = tc.get_buying_power_in_cents()

        self.assertTrue("account not found" in str(context.exception))

    def test_get_allocated_cents_per_stock(self):
        tc = TradingController(trading_client=None)
        allocation = tc.get_allocated_cents_per_stock(1000, 5)
        self.assertEqual(allocation, 200)

        allocation = tc.get_allocated_cents_per_stock(1000, 6)
        self.assertEqual(allocation, 166)

    def test_convert_dollar_to_cents_format(self):
        tc = TradingController(trading_client=None)
        usd_format = tc.convert_cents_to_dollar_format(2000)
        self.assertEqual(usd_format, "20.0")  

        usd_format = tc.convert_cents_to_dollar_format(2340914)
        self.assertEqual(usd_format, "23409.14")  

        usd_format = tc.convert_cents_to_dollar_format(0.33)
        self.assertEqual(usd_format, "0.0033")  

        usd_format = tc.convert_cents_to_dollar_format(33.333)
        self.assertEqual(usd_format[:6], "0.33333"[:6])  

    def test_build_orders(self):
        mock_account = Mock()
        mock_account.buying_power = 1000

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account
        tc = TradingController(trading_client=mock_trading_client)

        stocks_to_buy = ["AMZN", "WMT", "MSFT"]
        orders = tc.build_orders(stocks_to_buy)

        orders_made = set()

        self.assertEqual(3, len(orders))
        for order in orders:
            self.assertEqual(order.notional, 333.33)
            self.assertEqual(order.time_in_force, TimeInForce.DAY)
            self.assertEqual(order.side, OrderSide.BUY)
            self.assertTrue(order.symbol in stocks_to_buy)
            self.assertTrue(order.symbol not in orders_made)
            orders_made.add(order.symbol)

        
                               





