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
        mock_account.buying_power = "30000"

        mock_logger = Mock()

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account

        tc = TradingController(trading_client=mock_trading_client, logger=mock_logger)
        
        safeguard = 25000
        buying_power_in_cents = tc.get_buying_power_in_cents(safeguard)
        self.assertEqual(buying_power_in_cents, 500000)  # 1000 * 100 = 100000 cents

    def test_get_buying_power_in_cents(self):
        # trading account
        mock_account = Mock()
        mock_account.buying_power = "30000"

        mock_logger = Mock()

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account

        tc = TradingController(trading_client=mock_trading_client, logger=mock_logger)
        
        safeguard = 1000
        with self.assertRaises(Exception) as context:
            buying_power_in_cents = tc.get_buying_power_in_cents(safeguard)

        self.assertTrue("buying power too high" in str(context.exception))

    def test_get_buying_power_in_cents_no_buying_power(self):
        # trading account
        mock_account = Mock()
        mock_account.buying_power = None

        mock_logger = Mock()

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account
        tc = TradingController(trading_client=mock_trading_client, logger=mock_logger)

        with self.assertRaises(Exception) as context:
            buying_power_in_cents = tc.get_buying_power_in_cents(10)

        self.assertTrue("buying power not found" in str(context.exception))

    def test_get_buying_power_in_cents_no_acc(self):
        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = None
        mock_logger = Mock()
        
        tc = TradingController(trading_client=mock_trading_client, logger=mock_logger)

        with self.assertRaises(Exception) as context:
            buying_power_in_cents = tc.get_buying_power_in_cents(10)

        self.assertTrue("account not found" in str(context.exception))


    def test_get_allocated_cents_per_stock(self):
        mock_logger = Mock()
        tc = TradingController(trading_client=None, logger=mock_logger)
        allocation = tc.get_allocated_cents_per_stock(1000, 5)
        self.assertEqual(allocation, 200)

        allocation = tc.get_allocated_cents_per_stock(1000, 6)
        self.assertEqual(allocation, 166)

    def test_convert_dollar_to_cents_format(self):
        mock_logger = Mock()
        tc = TradingController(trading_client=None, logger=mock_logger)
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
        mock_account.buying_power = "29555"
        mock_logger = Mock()

        # trading client
        mock_trading_client = create_autospec(TradingClient)
        mock_trading_client.get_account.return_value = mock_account
        tc = TradingController(trading_client=mock_trading_client, logger=mock_logger)

        stocks_to_buy = ["AMZN", "WMT", "MSFT"]
        orders = tc.build_orders(stocks_to_buy)

        orders_made = set()

        self.assertEqual(3, len(orders))
        for order in orders:
            self.assertEqual(order.notional, 1518.33)
            self.assertEqual(order.time_in_force, TimeInForce.DAY)
            self.assertEqual(order.side, OrderSide.BUY)
            self.assertTrue(order.symbol in stocks_to_buy)
            self.assertTrue(order.symbol not in orders_made)
            orders_made.add(order.symbol)

        
                               





