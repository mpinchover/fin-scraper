import unittest

class SimpleWidgetTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_assert_something(self):
        self.assertEqual(50, 50)