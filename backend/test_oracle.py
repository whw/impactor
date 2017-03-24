import unittest
import oracle


class TestOracle(unittest.TestCase):

    def test_orders(self):
        self.assertEqual(oracle.handler(
            {"status": -1}, None), '{"orders": 0}')
        self.assertEqual(oracle.handler(
            {"status": 0}, None), '{"orders": 1}')
        self.assertEqual(oracle.handler(
            {"status": 1}, None), '{"orders": -1}')

if __name__ == '__main__':
    unittest.main()
