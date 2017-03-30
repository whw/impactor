import json
import unittest

from backend import oracle
from backend.db import db

db = db.get_db()


class TestOracle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(db.get_config())
        db.create_table()

    def test_orders(self):
        orders1 = oracle.handler(oracle._build_status(-1), None)
        self.assertEqual(0, json.loads(orders1)['power'])
        self.assertEqual(1, db.number_of_items_in_table())
        self.assertEqual('', db.scan_table())

        orders2 = oracle.handler(oracle._build_status(0), None)
        self.assertEqual(1, json.loads(orders2)['power'])
        self.assertEqual(2, db.number_of_items_in_table())
        self.assertEqual('', db.scan_table())

        orders3 = oracle.handler(oracle._build_status(1), None)
        self.assertEqual(-1, json.loads(orders3)['power'])
        self.assertEqual(3, db.number_of_items_in_table())
        self.assertEqual('', db.scan_table())

    @classmethod
    def tearDownClass(cls):
        db.delete_table()


if __name__ == '__main__':
    unittest.main()
