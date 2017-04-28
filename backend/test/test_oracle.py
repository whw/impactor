import json
import time
import unittest

from backend import oracle, data
from backend.db import db

db = db.get_db()


class TestOracle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        db.create_table()

    def test_orders(self):
        orders1 = oracle.handler(
            data._build_tumalow_packet(-1, 10.0, time.time()), None)
        self.assertEqual(0, json.loads(orders1)['power'])
        self.assertEqual(1, db.number_of_items_in_table())

        orders2 = oracle.handler(
            data._build_tumalow_packet(0, 10.0, time.time()), None)
        self.assertEqual(1, json.loads(orders2)['power'])
        self.assertEqual(2, db.number_of_items_in_table())

        orders3 = oracle.handler(
            data._build_tumalow_packet(1, 10.0, time.time()), None)
        self.assertEqual(-1, json.loads(orders3)['power'])
        self.assertEqual(3, db.number_of_items_in_table())

    @classmethod
    def tearDownClass(cls):
        db.delete_table()


if __name__ == '__main__':
    unittest.main()
