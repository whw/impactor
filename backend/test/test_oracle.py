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
        orders1 = oracle.handler({"status": -1}, None)
        self.assertEqual('{"orders": 0}', orders1)
        self.assertEqual(1, db.number_of_items_in_table())

        orders2 = oracle.handler({"status": 0}, None)
        self.assertEqual('{"orders": 1}', orders2)
        self.assertEqual(2, db.number_of_items_in_table())

        orders3 = oracle.handler({"status": 1}, None)
        self.assertEqual('{"orders": -1}', orders3)
        self.assertEqual(3, db.number_of_items_in_table())

    @classmethod
    def tearDownClass(cls):
        db.delete_table()


if __name__ == '__main__':
    unittest.main()
