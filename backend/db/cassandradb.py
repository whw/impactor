import json
import os
from cassandra.cqlengine import connection, columns
from cassandra.cqlengine.management import create_keyspace_simple, sync_table
from cassandra.cqlengine.models import Model

from basedb import BaseDB


class UsageModel(Model):
    # read_repair_chance = 0.05  # optional - defaults to 0.1
    modified = columns.DateTime()
    year = columns.Integer(primary_key=True)
    ts = columns.DateTime(primary_key=True, clustering_order='DESC')
    usage = columns.Float()
    total_demand = columns.Float()
    soc = columns.Float()
    output = columns.Float()
    regd_ts = columns.DateTime()

    access_name = 'usage'


class CassandraDB(BaseDB):

    def get_config(self):
        return "Cassandra"

    def create_table(self):
        connection.setup(['localhost'], "ops")
        create_keyspace_simple("ops", 1)
        sync_table(UsageModel)

    def number_of_items_in_table(self):
        connection.setup(['localhost'], "ops")

        return UsageModel.objects.count()

    def delete_table(self):
        False

    def scan_table(self):
        connection.setup(['localhost'], "ops")

        return UsageModel.objects.count()
