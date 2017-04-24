from datetime import datetime
import json
import os
from cassandra.cqlengine import connection, columns
from cassandra.cqlengine.management import create_keyspace_simple, drop_table, sync_table
from cassandra.cqlengine.models import Model

from basedb import BaseDB

# NOTE(whw): This fixes a warning.
os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "true"


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

    def __init__(self):
        self.session = connection.setup(['localhost'], "ops")

    def create_table(self):
        create_keyspace_simple("ops", 1)
        sync_table(UsageModel)

    def delete_table(self):
        drop_table(UsageModel)

    def number_of_items_in_table(self):
        return UsageModel.objects.count()

    def get_config(self):
        return "Cassandra"

    def scan_table(self):
        items = []
        for datapoint in UsageModel.objects().all():
            items.append(datapoint.items())

        return items

    def write_item(self, data):
        device_id = data.keys()[0]
        raw_data = data[device_id]

        now = datetime.now()

        UsageModel.create(
            modified=now,
            year=now.year,
            ts=raw_data['ts'],
            usage=raw_data['usage'],
            total_demand=raw_data['billing_demand'],
            soc=raw_data['soc'],
            output=raw_data['output'],
            regd_ts=raw_data['regd_ts'],
        ).save()
