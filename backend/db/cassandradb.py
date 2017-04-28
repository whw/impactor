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
    modified = columns.DateTime()
    year = columns.Integer(primary_key=True)
    ts = columns.DateTime(primary_key=True, clustering_order='DESC')
    usage = columns.Float()
    total_demand = columns.Float()
    soc = columns.Float()
    output = columns.Float()
    regd_ts = columns.DateTime()

    access_name = 'usage'


def get_model(table_name):
    if table_name == 'usage':
        return UsageModel
    else:
        print('Invalid table: ' + table_name)
        raise


class CassandraDB(BaseDB):

    def __init__(self):
        cassandra_ip = os.getenv('T_CASSANDRA_IP', 'localhost')
        self.session = connection.setup([cassandra_ip], "ops")
        create_keyspace_simple("ops", 1)

    def create_table(self, table_name):
        model = get_model(table_name)
        sync_table(model)

    def delete_table(self, table_name):
        model = get_model(table_name)
        drop_table(model)

    def count_items(self, table_name):
        model = get_model(table_name)
        return model.objects.count()

    def scan_table(self, table_name):
        model = get_model(table_name)
        items = []
        for datapoint in model.objects().all():
            items.append(datapoint.items())

        return items

    def write_item(self, data, table_name):
        model = get_model(table_name)
        device_id = data.pop('device_id')

        now = datetime.now()

        model.create(
            modified=now,
            year=now.year,
            ts=data['ts'],
            usage=data['usage'],
            total_demand=data['billing_demand'],
            soc=data['soc'],
            output=data['output'],
            regd_ts=data['regd_ts'],
        ).save()
