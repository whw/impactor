from datetime import datetime
import json
import os
from cassandra.cqlengine import connection, columns
from cassandra.cqlengine.management import create_keyspace_simple, drop_table, sync_table
from cassandra.cqlengine.models import Model

import tCass.db_client as dbc
from basedb import BaseDB

# NOTE(whw): This fixes a warning.
os.environ["CQLENG_ALLOW_SCHEMA_MANAGEMENT"] = "true"


class CassandraDB(BaseDB):

    def __init__(self):
        # cassandra_ip = os.getenv('T_CASSANDRA_IP', 'localhost')
        # self.session = connection.setup([cassandra_ip], "ops")
        # create_keyspace_simple("ops", 1)
        stage = os.getenv('T_STAGE', 'dev')
        keyspace = stage + '_ops'

        self.client = dbc.dataClient(keyspace=keyspace)

    def create_table(self, table_name):
        self.client.build_keyspace()

    def delete_table(self, table_name):
        # model = get_model(table_name)
        # drop_table(model)
        False

    def count_items(self, table_name):
        False

    def scan_table(self, table_name):
        # model = get_model(table_name)
        # items = []
        # for datapoint in model.objects().all():
        #     items.append(datapoint.items())

        # return items
        False

    def write(self, resource_packet):
        self.client.process_packet(resource_packet)
        # resouce_id = resource_packet['resource-id']

        # models = get_models(resource_id)

        # for model in models:
        #     model.write(resource_packet['data'])

        # model.create(
        #     modified=now,
        #     year=now.year,
        #     ts=data['ts'],
        #     usage=data['usage'],
        #     total_demand=data['billing_demand'],
        #     soc=data['soc'],
        #     output=data['output'],
        #     regd_ts=data['regd_ts'],
        # ).save()
