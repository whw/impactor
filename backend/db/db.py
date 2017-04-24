import os
from cassandradb import CassandraDB
from dynamodb import DynamoDB


def get_db(name='dynamodb'):
    name = os.getenv('T_DATABASE', name)

    if name == 'dynamodb':
        return DynamoDB()
    elif name == 'cassandra':
        return CassandraDB()
    else:
        raise 'Invalid database: ' + name
