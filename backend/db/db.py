import os

from dynamodb import DynamoDB


def get_db(name='cassandra'):
    name = os.getenv('T_DATABASE', name)

    if name == 'dynamodb':
        return DynamoDB()
    elif name == 'cassandra':
        from cassandradb import CassandraDB
        return CassandraDB()
    else:
        raise 'Invalid database: ' + name
