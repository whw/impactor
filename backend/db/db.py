import os


def get_db(name='cassandra'):
    name = os.getenv('T_DATABASE', name)

    if name == 'dynamodb':
        from dynamodb import DynamoDB
        return DynamoDB()

    elif name == 'cassandra':
        from cassandra_db import CassandraDB
        return CassandraDB()

    else:
        raise 'Invalid database: ' + name
