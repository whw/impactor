from cassandradb import CassandraDB
from dynamodb import DynamoDB


def get_db(name='dynamodb'):
    if name == 'dynamodb':
        return DynamoDB()
    elif name == 'cassandra':
        return CassandraDB()
    else:
        raise 'Invalid database: ' + name
