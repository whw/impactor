from dynamodb import DynamoDB


def get_db(name='dynamodb'):
    if name == 'dynamodb':
        return DynamoDB()
    elif name == 'cassandra':
        raise 'Cassandra not implemented yet'
    else:
        raise 'Invalid database: ' + name
