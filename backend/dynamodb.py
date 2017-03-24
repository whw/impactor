import boto3
import time

table_name = 'FleetStatus'


def create_table():
    dynamodb = boto3.resource(
        'dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'deviceId',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'deviceId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'N'  # Seconds since epoch
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)


def insert_status(status):
    dynamodb_client = boto3.client(
        'dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

    item = {
        'deviceId': {'S': 'abcdef'},
        'timestamp': {'N': str(time.time())},
        'status': {'N': str(status)},
    }

    dynamodb_client.put_item(TableName=table_name, Item=item)


def number_of_items_in_table():
    dynamodb = boto3.resource(
        'dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(table_name)

    return table.item_count


def delete_table():
    dynamodb = boto3.client(
        'dynamodb', region_name='us-west-2', endpoint_url="http://localhost:8000")

    table = dynamodb.delete_table(TableName=table_name)
