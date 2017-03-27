import boto3
import os
import time

# table_name = os.getenv('DYNAMO_TABLE_NAME', 'FleetStatus')
# region = os.getenv('DYNAMO_REGION', 'us-west-2')
# # 'http://localhost:8000'
# dynamodb_url = os.getenv('DYNAMO_ENDPOINT_URL', None)


def _get_config():
    stage = os.getenv('T_STAGE', 'dev')
    region = 'us-west-2'

    if stage == 'dev':
        table_name = 'dev_FleetStatus'
        dynamodb_url = 'http://localhost:8000'
    elif stage == 'test':
        table_name = 'test_FleetStatus'
        dynamodb_url = 'http://localhost:8000'
    elif stage == 'prod':
        table_name = 'FleetStatus'
        dynamodb_url = None
    else:
        print('Invalid stage recieved: ' + stage)
        raise

    return (table_name, region, dynamodb_url)


def create_table():
    table_name, region, dynamodb_url = _get_config()
    dynamodb = boto3.client(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

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

    dynamodb.get_waiter('table_exists').wait(TableName=table_name)

    print("Created " + table_name + " in " +
          region + " accessible at " + dynamodb_url)


def insert_status(status):
    table_name, region, dynamodb_url = _get_config()
    dynamodb_client = boto3.client(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    # NOTE(whw): Numbers are always sent to DynamoDB as strings
    item = {
        'deviceId': {'S': 'abcdef'},
        'timestamp': {'N': str(time.time())},
        'status': {'N': str(status)},
    }

    dynamodb_client.put_item(TableName=table_name, Item=item)


def number_of_items_in_table():
    table_name, region, dynamodb_url = _get_config()
    dynamodb = boto3.resource(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    table = dynamodb.Table(table_name)

    return table.item_count


def delete_table():
    table_name, region, dynamodb_url = _get_config()
    dynamodb = boto3.client(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    dynamodb.delete_table(TableName=table_name)
    dynamodb.get_waiter('table_not_exists').wait(TableName=table_name)
    print("Deleted " + table_name + " in " +
          region + " accessible at " + dynamodb_url)
