import boto3
import json
import os
import time
import dynamodb

status_to_orders = {
    -1: 0,
    0: 1,
    1: -1,
}


def handler(event, context):
    # print("Received Status: \n" + json.dumps(event, indent=2))
    status = event['status']

    if os.getenv('T_STAGE') != None:
        table_name, region, dynamodb_url = dynamodb._get_config()
    else:
        table_name = 'FleetStatus'
        region = 'us-west-2'
        dynamodb_url = None

    insert_status(status, table_name, region, dynamodb_url)

    # print("Received Status: " + str(status))
    return json.dumps({'orders': status_to_orders[status]})


def insert_status(status, table_name, region, dynamodb_url):

    dynamodb_client = boto3.client(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    # NOTE(whw): Numbers are always sent to DynamoDB as strings
    item = {
        'deviceId': {'S': 'abcdef'},
        'timestamp': {'N': str(time.time())},
        'status': {'N': str(status)},
    }

    dynamodb_client.put_item(TableName=table_name, Item=item)
