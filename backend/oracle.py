import boto3
import json
import os
import time

import status
from db import db
from predict import predict


def handler(datapoints, context):
    # datapoints is a native python data structure, not the raw json string
    # you expect. This conversion is handled by Lambda automatically.

    if os.getenv('T_STAGE') != None:
        table_name, region, dynamodb_url = db.get_db().get_config()
    else:
        table_name = 'FleetStatus'
        region = 'us-west-2'
        dynamodb_url = None

    prediction_strategy = predict.get_strategy()

    orders = None

    for datapoint in datapoints:
        device_id = datapoint.keys()[0]
        data = datapoint[device_id]
        _write(device_id, data, table_name, region, dynamodb_url)
        orders = prediction_strategy.predict(device_id, data)

    return orders


def _write(device_id, data, table_name, region, dynamodb_url):
    dynamodb_client = boto3.client(
        'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    # NOTE(whw): Numbers are always sent to DynamoDB as strings
    item = {
        'deviceId': {'S': device_id},
        'timestamp': {'N': str(data['ts'])},
        'data': {'S': json.dumps(data)},
    }

    dynamodb_client.put_item(TableName=table_name, Item=item)
