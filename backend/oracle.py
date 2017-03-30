import boto3
import json
import os
import time
from db import db

status_to_orders = {
    -1: 0,
    0: 1,
    1: -1,
}


def _build_status(output):
    return [
        {
            "VaTech": {
                "soc": 10.0,
                "billing_demand": -1.0,
                "regd_ts": 1430496000.0,
                "battery": 0.0,
                "emergency_time": 1.333333,
                "ts": time.time(),
                "other": {
                    "plug1": {
                        "Power Sum": None
                    },
                    "compressor": {
                        "Power Sum": None
                    },
                    "airhandler": {
                        "Power Sum": None
                    },
                    "plug2": {
                        "Power Sum": None
                    },
                    "lighting": {
                        "Power Sum": None
                    }
                },
                "usage": 0.0,
                "output": output,
                "target_output": 1.0,
                "pv_production": 0.0
            }
        }
    ]


def handler(datapoints, context):
    if os.getenv('T_STAGE') != None:
        table_name, region, dynamodb_url = db.get_db().get_config()
    else:
        table_name = 'FleetStatus'
        region = 'us-west-2'
        dynamodb_url = None

    orders = None

    for datapoint in datapoints:
        device_id = datapoint.keys()[0]
        data = datapoint[device_id]
        _write(device_id, data, table_name, region, dynamodb_url)
        orders = get_orders(data)

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


def get_orders(data):
    return json.dumps({
        "ts": data['ts'],
        "cmd": "output",
        "power": status_to_orders[data['output']]
    })
