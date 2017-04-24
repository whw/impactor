import boto3
import json
import os
from basedb import BaseDB


class DynamoDB(BaseDB):

    def create_table(self):
        table_name, region, dynamodb_url = self.get_config()
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

        print("Created " + table_name + " in " + region + " accessible at")
        print(dynamodb_url)

    def delete_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        dynamodb.delete_table(TableName=table_name)
        dynamodb.get_waiter('table_not_exists').wait(TableName=table_name)

        print("Deleted " + table_name + " in " + region + " accessible at")
        print(dynamodb_url)

    def get_config(self):
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

    def number_of_items_in_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        return dynamodb.scan(TableName=table_name)['Count']

    def scan_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        return dynamodb.scan(TableName=table_name)

    def write_item(self, data):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb_client = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        device_id = data.keys()[0]
        raw_data = data[device_id]

        # NOTE(whw): Numbers are always sent to DynamoDB as strings
        item = {
            'deviceId': {'S': device_id},
            'timestamp': {'N': str(raw_data['ts'])},
            'data': {'S': json.dumps(raw_data)},
        }

        dynamodb_client.put_item(TableName=table_name, Item=item)
