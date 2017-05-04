import boto3
import json
import os
from basedb import BaseDB


class DynamoDB(BaseDB):

    def __init__(self):
        stage = os.getenv('T_STAGE', 'dev')
        self.region = 'us-west-2'

        if stage == 'dev':
            self.table_name = 'dev_FleetStatus'
            self.dynamodb_url = 'http://localhost:8000'
        elif stage == 'prod':
            self.table_name = 'FleetStatus'
            self.dynamodb_url = None
        else:
            print('Invalid stage: ' + stage)
            raise

    def create_table(self, table_name):
        dynamodb = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.dynamodb_url)

        table = dynamodb.create_table(
            TableName=self.table_name,
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

        dynamodb.get_waiter('table_exists').wait(TableName=self.table_name)

        print("Created " + self.table_name +
              " in " + self.region + " accessible at")
        print(self.dynamodb_url)

    def delete_table(self, table_name):
        dynamodb = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.dynamodb_url)

        dynamodb.delete_table(TableName=self.table_name)
        dynamodb.get_waiter('table_not_exists').wait(TableName=self.table_name)

        print("Deleted " + self.table_name +
              " in " + self.region + " accessible at")
        print(self.dynamodb_url)

    def count_items(self, table_name):
        dynamodb = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.dynamodb_url)

        return dynamodb.scan(TableName=self.table_name)['Count']

    def scan_table(self, table_name):
        dynamodb = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.dynamodb_url)

        return dynamodb.scan(TableName=self.table_name)

    def write_item(self, resource_packet):
        raise "still need to solve the table name problem"

        dynamodb_client = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.dynamodb_url)

        # NOTE(whw): Numbers are always sent to DynamoDB as strings
        item = {
            'deviceId': {'S': resource_packet['resource']},
            'timestamp': {'N': str(resource_packet['ts'])},
            'data': {'S': json.dumps(resource_packet['data'])},
        }

        dynamodb_client.put_item(TableName='usage', Item=item)
