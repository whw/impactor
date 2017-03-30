import boto3
import json
import os
from basedb import BaseDB


class DynamoDB(BaseDB):

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

        # print(table_name + " " + region)
        # print(dynamodb_url)

        return (table_name, region, dynamodb_url)

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

    def number_of_items_in_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        return dynamodb.scan(TableName=table_name)['Count']

    def delete_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        dynamodb.delete_table(TableName=table_name)
        dynamodb.get_waiter('table_not_exists').wait(TableName=table_name)

        print("Deleted " + table_name + " in " + region + " accessible at")
        print(dynamodb_url)

    def scan_table(self):
        table_name, region, dynamodb_url = self.get_config()
        dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

        return dynamodb.scan(TableName=table_name)
