import json
import boto3
import os
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TRIP_TABLE'])

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # payload = json.loads(record['kinesis']['data'])
            # Decode base64 data from Kinesis
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)

            trip_id = payload['trip_id']
            print(f"Processing trip_start for trip_id: {trip_id}")

            # Prepare update expression
            update_expr = """SET pickup_location_id = :pli,
                                dropoff_location_id = :dli,
                                vendor_id = :vid,
                                pickup_datetime = :pdt,
                                estimated_dropoff_datetime = :eddt,
                                estimated_fare_amount = :efa,
                                event_start_received = :esr"""
            expr_vals = {
                ':pli': payload['pickup_location_id'],
                ':dli': payload['dropoff_location_id'],
                ':vid': payload['vendor_id'],
                ':pdt': payload['pickup_datetime'],
                ':eddt': payload['estimated_dropoff_datetime'],
                ':efa': Decimal(str(payload['estimated_fare_amount'])),
                ':esr': True
            }

            # Update or insert item in DynamoDB
            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_vals
            )

        except Exception as e:
            print(f"Error processing record: {str(e)}")
            raise e
