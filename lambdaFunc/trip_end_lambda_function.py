import json
import boto3
import os
from decimal import Decimal

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
            print(f"Processing trip_end for trip_id: {trip_id}")

            # First update the end fields
            update_expr = """SET dropoff_datetime = :ddt,
                                rate_code = :rc,
                                passenger_count = :pc,
                                trip_distance = :td,
                                fare_amount = :fa,
                                tip_amount = :ta,
                                payment_type = :pt,
                                trip_type = :tt,
                                event_end_received = :eer"""
            expr_vals = {
                ':ddt': payload['dropoff_datetime'],
                ':rc': Decimal(str(payload['rate_code'])),
                ':pc': Decimal(str(payload['passenger_count'])),
                ':td': Decimal(str(payload['trip_distance'])),
                ':fa': Decimal(str(payload['fare_amount'])),
                ':ta': Decimal(str(payload['tip_amount'])),
                ':pt': Decimal(str(payload['payment_type'])),
                ':tt': Decimal(str(payload['trip_type'])),
                ':eer': True
            }

            # Apply update
            table.update_item(
                Key={'trip_id': trip_id},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_vals
            )

            # Check if start event already arrived
            response = table.get_item(Key={'trip_id': trip_id})
            item = response.get('Item', {})

            if item.get('event_start_received') and item.get('event_end_received'):
                print(f"Trip {trip_id} is now complete.")
                table.update_item(
                    Key={'trip_id': trip_id},
                    UpdateExpression="SET trip_complete = :tc",
                    ExpressionAttributeValues={':tc': True}
                )

        except Exception as e:
            print(f"Error processing trip_end record: {str(e)}")
            raise e
