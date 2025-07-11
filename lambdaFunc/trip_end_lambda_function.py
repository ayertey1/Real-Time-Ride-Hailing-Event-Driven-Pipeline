import json
import boto3
import os
import base64
from decimal import Decimal, InvalidOperation

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TRIP_TABLE'])

def safe_decimal(value, field_name=""):
    try:
        if value is None or str(value).strip() == "":
            raise ValueError("Empty or None value")
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as e:
        print(f"[WARNING] Invalid decimal for '{field_name}': '{value}' - defaulting to 0")
        return Decimal('0')  # fallback value

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode base64 data from Kinesis
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)

            trip_id = str(payload.get('trip_id', 'UNKNOWN'))
            print(f"Processing trip_end for trip_id: {trip_id}")

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
                ':ddt': payload.get('dropoff_datetime', 'N/A'),
                ':rc': safe_decimal(payload.get('rate_code'), 'rate_code'),
                ':pc': safe_decimal(payload.get('passenger_count'), 'passenger_count'),
                ':td': safe_decimal(payload.get('trip_distance'), 'trip_distance'),
                ':fa': safe_decimal(payload.get('fare_amount'), 'fare_amount'),
                ':ta': safe_decimal(payload.get('tip_amount'), 'tip_amount'),
                ':pt': safe_decimal(payload.get('payment_type'), 'payment_type'),
                ':tt': safe_decimal(payload.get('trip_type'), 'trip_type'),
                ':eer': True
            }

            # Update the item in DynamoDB
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
            print(f"[ERROR] Error processing trip_end record for trip_id {trip_id}: {str(e)}")
            raise e
