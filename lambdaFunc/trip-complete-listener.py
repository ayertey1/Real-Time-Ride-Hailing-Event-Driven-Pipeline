import json
import boto3
import os

sqs = boto3.client('sqs')
queue_url = os.environ['TRIP_COMPLETE_QUEUE']

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] not in ['INSERT', 'MODIFY']:
            continue

        new_image = record['dynamodb'].get('NewImage', {})
        trip_complete = new_image.get('trip_complete', {}).get('BOOL', False)

        if not trip_complete:
            continue  # Only act on completed trips

        trip_id = new_image['trip_id']['S']
        dropoff_date = new_image['dropoff_datetime']['S'][:10]

        message = {
            'trip_id': trip_id,
            'trip_date': dropoff_date
        }

        try:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message)
            )
            print(f"Sent trip {trip_id} to SQS for aggregation.")
        except Exception as e:
            print(f"Failed to send trip {trip_id} to SQS: {str(e)}")
            raise e
