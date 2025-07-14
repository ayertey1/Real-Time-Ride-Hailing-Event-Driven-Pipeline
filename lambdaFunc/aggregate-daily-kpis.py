import boto3
import os
import json
from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging

# 🔹 Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

TABLE_NAME = os.environ['TRIP_TABLE']
BUCKET_NAME = os.environ['OUTPUT_BUCKET']

table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    filter_date = event.get('date')

    try:
        logger.info("Scanning DynamoDB for completed trips...")
        response = table.scan()
        trips = response['Items']
    except Exception as e:
        logger.error(f"Failed to scan DynamoDB: {e}")
        raise

    completed = [t for t in trips if t.get('trip_complete') and 'dropoff_datetime' in t]

    if not completed:
        logger.warning("No completed trips found. Exiting.")
        return

    # Group trips by date
    trips_by_date = defaultdict(list)
    for trip in completed:
        try:
            dropoff_date = trip['dropoff_datetime'][:10]
            trips_by_date[dropoff_date].append(trip)
        except Exception as e:
            logger.warning(f"Skipping invalid trip record: {trip.get('trip_id', 'UNKNOWN')} - {e}")

    if filter_date:
        logger.info(f"Filtering for only: {filter_date}")
        if filter_date not in trips_by_date:
            logger.warning(f"No trips found for {filter_date}")
            return
        dates_to_process = [filter_date]
    else:
        dates_to_process = sorted(trips_by_date.keys())

    for date in dates_to_process:
        try:
            trips = trips_by_date[date]
            fares = [Decimal(str(t.get('fare_amount', 0))) for t in trips]

            result = {
                "date": date,
                "total_fare": float(sum(fares)),
                "count_trips": len(fares),
                "average_fare": float(sum(fares) / len(fares)),
                "max_fare": float(max(fares)),
                "min_fare": float(min(fares))
            }

            key = f"trip-kpis/{date}/kpis.json"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=json.dumps(result, indent=2),
                ContentType='application/json'
            )

            logger.info(f"KPI for {date} written to s3://{BUCKET_NAME}/{key}")

        except Exception as e:
            logger.error(f"Failed to process aggregation for {date}: {e}")
            # Optionally continue to next date instead of failing whole function

    logger.info("KPI aggregation job complete.")
