import boto3
import csv
import time
import json
import base64
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-north-1')  

def stream_csv_to_kinesis(file_path, stream_name, key_column, delay=0.5):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            trip_id = row[key_column]
            # Optional: Convert datetime strings to ISO if needed
            for key, val in row.items():
                if 'datetime' in key and val:
                    try:
                        row[key] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").isoformat()
                    except:
                        pass
                elif val.replace('.', '', 1).isdigit():
                    row[key] = float(val)

            data_json = json.dumps(row)
            print(f"Sending: {data_json}")
            kinesis.put_record(
                StreamName=stream_name,
                Data=data_json.encode('utf-8'),
                PartitionKey=trip_id
            )
            time.sleep(delay)

stream_csv_to_kinesis("data/trip_start.csv", "trip-start-stream", "trip_id", delay=0.3)
stream_csv_to_kinesis("data/trip_end.csv", "trip-end-stream", "trip_id", delay=0.3)
