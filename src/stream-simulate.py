import boto3
import csv
import time
import json
import threading
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='eu-north-1')  

def stream_csv_to_kinesis(file_path, stream_name, key_column, delay=0.5):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            trip_id = row[key_column]
            # Convert datetime strings to ISO if needed
            for key, val in row.items():
                if 'datetime' in key and val:
                    try:
                        row[key] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").isoformat()
                    except:
                        pass
                elif val:
                    try:
                        row[key] = float(val)
                    except ValueError:
                        pass


            data_json = json.dumps(row)
            print(f"Sending: {data_json}")
            kinesis.put_record(
                StreamName=stream_name,
                Data=data_json.encode('utf-8'),
                PartitionKey=trip_id
            )
            time.sleep(delay)


# Define threads
start_thread = threading.Thread(target=stream_csv_to_kinesis, args=("data/trip_start.csv", "trip-start-stream", "trip_id", 0.3))
end_thread = threading.Thread(target=stream_csv_to_kinesis, args=("data/trip_end.csv", "trip-end-stream", "trip_id", 0.3))

# Start both streams
start_thread.start()
end_thread.start()

# Wait for both to complete
start_thread.join()
end_thread.join()
