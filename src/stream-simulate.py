import boto3
import csv
import time
import json
import threading
from datetime import datetime
import logging
from botocore.exceptions import BotoCoreError, ClientError

# Set up structured logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Create Kinesis client
try:
    kinesis = boto3.client('kinesis', region_name='eu-north-1')
except Exception as e:
    logger.error(f"Failed to initialize Kinesis client: {e}")
    raise

def stream_csv_to_kinesis(file_path, stream_name, key_column, delay=0.5):
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    trip_id = row[key_column]

                    # Convert datetime strings to ISO if needed
                    for key, val in row.items():
                        if 'datetime' in key and val:
                            try:
                                row[key] = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").isoformat()
                            except Exception as dt_err:
                                logger.warning(f"Failed to parse datetime for key '{key}': {val} - {dt_err}")
                        elif val:
                            try:
                                row[key] = float(val)
                            except ValueError:
                                pass  # keep as-is if not float

                    data_json = json.dumps(row)
                    logger.info(f"Sending to {stream_name}: {data_json}")
                    
                    kinesis.put_record(
                        StreamName=stream_name,
                        Data=data_json.encode('utf-8'),
                        PartitionKey=trip_id
                    )
                    time.sleep(delay)

                except (KeyError, BotoCoreError, ClientError, Exception) as err:
                    logger.error(f"Failed to send record to Kinesis: {err}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"Unexpected error reading file {file_path}: {e}")

# Define threads
start_thread = threading.Thread(target=stream_csv_to_kinesis, args=("data/trip_start.csv", "trip-start-stream", "trip_id", 0.3))
end_thread = threading.Thread(target=stream_csv_to_kinesis, args=("data/trip_end.csv", "trip-end-stream", "trip_id", 0.3))

# Start both streams
start_thread.start()
end_thread.start()

# Wait for both to complete
start_thread.join()
end_thread.join()
