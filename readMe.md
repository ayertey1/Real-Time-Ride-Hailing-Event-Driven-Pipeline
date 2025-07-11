# Real-Time Ride-Hailing Event-Driven Pipeline

This project implements a **production-grade, event-driven data pipeline** for ingesting, processing, and aggregating ride-hailing trip data in real time using **AWS** services. It is fully automated, supports historical backfills, and deploys via CI/CD using GitHub Actions.

---

## Project Structure

```
REAL-TIME-PIPELINE/
├── data/
│   ├── trip_start.csv                  # Source trip start events
│   └── trip_end.csv                    # Source trip end events
│
├── lambdaFunc/
│   ├── trip_start_lambda_function.py   # Lambda to handle trip start events
│   ├── trip_end_lambda_function.py     # Lambda to handle trip end events
│   ├── trip-complete-listener.py       # Lambda for completed trip detection
│   └── aggregate-daily-kpis.py         # Aggregates and stores KPIs to S3
│
├── src/
│   └── stream-simulate.py              # Local simulator for Kinesis stream
│
├── .github/workflows/
│   ├── deploy-lambdas.yml              # CI workflow to deploy Lambdas
│   └── run-simulation.yml              # CI workflow to run streaming script
└── .gitignore
```

---

## Features

* Real-time ingestion of `trip_start` and `trip_end` via **Amazon Kinesis**
* Stream processing using **AWS Lambda** functions
* Unified record storage in **Amazon DynamoDB** keyed on `trip_id`
* Completion detection and downstream triggering using **DynamoDB Streams**
* KPI aggregation and daily partitioned JSON export to **Amazon S3**
* Robust logging, error handling, retry mechanisms, and **Dead-Letter Queues (DLQs)**
* Fully automated CI/CD pipeline using **GitHub Actions**
* Simulation tool for replaying `trip_start.csv` and `trip_end.csv`

---

## AWS Architecture Diagram

```text
                         +-------------------+
                         | trip_start.csv    |
                         +--------+----------+
                                  |
                                  v
                        [Kinesis: trip-start-stream]
                                  |
                                  v
                 [Lambda: process-trip-start-dev/prod]
                                  |
                                  v
                              +-----------------------+
                              | DynamoDB: trip_events |
                              +-----------------------+
                                  ^              ^
                                  |              |
     [Lambda: process-trip-end-dev/prod]       [Kinesis: trip-end-stream]
                                  |
                                  v
                      [trip_complete = True flag]
                                  |
                        +----------------------------------+
                        | DynamoDB Streams Trigger         |
                        |   Lambda: trip-complete-listener |
                        +-------------+--------------------+
                                      |
                                      v
                        [SQS: completed-trip-queue-dev/prod]
                                      |
                              +----------------------------+
                              | aggregate-daily-kpis.py    |
                              | (triggered by EventBridge) |
                              +----------+-----------------+
                                         |
                                         v
                    s3://analytics-bucket/trip-kpis/YYYY-MM-DD/kpis.json
```

---

## IAM Policies & Roles

All Lambdas follow **least privilege principles**:

### 1. Lambda: `process-trip-start`

```json
{
  "Effect": "Allow",
  "Action": ["dynamodb:UpdateItem"],
  "Resource": "arn:aws:dynamodb:*:*:table/trip_events"
}
```

### 2. Lambda: `process-trip-end`

Same as above with read permissions:

```json
{
  "Effect": "Allow",
  "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
  "Resource": "arn:aws:dynamodb:*:*:table/trip_events"
}
```

### 3. Lambda: `trip-complete-listener`

```json
{
  "Effect": "Allow",
  "Action": ["sqs:SendMessage"],
  "Resource": "arn:aws:sqs:*:*:completed-trip-queue-*"
}
```

### 4. Lambda: `aggregate-daily-kpis`

```json
{
  "Effect": "Allow",
  "Action": ["dynamodb:Scan", "s3:PutObject"],
  "Resource": [
    "arn:aws:dynamodb:*:*:table/trip_events",
    "arn:aws:s3:::analytics-bucket/*"
  ]
}
```

### 5. DLQ Setup (optional but implemented):

Each stream Lambda (start, end, listener) has a dedicated **SQS Dead-Letter Queue** attached for failed invocations.

---

## 📖 Development Process

1. **Plan architecture** using modular Lambda components
2. **Ingest data** from Kinesis (simulated locally via `stream-simulate.py`)
3. **Process events** in parallel and update DynamoDB
4. **Track trip completion** using atomic flags (`event_start_received`, `event_end_received`)
5. **Trigger downstream processing** on trip completion via DynamoDB Streams → SQS
6. **Aggregate KPIs** once daily via EventBridge + aggregation Lambda
7. **CI/CD pipelines** auto-deploy Lambdas on `devlab6` and `prodlab6`

---

## 🧪 CI/CD Workflows

### `deploy-lambdas.yml`

* Trigger: Push to `devlab6` or `prodlab6`
* Action: Package and update each Lambda with branch-specific suffix

### `run-simulation.yml`

* Trigger: Manual workflow\_dispatch
* Action: Run local `stream-simulate.py` to push data into Kinesis

---

## 👨‍💻 Usage Guide

### 🟢 Real-Time Streaming

```bash
python src/stream-simulate.py
```

* Streams `trip_start.csv` and `trip_end.csv` into Kinesis
* Delays between events simulate live system behavior

### 🟡 On-Demand KPI Aggregation

Trigger aggregation manually via test event in Lambda:

```json
{ "date": "2024-05-25" }
```

Or let EventBridge trigger it daily (00:30 UTC).

### 🔴 Backfill Mode

Lambda `aggregate-daily-kpis.py` will automatically detect all available `dropoff_datetime`s and write partitioned JSON files to:

```
s3://analytics-bucket/trip-kpis/YYYY-MM-DD/kpis.json
```

---

## 📊 Sample Output (S3 KPI File)

```json
{
  "date": "2024-05-25",
  "total_fare": 417.02,
  "count_trips": 7,
  "average_fare": 59.57,
  "max_fare": 81.47,
  "min_fare": 27.60
}
```

---

## ✅ Next Steps / Enhancements

* [ ] Add unit tests and test harness with `pytest`
* [ ] Create CloudWatch dashboards for traffic and failure visibility
* [ ] Enable alarms on DLQ depth / failed invocations
* [ ] Export metrics to Athena / QuickSight for business reporting

---

## 🙌 Credits

Built by **Peter Caleb Ayertey** with architectural and engineering guidance powered by ChatGPT 4o.

---

## 📝 License

This project is released under the MIT License.
