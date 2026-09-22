Here’s a **GitHub-friendly shorter README** — enough technical depth to demonstrate the project without becoming huge.

# 🚕 Real-Time Taxi Analytics

A real-time data engineering pipeline that streams NYC taxi trip data through **Kafka**, processes it using **PySpark Structured Streaming**, validates and enriches the data using a **Bronze → Silver → Gold** architecture, and produces analytics-ready datasets for **Power BI**.

The project intentionally injects invalid and duplicate events to simulate real-world data quality problems.

---

## 🏗️ Architecture

```text
NYC TLC Dataset
      │
      ▼
Python Producer
      │
      │ JSON events
      ▼
    Kafka
      │
      ▼
Spark Structured Streaming
      │
      ▼
   ┌───────────────┐
   │    BRONZE     │
   │ Raw + Metadata│
   │ Validation    │
   └───────┬───────┘
           │
      ┌────┴─────┐
      ▼          ▼
   SILVER    QUARANTINE
      │
      │ Clean data
      ▼
    GOLD
      │
      ▼
   Power BI
```

---

## 🛠️ Tech Stack

* **Python**
* **Apache Kafka**
* **PySpark / Spark Structured Streaming**
* **Docker**
* **Parquet**
* **Jupyter Notebook**
* **Power BI**
* **PostgreSQL** *(planned/optional serving layer)*

---

## 📂 Project Structure

```text
Real_time_Taxi/
│
├── producer/
│   └── producer.py
│
├── jobs/
│   ├── bronze_silver_stream.py
│   └── gold_job.py
│
├── notebooks/
│   └── ...
│
├── data/
│   ├── green_tripdata_2026-01.parquet
│   └── taxi_zone_lookup.csv
│
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   ├── quarantine/
│   └── gold/
│
├── checkpoints/
│
├── docker/
│   └── docker-compose.yml
│
└── README.md
```

---

## 🔄 Data Flow

### 1. Producer

The Python producer reads NYC taxi data and converts individual records into streaming events.

It also generates:

* Unique `event_id`
* Current pickup/dropoff timestamps
* Random invalid records
* Duplicate events

Example simulated errors:

```text
negative_fare
negative_distance
invalid_location
null_passenger_count
invalid_payment_type
future_timestamp
dropoff_before_pickup
huge_fare
```

---

### 2. Kafka

Events are published to:

```text
taxi-data
```

Kafka provides:

* Streaming transport
* Partitioning
* Offsets
* Buffering
* Replayability
* Retention

Kafka is used as the **streaming layer**, not permanent analytical storage.

---

### 3. Bronze

Spark reads Kafka using `readStream` and stores the incoming data as Parquet.

Bronze contains:

* Taxi event data
* Kafka topic/partition/offset
* Kafka timestamp
* Location enrichment
* Validation flags
* Validation errors
* Duplicate status

Example:

```text
data_lake/
└── bronze/
    └── taxi/
        ├── ingest_date=2026-09-21/
        └── ingest_date=2026-09-22/
```

Bronze answers:

> **What did the pipeline receive?**

---

### 4. Silver

Silver contains validated and cleaned data.

The pipeline checks:

* Required fields
* Vendor IDs
* Passenger count
* Distance
* Location IDs
* Payment type
* Fare values
* Duration
* Timestamp ordering
* Duration consistency
* Future timestamps
* Duplicate events

Valid records move to:

```text
Silver
```

Invalid records are preserved in:

```text
Quarantine
```

Silver answers:

> **What data can downstream systems trust?**

---

### 5. Gold

Gold contains business-oriented aggregations such as:

```text
Daily Revenue
Hourly Demand
Zone Metrics
Route Metrics
Payment Metrics
```

Example:

```text
date
total_trips
total_revenue
avg_fare
avg_distance
avg_duration
```

Gold answers:

> **What does the data tell us?**

---

## ⚡ Streaming Model

Spark Structured Streaming processes Kafka data using **micro-batches**.

```text
Kafka
  │
  ▼
Spark
  │
  ├── Micro-batch 1 → Parquet
  ├── Micro-batch 2 → Parquet
  ├── Micro-batch 3 → Parquet
  └── ...
```

A trigger can be configured, for example:

```python
.trigger(processingTime="10 seconds")
```

Spark manages the micro-batch execution and checkpointing.

---

## 💾 Storage

The current project uses **Parquet**:

```text
data_lake/
├── bronze/
├── silver/
├── quarantine/
└── gold/
```

Production evolution:

```text
Local Parquet
     ↓
S3 / ADLS / GCS
     ↓
Delta Lake / Apache Iceberg
```

Partitioning is used to improve query performance and enable partition pruning.

---

## 🔁 Checkpointing

Spark checkpoints streaming progress separately from the data:

```text
checkpoints/
└── bronze_silver/
```

This allows the streaming application to recover its progress after failures.

---

## 📊 Analytics

Gold datasets are designed for consumption by **Power BI**.

Example:

```text
Kafka
  ↓
Spark
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Power BI
```

---

## 🚀 Production Evolution

The current project is a local production-style implementation.

A future cloud architecture:

```text
Kafka / Amazon MSK
        │
        ▼
Spark Structured Streaming
        │
        ▼
Amazon S3
        │
   ┌────┴────┐
   ▼         ▼
Bronze   Quarantine
   │
   ▼
Silver
   │
   ▼
Gold
   │
   ▼
Athena / Redshift
   │
   ▼
Power BI
```

Future improvements include:

* Watermarking
* Stateful deduplication
* Data-quality monitoring
* Kafka lag monitoring
* Parquet compaction
* Delta Lake / Iceberg
* Airflow orchestration
* AWS deployment
* Incremental Gold processing
* CI/CD and monitoring

---

## 🎯 Key Concepts Demonstrated

* Kafka Producers & Topics
* Kafka Partitions & Offsets
* Spark Structured Streaming
* Micro-batch Processing
* Checkpointing
* Data Quality & Quarantine
* Deduplication
* Schema & Type Handling
* Data Enrichment
* Medallion Architecture
* Parquet & Partitioning
* Streaming → Data Lake
* Analytical Aggregations
* Power BI

---

## 👨‍💻 Author

**Vishwajeet Patil**

Data Engineer | Gen AI Engineer

[Portfolio](https://vishwajeetpatill.netlify.app/)
