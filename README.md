# Formula 1 End-to-End Medallion Data Pipeline

## Overview
This repository contains an end-to-end data engineering pipeline designed to ingest, transform, and model Formula 1 historical and real-time data. Built using a **Medallion Architecture (Bronze -> Silver -> Gold)**, the pipeline combines batch ingestion from flat files with streaming ingestion from external REST APIs via Apache Kafka, orchestrated using Apache Airflow and fully containerized with Docker.

---

## Architecture and Design Patterns

### 1. Medallion Architecture
* **Bronze Layer:** Raw ingestion of historical CSV dumps and streaming JSON payloads from the Ergast/Jolpi F1 API. Preserves data lineage with ingestion timestamps and metadata.
* **Silver Layer:** Cleaned, validated, and normalized relational entities (Races, Drivers, Constructors, Results, Lap Times, Pit Stops). Enforces referential integrity, deduplication, and handles idempotent updates (`UPSERT`).
* **Gold Layer:** Dimensional analytical model based on Kimball star/snowflake schema methodology. Includes fact tables (`fact_results`, `fact_lap_times`, `fact_pit_stops`) and dimension tables optimized for analytical queries.

### 2. Incremental Processing and Watermarking
* Dynamic watermarking using composite logical keys (`season` and `round`) tracked directly in the database.
* The API Producer queries the latest processed state to request only new and completed race weekends.

### 3. Pipeline Short-Circuiting
* Integrated conditional execution within Airflow via `AirflowSkipException`.
* Evaluates incoming CSV row counts and API watermark state: if no new events or rows are ingested into Bronze, the pipeline short-circuits and skips execution of downstream Silver and Gold transformation tasks, conserving database compute and avoiding unnecessary table locks.

---

## Tech Stack
* **Orchestration:** Apache Airflow
* **Message Broker / Streaming:** Apache Kafka (Confluent Kafka)
* **Database / Storage:** PostgreSQL
* **Data Processing & Transformations:** Python, Pandas, SQLAlchemy, Raw SQL (ELT)
* **Containerization:** Docker, Docker Compose

---

## Repository Structure

```text
.
├── airflow/
│   ├── config/
│   │   ├── airflow.cfg
│   │   └── settings.py
│   ├── dags/
│   │   ├── f1_medallion_pipeline.py
│   │   └── test_dag.py
│   ├── logs/
│   └── plugins/
├── data/
│   └── dataEngineeringDataset.csv
├── docs/
│   └── database_schema.dbml
├── f1_data_pipeline/
│   ├── kafka/
│   │   ├── consumer.py
│   │   └── producer.py
│   ├── sql/
│   │   ├── create_bronze_tables.sql
│   │   └── create_table.sql
│   ├── utils/
│   │   ├── api_client.py
│   │   └── logger.py
│   ├── bronze_ingestion.py
│   ├── gold_transformations.py
│   └── silver_transformations.py
├── images/
│   └── snowflake.png
├── logs/
│   └── f1_pipeline.log
├── notebooks/
│   └── 01_eda.ipynb
├── .env
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup and Execution

### Prerequisites
* Docker and Docker Compose installed.
* Host access to ports: `8080` (Airflow Webserver), `5432` (PostgreSQL), `9092`/`29092` (Kafka).

### 1. Environment Configuration
Create a `.env` file in the root directory (or use default container environment variables):
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=f1_db
POSTGRES_HOST=f1_postgres
POSTGRES_PORT=5432
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

### 2. Build and Start Services
Start the full application stack using Docker Compose:
```bash
docker compose up -d --build
```

### 3. Database Initialization
Tables are initialized automatically via startup DDL scripts located in `f1_data_pipeline/sql/`. The bronze, silver, and gold schemas are created on first run.

### 4. Running the Pipeline
Access the Airflow UI at `http://localhost:8080` (default credentials: `admin` / `admin`).

1. Enable the DAG: `f1_medallion_pipeline`.
2. Trigger the DAG manually or let it run on its scheduled intervals (runs weekly on Saturdays and Sundays to capture race weekend events).
3. Execution flow:
   * `get_last_processed_race` fetches the latest season and round watermark.
   * `produce_api_data` and `consume_api_data` stream and persist API events via Kafka.
   * `ingest_bronze_csv` appends any incremental CSV data and returns row counts to XCom.
   * `transform_silver` evaluates if new data arrived. If yes, it runs ELT transformations; if not, it skips itself and the downstream `transform_gold` task.