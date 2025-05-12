## Real-Time Customer Heart Beat Monitoring System
## Overview
The Real-Time Customer Heart Beat Monitoring System is a data engineering project that simulates and processes heart rate data in real-time using Apache Kafka, PostgreSQL, and Streamlit. The pipeline generates synthetic heart rate data, streams it through Kafka, validates and stores it in PostgreSQL, and visualizes it via an interactive dashboard. The system detects anomalies (heart rates outside 60–100 bpm) and discards invalid data (<20 or >220 bpm), making it suitable for health monitoring applications.

## Key Features

Synthetic Data Generation: Creates realistic heart rate data for 6 customers (C001–C006) with 80% normal (60–100 bpm), 15% anomalous (20–59 or 101–220 bpm), and 5% invalid (<20 or >220 bpm) distributions.
Real-Time Streaming: Uses Kafka to stream data, ensuring scalability and low latency.
Data Validation and Anomaly Detection: Filters invalid data and flags anomalies for storage.
Persistent Storage: Stores validated data in PostgreSQL with indexes for performance.
Interactive Visualization: Displays heart rate trends and anomalies using a Streamlit dashboard.
Comprehensive Logging: Logs all operations to files (logs/*.log) and console for debugging.

## Prerequisites

Docker and Docker Compose: To run PostgreSQL, Kafka, and Zookeeper.
Python 3.8+: For running scripts.
Git: For version control.

## Setup

Clone the Repository:
git clone (https://github.com/Eugenia-DE/data-portfolio/tree/feature/heart-rate-streaming)
cd heartbeat_monitoring


Create a Virtual Environment:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


Install Dependencies:
pip install kafka-python psycopg2-binary python-dotenv streamlit pandas plotly


Configure Environment Variables:

Create a .env file in the project root

Ensure .env is listed in .gitignore.


Start Docker Services:
docker-compose up -d --remove-orphans


## Project Structure
heartbeat_monitoring/
├── docker-compose.yml      # Defines PostgreSQL, Kafka, Zookeeper services
├── .env                    # Environment variables (not in Git)
├── .gitignore              # Ignores venv/, logs/, .env
├── scripts/
│   ├── data_generator.py   # Generates synthetic heart rate data
│   ├── kafka_producer.py   # Streams data to Kafka
│   ├── kafka_consumer.py   # Consumes, validates, and stores data
│   ├── test_pipeline.py    # Tests the end-to-end pipeline
│   ├── dashboard.py        # Visualizes data with Streamlit
├── logs/                   # Log files (data_generator.log, producer.log, etc.)
├── docs/                   # Documentation artifacts
│   ├── dashboard.png       # Screenshot of Streamlit dashboard
│   ├── data_flow_diagram.pdf  # Data flow diagram

## Usage
Running the Pipeline

Start the Producer (generates and streams data to Kafka):
python scripts/kafka_producer.py


Start the Consumer (processes and stores data in PostgreSQL):
python scripts/kafka_consumer.py


Run Both in Background:
python scripts/kafka_producer.py & python scripts/kafka_consumer.py &


Test the Entire Pipeline:
python scripts/test_pipeline.py


Generates 50 records, processes them, and verifies storage (expects ≥8 records stored).



## Visualizing Data Using Streamlite

Run the Streamlit dashboard:streamlit run scripts/dashboard.py


Open http://localhost:8501 in a browser.
Select a customer (C001–C005) to view:
A scatter plot (blue for normal heart rates, red for anomalies).
A table of detected anomalies (heart rates <60 or >100 bpm).


Stopping Services

Stop Docker services:docker-compose down -v


Logging

Logs are saved in the logs/ directory:
data_generator.log: Data generation logs.
producer.log: Kafka producer logs.
consumer.log: Kafka consumer logs.
dashboard.log: Dashboard interaction logs.


Example log entry:2025-05-12 02:00:00,123 - __main__ - INFO - Stored (Normal): {'customer_id': 'C001', 'timestamp': '2025-05-12T02:00:00Z', 'heart_rate': 75}


Use cat logs/<log-file> to view logs.

## Core Components

Synthetic Data Generator (data_generator.py): Creates heart rate data with realistic distributions.
Kafka Producer (kafka_producer.py): Streams data to the heart_rate_data topic.
Kafka Consumer (kafka_consumer.py): Validates data, detects anomalies, and stores in PostgreSQL.
PostgreSQL Database (docker-compose.yml): Persists heart rate records with indexes.
Dashboard (dashboard.py): Visualizes heart rate trends and anomalies.

## Tasks

Data Simulation & Ingestion: Generates and streams heart rate data.
Real-Time Data Processing: Validates and processes data via Kafka.
Visualization: Displays data with anomaly highlighting.

## Artifacts

Screenshots:
Dashboard: docs/dashboard.png

Data Flow Diagram: docs/data_flow_diagram.pdf
Illustrates the pipeline: Generator → Producer → Kafka → Consumer → PostgreSQL → Dashboard.


## Troubleshooting

No Data in Dashboard:
Run python scripts/test_pipeline.py to populate PostgreSQL.
Check PostgreSQL:docker exec -it $(docker ps -q -f name=postgres) psql -U postgres -d heart_rate_db
SELECT * FROM heart_rate LIMIT 5;


Streamlit Not Found:
Ensure dependencies:pip install streamlit pandas plotly


Kafka Connection Issues:
Verify Docker services:docker ps
docker logs $(docker ps -q -f name=kafka)

Check logs in logs/ for detailed errors.

Development Notes

Git Workflow:
Developed on feature/heart-rate-streaming branch.
.gitignore excludes venv/, logs/, .env.


Added file logging to all scripts.
Installed Streamlit for visualization.

