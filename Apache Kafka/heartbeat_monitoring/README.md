## Real-Time Customer Heart Beat Monitoring System

This project simulates a real-time heart rate monitoring system for multiple customers. It generates synthetic heart rate data, streams it using Apache Kafka, stores it in a PostgreSQL database, and visualizes it in Grafana. The entire pipeline is fully automated using Docker and Docker Compose, making it easy to set up and run.

## Features

- **Real-time Data Generation**: Simulates heart rate data for 10 customers at regular intervals.
- **Data Streaming**: Uses Apache Kafka to stream heart rate data in real-time.
- **Data Storage**: Stores the heart rate data in a PostgreSQL database for persistence.
- **Data Visualization**: Visualizes the heart rate data using Grafana dashboards.
- **Automated Pipeline**: The entire system is containerized and orchestrated using Docker Compose for easy deployment.

## Architecture

The system follows a simple yet effective architecture:

1. **Data Generator (Producer)**: Generates synthetic heart rate data for customers and sends it to a Kafka topic.
2. **Apache Kafka**: Acts as the message broker, streaming the data in real-time.
3. **Consumer**: Consumes the data from Kafka and inserts it into a PostgreSQL database.
4. **PostgreSQL**: Stores the heart rate data for querying and analysis.
5. **Grafana**: Provides a dashboard to visualize the heart rate data in real-time.

All components are containerized using Docker, ensuring isolation and ease of management.

## Prerequisites

To run this project, you need the following tools installed:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Git](https://git-scm.com/downloads)

## Installation

1. **Clone the Repository**:
   ```bash
   git https://github.com/Eugenia-DE/data-portfolio.git
   cd heartbeat_monitoring

2. Start the Services:
docker-compose up -d
This command will start all the necessary services (Zookeeper, Kafka, PostgreSQL, Grafana, producer, and consumer) in detached mode.

3. Verify the Services:
docker ps

Configuration
The project uses a .env file to manage environment variables. Ensure the following variables are set in your .env file:
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
GF_SECURITY_ADMIN_USER=
GF_SECURITY_ADMIN_PASSWORD=
KAFKA_BOOTSTRAP_SERVERS=kafka:

Usage
1. View Producer and Consumer Logs
Producer Logs (data generation):

docker logs hb-producer

Consumer Logs (data processing):

docker logs hb-consumer

2. Query the PostgreSQL Database
To view the heart rate data stored in PostgreSQL:

Access the Database:


docker exec -it hb-postgres-container-1 psql -U heartbeat_user -d heartbeat_db
View the Latest 10 Records:
sql

SELECT * FROM heart_rate_records ORDER BY timestamp DESC LIMIT 10;

2. Query the PostgreSQL Database
To view the heart rate data stored in PostgreSQL:

Access the Database:

docker exec -it hb-postgres-container-1 psql -U heartbeat_user -d heartbeat_db

View the Latest 10 Records:

SELECT * FROM heart_rate_records ORDER BY timestamp DESC LIMIT 10;