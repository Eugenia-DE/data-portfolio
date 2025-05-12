import time
import logging
import json
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from data_generator import generate_heart_rate_data
from dotenv import load_dotenv
import os
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
NORMAL_MIN = int(os.getenv("NORMAL_HEART_RATE_MIN", 60))
NORMAL_MAX = int(os.getenv("NORMAL_HEART_RATE_MAX", 100))
VALID_MIN = int(os.getenv("VALID_HEART_RATE_MIN", 20))
VALID_MAX = int(os.getenv("VALID_HEART_RATE_MAX", 220))

def check_env_vars():
    """Verify required environment variables."""
    required_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")

def init_db():
    """Initialize the PostgreSQL database schema."""
    check_env_vars()
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS heart_rate (
                id SERIAL PRIMARY KEY,
                customer_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                heart_rate INTEGER NOT NULL CHECK (heart_rate >= 20 AND heart_rate <= 220),
                is_anomaly BOOLEAN DEFAULT FALSE,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_timestamp ON heart_rate (timestamp);
            CREATE INDEX IF NOT EXISTS idx_customer_id ON heart_rate (customer_id);
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized successfully")
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def produce_data(num_records=50, customer_ids=None):
    """Generate and send data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for _ in range(num_records):
        data = generate_heart_rate_data(customer_ids)
        producer.send('heart_rate_data', value=data)
        logger.info(f"Produced: {data}")
        time.sleep(0.5)
    producer.flush()
    producer.close()

def consume_data(num_records=10):
    """Consume and store data in PostgreSQL."""
    check_env_vars()
    consumer = KafkaConsumer(
        'heart_rate_data',
        bootstrap_servers=['localhost:9095'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="5432"
    )
    records_processed = 0
    try:
        for message in consumer:
            data = message.value
            heart_rate = data.get("heart_rate", 0)
            if not (VALID_MIN <= heart_rate <= VALID_MAX):
                logger.warning(f"Invalid heart rate: {data}")
                continue
            is_anomaly = not (NORMAL_MIN <= heart_rate <= NORMAL_MAX)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO heart_rate (customer_id, timestamp, heart_rate, is_anomaly)
                VALUES (%s, %s, %s, %s)
                """,
                (data["customer_id"], data["timestamp"], data["heart_rate"], is_anomaly)
            )
            conn.commit()
            logger.info(f"Consumed and stored: {data} (Anomaly: {is_anomaly})")
            records_processed += 1
            if records_processed >= num_records:
                break
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        conn.close()
        consumer.close()

def verify_data():
    """Verify data in PostgreSQL."""
    check_env_vars()
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM heart_rate")
    total_records = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM heart_rate WHERE is_anomaly = TRUE")
    anomaly_records = cur.fetchone()[0]
    cur.execute("SELECT customer_id, timestamp, heart_rate, is_anomaly FROM heart_rate LIMIT 5")
    sample_records = cur.fetchall()
    
    logger.info(f"Total records in database: {total_records}")
    logger.info(f"Anomalous records: {anomaly_records}")
    logger.info("Sample records:")
    for record in sample_records:
        logger.info(record)
    
    cur.close()
    conn.close()
    return total_records, anomaly_records

def main():
    """Test the entire pipeline."""
    logger.info("Starting pipeline test")
    
    # Initialize database
    init_db()
    
    # Clear existing data for clean test
    check_env_vars()
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE heart_rate RESTART IDENTITY")
    conn.commit()
    cur.close()
    conn.close()
    
    # Generate customer IDs
    customer_ids = [f"C{str(i).zfill(3)}" for i in range(1, 6)]
    
    # Run producer and consumer concurrently
    num_records = 50
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(produce_data, num_records, customer_ids)
        executor.submit(consume_data, num_records)
    
    # Wait for processing to complete
    time.sleep(2)
    
    # Verify results
    total_records, anomaly_records = verify_data()
    
    # Check if pipeline worked
    if total_records >= num_records * 0.8:
        logger.info("Pipeline test successful!")
    else:
        logger.error("Pipeline test failed: insufficient records stored")

if __name__ == "__main__":
    main()