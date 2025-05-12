import json
import psycopg2
from kafka import KafkaConsumer
import logging
from dotenv import load_dotenv
import os

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'consumer.log')),
        logging.StreamHandler()
    ]
)
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

def store_data(data, conn, is_anomaly=False):
    """Store heart rate data in PostgreSQL, marking anomalies."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO heart_rate (customer_id, timestamp, heart_rate, is_anomaly)
            VALUES (%s, %s, %s, %s)
            """,
            (data["customer_id"], data["timestamp"], data["heart_rate"], is_anomaly)
        )
        conn.commit()
        status = "Anomaly" if is_anomaly else "Normal"
        logger.info(f"Stored ({status}): {data}")
    except Exception as e:
        logger.error(f"Failed to store data: {e}")
        conn.rollback()
    finally:
        cur.close()

def main():
    """Run the Kafka consumer to process and store heart rate data."""
    init_db()
    
    check_env_vars()
    consumer = KafkaConsumer(
        'heart_rate_data',
        bootstrap_servers=['localhost:9095'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host="localhost",
        port="5432"
    )
    
    logger.info("Starting consumer")
    
    try:
        for message in consumer:
            data = message.value
            heart_rate = data.get("heart_rate", 0)
            if not (VALID_MIN <= heart_rate <= VALID_MAX):
                logger.warning(f"Invalid heart rate: {data}")
                continue
            is_anomaly = not (NORMAL_MIN <= heart_rate <= NORMAL_MAX)
            store_data(data, conn, is_anomaly)
    except KeyboardInterrupt:
        logger.info("Stopping consumer")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()