import json
import logging
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_db_connection():
    """Set up and return a PostgreSQL connection."""
    db_params = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': 'localhost',
        'port': '5433'
    }
    try:
        conn = psycopg2.connect(**db_params)
        logger.info("PostgreSQL connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def main():
    # Kafka consumer configuration
    consumer = KafkaConsumer(
        'heart_rate_data',
        bootstrap_servers=['localhost:9095'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Database connection
    conn = setup_db_connection()
    
    logger.info("Starting consumer for topic heart_rate_data")
    
    try:
        for message in consumer:
            data = message.value
            customer_id = data['customer_id']
            heart_rate = data['heart_rate']
            timestamp = data['timestamp']
            
            logger.info(f"Received data: {data}")
            
            # Insert into PostgreSQL
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO heart_rate_records (customer_id, heart_rate, timestamp)
                        VALUES (%s, %s, %s)
                        """,
                        (customer_id, heart_rate, timestamp)
                    )
                conn.commit()
                logger.info(f"Inserted into PostgreSQL: customer_id={customer_id}, heart_rate={heart_rate}, timestamp={timestamp}")
            except Exception as e:
                logger.error(f"Failed to insert into PostgreSQL: {e}")
                conn.rollback()
    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer and database connection closed")

if __name__ == '__main__':
    main()