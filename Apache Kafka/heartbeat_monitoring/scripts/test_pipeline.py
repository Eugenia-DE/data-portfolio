import json
import logging
import time
import os
from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
import psycopg2

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_pipeline.log'),
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

def test_single_message():
    """Test producing and consuming a single message."""
    logger.info("Running test: Test single message for customer 1")
    
    # Produce a test message
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    test_message = {
        'customer_id': 1,
        'heart_rate': 75,
        'timestamp': '2025-05-07T12:00:00.000Z'
    }
    producer.send('heart_rate_data', test_message)
    producer.flush()
    producer.close()
    logger.info(f"Produced test message: {test_message}")
    
    # Wait for consumer to process
    time.sleep(2)
    
    # Check PostgreSQL
    conn = setup_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM heart_rate_records WHERE customer_id = %s AND heart_rate = %s",
                (1, 75)
            )
            count = cur.fetchone()[0]
            if count > 0:
                logger.info(f"Database check passed: Found {count} records for customer 1")
                return True
            else:
                logger.error("Database check failed: No records found for customer 1")
                return False
    finally:
        conn.close()

def main():
    tests_passed = 0
    total_tests = 1
    
    # Run tests
    if test_single_message():
        tests_passed += 1
        logger.info("Test passed: Test single message for customer 1")
    else:
        logger.error("Test failed: Test single message for customer 1")
    
    # Summary
    logger.info(f"Test Summary: {tests_passed}/{total_tests} tests passed")
    if tests_passed == total_tests:
        logger.info("All tests passed successfully")
    else:
        logger.error("Some tests failed")

if __name__ == '__main__':
    main()