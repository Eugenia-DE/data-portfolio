import argparse
import json
import logging
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_heart_rate():
    """Generate a random heart rate between 60 and 100."""
    return random.randint(60, 100)

def main(customers, interval):
    # Kafka producer configuration
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    logger.info(f"Starting producer for {customers} customers with {interval}s interval")
    
    try:
        while True:
            for customer_id in range(1, customers + 1):
                heart_rate = generate_heart_rate()
                timestamp = datetime.utcnow().isoformat() + 'Z'
                message = {
                    'customer_id': customer_id,
                    'heart_rate': heart_rate,
                    'timestamp': timestamp
                }
                
                # Send message to Kafka
                producer.send('heart_rate_data', message)
                logger.info(f"Sent data for customer {customer_id}: {message}")
            
            producer.flush()
            time.sleep(interval)
    
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate heart rate data for customers')
    parser.add_argument('--customers', type=int, default=5, help='Number of customers')
    parser.add_argument('--interval', type=float, default=2.0, help='Interval between messages in seconds')
    args = parser.parse_args()
    
    main(args.customers, args.interval)