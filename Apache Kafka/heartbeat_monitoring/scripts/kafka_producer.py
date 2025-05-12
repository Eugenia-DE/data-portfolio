from kafka import KafkaProducer
import json
import os
import time
import logging
from data_generator import generate_heart_rate_data

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'producer.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main(num_customers=5, interval=5):
    """Run the Kafka producer to send heart rate data."""
    # Generate fake customer IDs
    customer_ids = [f"C{str(i).zfill(3)}" for i in range(1, num_customers + 1)]
    
    # Kafka producer configuration
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9095'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    logger.info(f"Starting producer for {num_customers} customers with {interval}s interval")
    
    try:
        while True:
            # Generate data using data_generator
            data = generate_heart_rate_data(customer_ids)
            producer.send('heart_rate_data', value=data)
            logger.info(f"Sent: {data}")
            producer.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Stopping producer")
    finally:
        producer.close()

if __name__ == "__main__":
    main()