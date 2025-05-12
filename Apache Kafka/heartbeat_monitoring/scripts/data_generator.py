import random
import time
from datetime import datetime, timezone
import logging
import os
from dotenv import load_dotenv

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'data_generator.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
NORMAL_MIN = int(os.getenv("NORMAL_HEART_RATE_MIN", 60))
NORMAL_MAX = int(os.getenv("NORMAL_HEART_RATE_MAX", 100))
VALID_MIN = int(os.getenv("VALID_HEART_RATE_MIN", 20))
VALID_MAX = int(os.getenv("VALID_HEART_RATE_MAX", 220))

def generate_heart_rate_data(customer_ids=None, customer_index=None):
    """Generate synthetic heart rate data for a customer."""
    if customer_ids is None:
        customer_ids = [f"C{str(i).zfill(3)}" for i in range(1, 7)]
    
    # Cycle through customers if customer_index is provided
    if customer_index is not None:
        customer_id = customer_ids[customer_index % len(customer_ids)]
    else:
        customer_id = random.choice(customer_ids)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Heart rate distribution: 80% normal, 15% anomalous, 5% invalid
    rand = random.random()
    if rand < 0.80:
        heart_rate = random.randint(NORMAL_MIN, NORMAL_MAX)  # Normal
    elif rand < 0.95:
        # Anomalous: either low (20–59) or high (101–220)
        if random.random() < 0.5:
            heart_rate = random.randint(VALID_MIN, NORMAL_MIN - 1)
        else:
            heart_rate = random.randint(NORMAL_MAX + 1, VALID_MAX)
    else:
        # Invalid: <20 or >220
        if random.random() < 0.5:
            heart_rate = random.randint(0, VALID_MIN - 1)
        else:
            heart_rate = random.randint(VALID_MAX + 1, 300)
    
    data = {
        "customer_id": customer_id,
        "timestamp": timestamp,
        "heart_rate": heart_rate
    }
    return data

def main(num_customers=5, num_records=10):
    """Demonstrate data generation for testing."""
    customer_ids = [f"C{str(i).zfill(3)}" for i in range(1, num_customers + 1)]
    for i in range(num_records):
        data = generate_heart_rate_data(customer_ids, customer_index=i)
        logger.info(f"Generated: {data}")
        time.sleep(1)

if __name__ == "__main__":
    main()