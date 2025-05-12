import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from dotenv import load_dotenv
import os
import logging

# Configure logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'dashboard.log')),
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
        raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}. Ensure .env file is in {project_root}")

def fetch_data(customer_id):
    """Fetch heart rate data for a customer from PostgreSQL."""
    check_env_vars()
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host="localhost",
            port="5432"
        )
        query = """
            SELECT timestamp, heart_rate, is_anomaly
            FROM heart_rate
            WHERE customer_id = %s
            ORDER BY timestamp
        """
        df = pd.read_sql(query, conn, params=(customer_id,))
        conn.close()
        logger.info(f"Fetched {len(df)} records for customer {customer_id}")
        return df
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch data for customer {customer_id}: {e}")
        raise

def main():
    """Run the Streamlit dashboard for heart rate visualization."""
    st.title("Heart Rate Monitoring Dashboard")
    
    # Customer selection
    customer_id = st.selectbox("Select Customer", [f"C{str(i).zfill(3)}" for i in range(1, 7)])
    
    # Fetch and display data
    df = fetch_data(customer_id)
    if not df.empty:
        # Color-code anomalies
        df['color'] = df['is_anomaly'].apply(lambda x: 'red' if x else 'blue')
        fig = px.scatter(
            df,
            x="timestamp",
            y="heart_rate",
            color='color',
            color_discrete_map={'red': 'red', 'blue': 'blue'},
            title=f"Heart Rate for {customer_id} (Red = Anomaly)",
            labels={"timestamp": "Timestamp", "heart_rate": "Heart Rate (bpm)"}
        )
        fig.update_layout(
            xaxis_title="Timestamp",
            yaxis_title="Heart Rate (bpm)",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Display anomalies
        anomalies = df[df['is_anomaly']]
        if not anomalies.empty:
            st.subheader("Detected Anomalies")
            st.dataframe(
                anomalies[['timestamp', 'heart_rate']],
                column_config={
                    "timestamp": "Timestamp",
                    "heart_rate": "Heart Rate (bpm)"
                }
            )
            logger.info(f"Displayed {len(anomalies)} anomalies for {customer_id}")
        else:
            st.write("No anomalies detected for this customer.")
            logger.info(f"No anomalies found for {customer_id}")
    else:
        st.write("No data available for this customer.")
        logger.warning(f"No data found for customer {customer_id}")

if __name__ == "__main__":
    main()