import redis
import time
import json
import numpy as np
import psycopg2 
from sklearn.ensemble import IsolationForest

# Configuration
STREAM_KEY = "log_stream"
GROUP_NAME = "ai_group"
CONSUMER_NAME = "worker_1"

# --- DATABASE CONNECTION ---
def get_db_connection():
    return psycopg2.connect(
        host="localhost", # In Docker, use "postgres_db" if running python inside docker
        database="logs_db",
        user="user",
        password="password",
        port="5432"
    )

# --- THE AI BRAIN ---
class AnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(contamination=0.1)
        self.is_trained = False

    def train(self):
        print("🧠 Training Anomaly Model...", end="")
        X_train = np.random.normal(loc=30, scale=10, size=(1000, 1))
        self.model.fit(X_train)
        self.is_trained = True
        print(" Done!")

    def predict(self, message):
        features = np.array([[len(message)]])
        return self.model.predict(features)[0]

# --- INIT ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
detector = AnomalyDetector()
detector.train()

def create_consumer_group():
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
    except:
        pass

def save_log(log_id, log_data, is_anomaly):
    """
    Inserts the processed log into PostgreSQL
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO logs (log_id, service, level, message, is_anomaly)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            log_id, 
            log_data.get('service'), 
            log_data.get('level'), 
            log_data.get('message'), 
            is_anomaly
        ))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")

def process_log(log_id, log_data):
    message = log_data.get('message', '')
    service = log_data.get('service', 'unknown')
    
    # AI PREDICTION
    prediction = detector.predict(message)
    is_anomaly = True if prediction == -1 else False
    
    status_icon = "🚨 ANOMALY" if is_anomaly else "✅ Normal"
    print(f"[{service}] {status_icon} | ID: {log_id} | Saved to DB")

    # SAVE TO DB
    save_log(log_id, log_data, is_anomaly)

def main():
    print("🚀 Full-Stack Worker Started...")
    create_consumer_group()

    while True:
        try:
            entries = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: '>'}, count=1, block=2000)
            
            if not entries:
                continue

            for stream, messages in entries:
                for log_id, log_data in messages:
                    process_log(log_id, log_data)
                    r.xack(STREAM_KEY, GROUP_NAME, log_id)
        
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()