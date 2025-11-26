import redis
import time
import json
import numpy as np
import psycopg2 
from sklearn.ensemble import IsolationForest 
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

# Configuration
STREAM_KEY = "log_stream"
GROUP_NAME = "ai_group"
CONSUMER_NAME = "worker_1"

# --- FAKE WEB SERVER FOR RENDER ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def start_health_check():
    # Render assigns a random port in the PORT env var
    port = int(os.getenv("PORT", 10000)) 
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"✅ Health Check Server running on port {port}")
    server.serve_forever()

# --- DATABASE & REDIS ---
def get_db_connection():
    db_url = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/logs_db")
    return psycopg2.connect(db_url)

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
r = redis.from_url(redis_url, decode_responses=True)

class AnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(contamination=0.1)
        self.is_trained = False

    def train(self):
        print("Training Anomaly Model...", end="")
        data_short = np.random.normal(loc=15, scale=5, size=(500, 1))
        data_long = np.random.normal(loc=40, scale=10, size=(500, 1))
        X_train = np.concatenate([data_short, data_long])
        self.model.fit(X_train)
        self.is_trained = True
        print(" Done!")

    def predict(self, message):
        features = np.array([[len(message)]])
        return self.model.predict(features)[0]

# --- INIT ---
detector = AnomalyDetector()
detector.train()

def create_consumer_group():
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
    except:
        pass

def save_log(log_id, log_data, is_anomaly):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO logs (log_id, service, level, message, is_anomaly)
            VALUES (%s, %s, %s, %s, %s)
        """, (log_id, log_data.get('service'), log_data.get('level'), log_data.get('message'), is_anomaly))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Error: {e}")

def process_log(log_id, log_data):
    message = log_data.get('message', '')
    service = log_data.get('service', 'unknown')
    prediction = detector.predict(message)
    is_anomaly = True if prediction == -1 else False
    status_icon = "ANOMALY" if is_anomaly else "Normal"
    print(f"[{service}] {status_icon} | ID: {log_id}")
    save_log(log_id, log_data, is_anomaly)

def main():
    print("Full-Stack Worker Started...")
    
    # START THE FAKE SERVER IN BACKGROUND
    # This keeps Render happy so it doesn't kill the app
    threading.Thread(target=start_health_check, daemon=True).start()
    
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
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()