import redis
import time
import json
import numpy as np
from sklearn.ensemble import IsolationForest

# Configuration
STREAM_KEY = "log_stream"
GROUP_NAME = "ai_group"
CONSUMER_NAME = "worker_1"

# --- THE AI BRAIN ---
class AnomalyDetector:
    def __init__(self):
        self.model = IsolationForest(contamination=0.1) # 10% of data is expected to be anomalous
        self.is_trained = False

    def train(self):
        """
        In production, you load a .pkl file here.
        For this demo, we train on dummy 'normal' data (Log lengths).
        """
        print("🧠 Training Anomaly Model...", end="")
        
        # We assume 'Normal' logs have a length between 10 and 50 characters.
        # We generate 1000 random data points to teach the model "Normality".
        X_train = np.random.normal(loc=30, scale=10, size=(1000, 1))
        
        self.model.fit(X_train)
        self.is_trained = True
        print(" Done!")

    def predict(self, message):
        """
        Returns: -1 for Anomaly, 1 for Normal
        """
        # We use 'Message Length' as the feature. 
        # In a real job, you would use TF-IDF or BERT embeddings.
        features = np.array([[len(message)]])
        return self.model.predict(features)[0]

# --- INIT ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
detector = AnomalyDetector()
detector.train() # Train immediately on startup

def create_consumer_group():
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
    except:
        pass

def process_log(log_id, log_data):
    message = log_data.get('message', '')
    service = log_data.get('service', 'unknown')
    
    # AI PREDICTION
    prediction = detector.predict(message)
    is_anomaly = True if prediction == -1 else False
    
    status_icon = "🚨 ANOMALY DETECTED" if is_anomaly else "✅ Normal"
    
    print(f"[{service}] {status_icon} | Msg Len: {len(message)} | ID: {log_id}")

    # TODO: Next step - Save to SQL Database

def main():
    print("🚀 AI Worker Started...")
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