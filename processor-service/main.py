import redis
import time
import json
import os

# Configuration
STREAM_KEY = "log_stream"
GROUP_NAME = "ai_group"
CONSUMER_NAME = "worker_1" # In production, this would be a unique UUID (e.g., hostname)

# 1. Connect to Redis
# Again, using localhost for now. In Docker, this will be "redis_queue"
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def create_consumer_group():
    """
    We must create the group before we can read from it.
    '0' means 'start from the very first message ever sent'.
    """
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id='0', mkstream=True)
        print(f"Consumer Group '{GROUP_NAME}' created.")
    except redis.exceptions.ResponseError as e:
        # If group already exists, Redis throws an error. We ignore it.
        if "BUSYGROUP" in str(e):
            print(f"Consumer Group '{GROUP_NAME}' already exists.")
        else:
            raise e

def process_log(log_id, log_data):
    """
    This is where the AI Magic will happen later.
    For now, we just simulate work.
    """
    print(f"Processing Log {log_id}: {log_data}")
    
    # Simulate AI processing time
    # time.sleep(0.1) 
    
    # TODO: Insert into Database here

def main():
    print("Worker Interface Initiated...")
    create_consumer_group()

    while True:
        try:
            # 2. READ from the Group
            # xreadgroup params:
            # - group: Who are we?
            # - consumer: Which specific worker?
            # - streams: {STREAM_KEY: '>'} 
            #   The '>' symbol is MAGIC. It means "Give me new messages that NO OTHER worker has seen yet."
            # Pass arguments in order: Group Name, Consumer Name, Streams Dict
            entries = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: '>'}, count=1, block=2000)

            if not entries:
                # No new mail. Wait and try again.
                continue

            # 3. Parse the message
            # Redis returns a nested list: [[stream_name, [[id, data], [id, data]]]]
            for stream, messages in entries:
                for log_id, log_data in messages:
                    process_log(log_id, log_data)

                    # 4. ACKNOWLEDGE (The most critical step)
                    # We tell Redis: "I am done. You can mark this as processed."
                    # If we don't do this, the message stays in "Pending" forever.
                    r.xack(STREAM_KEY, GROUP_NAME, log_id)
        
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()