from locust import HttpUser, task, between
import random

services = ["auth-service", "payment-gateway", "shipping-worker", "db-shard-01"]
levels = ["info", "info", "info", "warn", "error"] # Weighted: mostly info

class LogGenerator(HttpUser):
    # Wait between 0.1 and 0.5 seconds per user (aggressive)
    wait_time = between(0.1, 0.5) 

    @task
    def send_log(self):
        # 1. Randomize Data
        service = random.choice(services)
        level = random.choice(levels)
        msg_len = random.randint(10, 60) # Mix of short and long
        message = "X" * msg_len # Dummy message
        
        payload = {
            "service": service,
            "level": level,
            "message": message
        }

        # 2. Hit the Go API
        self.client.post("/ingest", json=payload)