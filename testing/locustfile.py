from locust import HttpUser, task, between

class CheckoutUser(HttpUser):
    wait_time = between(1, 3)
    
    @task
    def complete_checkout(self):
        # Simulate full checkout flow through orchestrator
        self.client.post("/api/checkout", json={...})