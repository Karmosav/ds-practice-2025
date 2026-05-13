from locust import HttpUser, task, between
import random
import uuid

# Sample book names that match the database
AVAILABLE_BOOKS = [
    "Harry Potter and the Philosopher's Stone",
    "The Hobbit"
]

class CheckoutUser(HttpUser):
    wait_time = between(1, 3)

    def generate_base_order(self, items):
        return {
            "user": {
                "name": f"User {uuid.uuid4().hex[:6]}",
                "contact": "test@example.com"
            },
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/25",
                "cvv": "123"
            },
            "userComment": "Automated Locust test order",
            "items": items,
            "billingAddress": {
                "street": "123 Main St",
                "city": "Springfield",
                "state": "IL",
                "zip": "62701",
                "country": "USA"
            },
            "shippingMethod": "Standard",
            "giftWrapping": False,
            "termsAccepted": True
        }

    @task(4)
    def single_and_multiple_non_fraudulent_orders(self):
        """
        Scenarios 1 & 2: Single and Multiple non-fraudulent non-conflicting orders.
        Uses random books to minimize conflicting purchases during concurrent runs.
        """
        book = random.choice(AVAILABLE_BOOKS)
        payload = self.generate_base_order([{"name": str(book), "quantity": 1}])
        
        # Adjust URL endpoint depending on your Orchestrator routing (e.g., "/checkout" or "/api/checkout")
        with self.client.post("/checkout", json=payload, catch_response=True) as response:
            if response.status_code in [200, 202]:
                response.success()
            else:
                response.failure(f"Failed order, status {response.status_code}: {response.text}")

    @task(2)
    def mixed_fraudulent_order(self):
        """
        Scenario 3: Mixed fraudulent orders.
        Simulates fraud by specifying an invalid/stolen credit card or abnormal quantity.
        """
        book = random.choice(AVAILABLE_BOOKS)
        payload = self.generate_base_order([{"name": str(book), "quantity": 1}])
        # Simulate fraudulent card number (modify depending on your fraud_model criteria)
        payload["creditCard"]["number"] = "9999999999999999" 
        payload["userComment"] = "Fraudulent transaction simulation"
        
        with self.client.post("/checkout", json=payload, catch_response=True) as response:
            # We expect either a success block or a specific error (e.g. 400 Bad Request / 406 Not Acceptable)
            # The goal is that the system doesn't crash (500)
            if response.status_code < 500:
                response.success()
            else:
                response.failure(f"System crash on fraud handled poorly, status {response.status_code}")

    @task(1)
    def conflicting_orders(self):
        """
        Scenario 4: Conflicting orders.
        Hardcoded to aggressively request the exact same book to trigger DB locked/conflict state.
        """
        # Constantly buying the same book to trigger a conflict
        payload = self.generate_base_order([{"name": "CONFLICTING_BOOK_NAME", "quantity": 1}])
        payload["userComment"] = "Conflicting order simulation"
        
        with self.client.post("/checkout", json=payload, catch_response=True) as response:
            # 409 Conflict/400 Bad Request due to inventory are valid business logic responses
            if response.status_code in [200, 202, 400, 409]: 
                response.success()
            else:
                response.failure(f"Unexpected status on conflict scenario: {response.status_code}")