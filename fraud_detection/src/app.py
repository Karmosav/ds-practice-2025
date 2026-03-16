import sys
import os
import json
import threading
import grpc
import joblib
import re
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc


MY_IDX = 1
ORDER_CACHE = {}
ORDER_CACHE_LOCK = threading.Lock()
TOTAL_SERVICES = 3

fraud_ai = joblib.load("./fraud_detection/ai/fraud_model.joblib")

# initial vector clock is [0,0,0]
def zero_vc():
    return [0] * TOTAL_SERVICES

# Utility functions for vector clock comparison and merging
def vc_max(a, b):
    return [max(x, y) for x, y in zip(a, b)]

# Returns True if vc_a <= vc_b (i.e. vc_a is causally before or concurrent with vc_b)
def vc_leq(a, b):
    return all(x <= y for x, y in zip(a, b))

# Returns a new vector clock that is the merge of local_vc and incoming_vc, and increments this service's index to reflect the new event
def merge_and_increment(local_vc, incoming_vc, my_idx):
    merged = vc_max(local_vc, incoming_vc)
    merged[my_idx] += 1
    return merged

# The gRPC service definitions for the fraud detection service, generated from the .proto file. We will implement the server methods in the FraudDetectionService class below.
def _safe_card_to_int(card_number):
    digits = re.sub(r"\D", "", str(card_number))
    if not digits:
        return 0
    # Keep feature bounded and aligned with training-style numeric signal.
    return int(digits[-16:])


class OrderState:
    def __init__(self, order_data):
        self.order_data = order_data
        self.local_vc = zero_vc() # vector clock tracking this service's view of the order state
        self.event_vc = {}
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)


class FraudDetectionService(fraud_detection_grpc.FraudDetectionServiceServicer):
    def _get_state_or_abort(self, order_id, context):
        # Helper to get the order state or abort the gRPC call if the order is not found
        with ORDER_CACHE_LOCK: # we need to lock the cache to safely read the order state
            state = ORDER_CACHE.get(order_id)
        if state is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Order {order_id} not initialized")
        return state

    def _remove_order(self, order_id):
        with ORDER_CACHE_LOCK:
            ORDER_CACHE.pop(order_id, None)

    # This is the main entry point to initialize the order state in the fraud detection service. 
    # It will be called by the orchestrator at the start of the checkout flow, and it sets up the initial vector clock and order data for this service.
    def InitOrder(self, request, context):
        try:
            order_data = json.loads(request.order_payload_json or "{}")
            state = OrderState(order_data)
            state.local_vc = merge_and_increment(zero_vc(), list(request.vector_clock), MY_IDX) # merge with incoming vc and increment for this event

            with ORDER_CACHE_LOCK:
                ORDER_CACHE[request.order_id] = state

            print(f"[FD] InitOrder {request.order_id} vc={state.local_vc}")
            return fraud_detection.InitOrderResponse(acknowledged=True)
        except Exception as e:
            # In case of any error, we return an INTERNAL gRPC error with the exception details.
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return fraud_detection.InitOrderResponse(acknowledged=False)

    # Explicit event RPC: d
    def CheckUserFraud(self, request, context):
        state = self._get_state_or_abort(request.order_id, context)
        if state is None:
            return fraud_detection.OrderEventResponse(
                fail=True,
                message="Order not initialized",
                vc=zero_vc(),
            )

        with state.cond:
            incoming_vc = list(request.incoming_vc)
            state.local_vc = merge_and_increment(state.local_vc, incoming_vc, MY_IDX)
            user = state.order_data.get("user", {})
            suspicious = "fraud" in str(user.get("name", "")).lower()
            state.event_vc["d"] = list(state.local_vc)
            vc_out = list(state.local_vc)
            print(f"[FD] event d vc={state.local_vc}")

        return fraud_detection.OrderEventResponse(
            fail=suspicious,
            message="Order Declined: fraud detected in user data" if suspicious else "OK",
            vc=vc_out,
        )

    # Explicit event RPC: e
    def CheckCardFraud(self, request, context):
        state = self._get_state_or_abort(request.order_id, context)
        if state is None:
            return fraud_detection.OrderEventResponse(
                fail=True,
                message="Order not initialized",
                vc=zero_vc(),
            )

        with state.cond:
            incoming_vc = list(request.incoming_vc)
            state.local_vc = merge_and_increment(state.local_vc, incoming_vc, MY_IDX)

            card = state.order_data.get("creditCard", {})
            number = str(card.get("number", ""))
            amount = float(card.get("orderAmount", 0))
            prediction = fraud_ai.predict([[amount, _safe_card_to_int(number)]])[0]
            suspicious = bool(prediction)

            state.event_vc["e"] = list(state.local_vc)
            vc_out = list(state.local_vc)
            print(f"[FD] event e vc={state.local_vc}")

        return fraud_detection.OrderEventResponse(
            fail=suspicious,
            message="Order Declined: card fraud suspected" if suspicious else "OK",
            vc=vc_out,
        )

    # Final cleanup event broadcast by orchestrator.
    # Service only clears cached order state if local_vc <= final_vector_clock.
    def CleanupOrder(self, request, context):
        with ORDER_CACHE_LOCK:
            state = ORDER_CACHE.get(request.order_id)

        if state is None:
            return fraud_detection.CleanupOrderResponse(
                cleaned=True,
                vc_valid=True,
                message="Order not found (already cleaned or never initialized)",
                local_vector_clock=zero_vc(),
            )

        final_vc = list(request.final_vector_clock)
        with state.cond:
            local_vc = list(state.local_vc)
            is_valid = vc_leq(local_vc, final_vc)

        if is_valid:
            self._remove_order(request.order_id)
            print(f"[FD] CleanupOrder order={request.order_id} local_vc={local_vc} final_vc={final_vc} status=cleaned")
            return fraud_detection.CleanupOrderResponse(
                cleaned=True,
                vc_valid=True,
                message="Cleanup successful",
                local_vector_clock=local_vc,
            )

        print(f"[FD] CleanupOrder order={request.order_id} local_vc={local_vc} final_vc={final_vc} status=rejected")
        return fraud_detection.CleanupOrderResponse(
            cleaned=False,
            vc_valid=False,
            message="Cleanup rejected: local vector clock is not <= final vector clock",
            local_vector_clock=local_vc,
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # We add the FraudDetectionServiceServicer to the gRPC server, which will handle incoming gRPC requests for the fraud detection service. 
    # The actual logic for handling the requests is implemented in the methods of the FraudDetectionService class above.
    fraud_detection_grpc.add_FraudDetectionServiceServicer_to_server(
        FraudDetectionService(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Fraud service listening on 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()