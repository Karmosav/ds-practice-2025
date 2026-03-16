import sys
import os
import json
import threading
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc


SERVICE_NAME = "transaction_verification"
MY_IDX = 0
ORDER_CACHE = {}
ORDER_CACHE_LOCK = threading.Lock()
TOTAL_SERVICES = 3


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


class OrderState:
    def __init__(self, order_data):
        self.order_data = order_data
        self.local_vc = zero_vc() # vector clock tracking this service's view of the order state
        self.event_vc = {} # vector clocks for completed events, e.g. {"a": [1,0,0], "b": [2,0,0]}
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)


class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationServiceServicer):
    def _get_state_or_abort(self, order_id, context):
        # Helper to get the order state or abort the gRPC call if the order is not found
        with ORDER_CACHE_LOCK:
            state = ORDER_CACHE.get(order_id)
        if state is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Order {order_id} not initialized")
        return state

    def _remove_order(self, order_id):
        with ORDER_CACHE_LOCK:
            ORDER_CACHE.pop(order_id, None)

    # This is the main entry point to initialize the order state in the transaction verification service.
    # It is called by the orchestrator at checkout start, and sets up local state and initial vector clock.
    def InitOrder(self, request, context):
        try:
            order_data = json.loads(request.order_payload_json or "{}")
            print(f"[TV] InitOrder {request.order_id} payload={order_data} vc={request.vector_clock}")
            state = OrderState(order_data)
            state.local_vc = merge_and_increment(zero_vc(), list(request.vector_clock), MY_IDX)

            with ORDER_CACHE_LOCK:
                ORDER_CACHE[request.order_id] = state

            print(f"[TV] InitOrder {request.order_id} vc={state.local_vc}")
            return transaction_verification.InitOrderResponse(acknowledged=True)
        except Exception as e:
            # In case of any error, we return an INTERNAL gRPC error with the exception details.
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return transaction_verification.InitOrderResponse(acknowledged=False)

    # Explicit event RPC: a
    def VerifyItems(self, request, context):
        state = self._get_state_or_abort(request.order_id, context)
        if state is None:
            return transaction_verification.OrderEventResponse(
                fail=True,
                message="Order not initialized",
                vc=zero_vc(),
            )

        with state.cond:
            incoming_vc = list(request.incoming_vc)
            state.local_vc = merge_and_increment(state.local_vc, incoming_vc, MY_IDX)
            items = state.order_data.get("items", [])
            ok = len(items) > 0
            state.event_vc["a"] = list(state.local_vc)
            vc_out = list(state.local_vc)
            print(f"[TV] event a vc={state.local_vc}")

        return transaction_verification.OrderEventResponse(
            fail=not ok,
            message="OK" if ok else "Order Declined: no items in order",
            vc=vc_out,
        )

    # Explicit event RPC: b
    def VerifyUserData(self, request, context):
        state = self._get_state_or_abort(request.order_id, context)
        if state is None:
            return transaction_verification.OrderEventResponse(
                fail=True,
                message="Order not initialized",
                vc=zero_vc(),
            )

        with state.cond:
            incoming_vc = list(request.incoming_vc)
            state.local_vc = merge_and_increment(state.local_vc, incoming_vc, MY_IDX)
            user = state.order_data.get("user", {})
            billing = state.order_data.get("billingAddress", {})
            ok = bool(user.get("name")) and bool(user.get("contact")) and bool(billing.get("street"))
            state.event_vc["b"] = list(state.local_vc)
            vc_out = list(state.local_vc)
            print(f"[TV] event b vc={state.local_vc}")

        return transaction_verification.OrderEventResponse(
            fail=not ok,
            message="OK" if ok else "Order Declined: mandatory user data missing",
            vc=vc_out,
        )

    # Explicit event RPC: c
    def VerifyCardFormat(self, request, context):
        state = self._get_state_or_abort(request.order_id, context)
        if state is None:
            return transaction_verification.OrderEventResponse(
                fail=True,
                message="Order not initialized",
                vc=zero_vc(),
            )

        with state.cond:
            incoming_vc = list(request.incoming_vc)
            state.local_vc = merge_and_increment(state.local_vc, incoming_vc, MY_IDX)

            card = state.order_data.get("creditCard", {})
            number = str(card.get("number", ""))
            expiry = str(card.get("expirationDate", ""))
            cvv = str(card.get("cvv", ""))
            ok = (
                number.isdigit() and len(number) >= 12 and
                len(expiry) == 5 and expiry[2] == "/" and
                cvv.isdigit() and len(cvv) == 3
            )

            state.event_vc["c"] = list(state.local_vc)
            vc_out = list(state.local_vc)
            print(f"[TV] event c vc={state.local_vc}")

        return transaction_verification.OrderEventResponse(
            fail=not ok,
            message="OK" if ok else "Order Declined: invalid credit card format",
            vc=vc_out,
        )

    # Final cleanup event broadcast by orchestrator.
    # Service only clears cached order state if local_vc <= final_vector_clock.
    def CleanupOrder(self, request, context):
        with ORDER_CACHE_LOCK:
            state = ORDER_CACHE.get(request.order_id)

        if state is None:
            return transaction_verification.CleanupOrderResponse(
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
            print(f"[TV] CleanupOrder order={request.order_id} local_vc={local_vc} final_vc={final_vc} status=cleaned")
            return transaction_verification.CleanupOrderResponse(
                cleaned=True,
                vc_valid=True,
                message="Cleanup successful",
                local_vector_clock=local_vc,
            )

        print(f"[TV] CleanupOrder order={request.order_id} local_vc={local_vc} final_vc={final_vc} status=rejected")
        return transaction_verification.CleanupOrderResponse(
            cleaned=False,
            vc_valid=False,
            message="Cleanup rejected: local vector clock is not <= final vector clock",
            local_vector_clock=local_vc,
        )

def serve():
    # Bootstraps and starts the gRPC server for this service.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(
        TransactionVerificationService(), server
    )
    server.add_insecure_port("[::]:50052")
    server.start()
    print("Transaction service listening on 50052")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()