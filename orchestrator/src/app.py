import sys
import os
import grpc
import json
import logging
import threading
from flask import Flask, request
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor
import uuid

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

# Global in-memory cache to track order states and vector clocks for simplicity
def initialize_order(order_id, order_data, service_name):
    payload = json.dumps(order_data)
    initial_vc = [0, 0, 0]

    if service_name == "transaction_verification":
        # Initialize the order with the transaction verification service
        with grpc.insecure_channel("transaction_verification:50052") as channel:
            # Call the InitOrder method on the transaction verification service to set up the initial state for this order
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
            return stub.InitOrder(
                transaction_verification.InitOrderRequest(
                    order_id=order_id,
                    order_payload_json=payload,
                    vector_clock=initial_vc
                )
            ).acknowledged

    if service_name == "fraud_detection":
        with grpc.insecure_channel("fraud_detection:50051") as channel:
            stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
            return stub.InitOrder(
                fraud_detection.InitOrderRequest(
                    order_id=order_id,
                    order_payload_json=payload,
                    vector_clock=initial_vc
                )
            ).acknowledged

    if service_name == "suggestions":
        with grpc.insecure_channel("suggestions:50053") as channel:
            stub = suggestions_grpc.SuggestionsServiceStub(channel)
            return stub.InitOrder(
                suggestions.InitOrderRequest(
                    order_id=order_id,
                    order_payload_json=payload,
                    vector_clock=initial_vc
                )
            ).acknowledged

    raise ValueError(f"Unknown service {service_name}")


def cleanup_order(order_id, final_vector_clock, service_name):
    if service_name == "transaction_verification":
        with grpc.insecure_channel("transaction_verification:50052") as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
            response = stub.CleanupOrder(
                transaction_verification.CleanupOrderRequest(
                    order_id=order_id,
                    final_vector_clock=final_vector_clock,
                )
            )
            return {
                "service": service_name,
                "cleaned": response.cleaned,
                "vcValid": response.vc_valid,
                "message": response.message,
                "localVectorClock": list(response.local_vector_clock),
            }

    if service_name == "fraud_detection":
        with grpc.insecure_channel("fraud_detection:50051") as channel:
            stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
            response = stub.CleanupOrder(
                fraud_detection.CleanupOrderRequest(
                    order_id=order_id,
                    final_vector_clock=final_vector_clock,
                )
            )
            return {
                "service": service_name,
                "cleaned": response.cleaned,
                "vcValid": response.vc_valid,
                "message": response.message,
                "localVectorClock": list(response.local_vector_clock),
            }

    if service_name == "suggestions":
        with grpc.insecure_channel("suggestions:50053") as channel:
            stub = suggestions_grpc.SuggestionsServiceStub(channel)
            response = stub.CleanupOrder(
                suggestions.CleanupOrderRequest(
                    order_id=order_id,
                    final_vector_clock=final_vector_clock,
                )
            )
            return {
                "service": service_name,
                "cleaned": response.cleaned,
                "vcValid": response.vc_valid,
                "message": response.message,
                "localVectorClock": list(response.local_vector_clock),
            }

    raise ValueError(f"Unknown service {service_name}")


app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': '*'}})


@app.route("/checkout", methods=["POST"])
def checkout():
   
    request_data = json.loads(request.data)
    print(f"[ORCH] Checkout request payload={request_data}")
    try:
        # Generate a unique order ID for this checkout flow and initialize all services in parallel
        order_id = str(uuid.uuid4())

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(initialize_order, order_id, request_data, "transaction_verification"),
                executor.submit(initialize_order, order_id, request_data, "fraud_detection"),
                executor.submit(initialize_order, order_id, request_data, "suggestions"),
            ]
            if not all(f.result() for f in futures):
                return {"error": "Failed to initialize all services"}, 500

        with grpc.insecure_channel("transaction_verification:50052") as tx_channel, \
             grpc.insecure_channel("fraud_detection:50051") as fd_channel, \
             grpc.insecure_channel("suggestions:50053") as sg_channel:

            tx_stub = transaction_verification_grpc.TransactionVerificationServiceStub(tx_channel)
            fd_stub = fraud_detection_grpc.FraudDetectionServiceStub(fd_channel)
            sg_stub = suggestions_grpc.SuggestionsServiceStub(sg_channel)

            stop_event = threading.Event()
            failure_message = [None]
            final_vector_clock = [[0, 0, 0]]
            suggestions_out = [[]]
            state_lock = threading.Lock()

            def merge_vc(vc1, vc2):
                return [max(vc1[i], vc2[i]) for i in range(3)]

            def mark_failure(message, vc):
                with state_lock:
                    if failure_message[0] is None:
                        failure_message[0] = message
                        final_vector_clock[0] = list(vc)
                stop_event.set()

            def run_event_a():
                if stop_event.is_set():
                    return None
                resp = tx_stub.VerifyItems(
                    transaction_verification.OrderEventRequest(order_id=order_id, incoming_vc=[0, 0, 0])
                )
                if resp.fail:
                    mark_failure(resp.message, resp.vc)
                    return None
                return resp

            def run_event_b():
                if stop_event.is_set():
                    return None
                resp = tx_stub.VerifyUserData(
                    transaction_verification.OrderEventRequest(order_id=order_id, incoming_vc=[0, 0, 0])
                )
                if resp.fail:
                    mark_failure(resp.message, resp.vc)
                    return None
                return resp

            with ThreadPoolExecutor(max_workers=6) as executor:
                future_a = executor.submit(run_event_a)
                future_b = executor.submit(run_event_b)

                def run_event_c():
                    resp_a = future_a.result()
                    if resp_a is None or stop_event.is_set():
                        return None
                    resp = tx_stub.VerifyCardFormat(
                        transaction_verification.OrderEventRequest(order_id=order_id, incoming_vc=list(resp_a.vc))
                    )
                    if resp.fail:
                        mark_failure(resp.message, resp.vc)
                        return None
                    return resp

                def run_event_d():
                    resp_b = future_b.result()
                    if resp_b is None or stop_event.is_set():
                        return None
                    resp = fd_stub.CheckUserFraud(
                        fraud_detection.OrderEventRequest(order_id=order_id, incoming_vc=list(resp_b.vc))
                    )
                    if resp.fail:
                        mark_failure(resp.message, resp.vc)
                        return None
                    return resp

                future_c = executor.submit(run_event_c)
                future_d = executor.submit(run_event_d)

                def run_event_e():
                    resp_c = future_c.result()
                    resp_d = future_d.result()
                    if resp_c is None or resp_d is None or stop_event.is_set():
                        return None
                    merged_vc = merge_vc(list(resp_c.vc), list(resp_d.vc))
                    resp = fd_stub.CheckCardFraud(
                        fraud_detection.OrderEventRequest(order_id=order_id, incoming_vc=merged_vc)
                    )
                    if resp.fail:
                        mark_failure(resp.message, resp.vc)
                        return None
                    return resp

                future_e = executor.submit(run_event_e)

                def run_event_f():
                    resp_e = future_e.result()
                    if resp_e is None or stop_event.is_set():
                        return None
                    resp = sg_stub.GenerateSuggestions(
                        suggestions.OrderEventRequest(order_id=order_id, incoming_vc=list(resp_e.vc))
                    )
                    if resp.fail:
                        mark_failure(resp.message, resp.vc)
                        return None
                    with state_lock:
                        final_vector_clock[0] = list(resp.vc)
                        suggestions_out[0] = [
                            {"title": s.title, "author": s.author} for s in resp.suggestions
                        ]
                    return resp

                future_f = executor.submit(run_event_f)
                future_f.result()

        success = not stop_event.is_set()
        status = "Order Approved" if success else (failure_message[0] or "Order Declined")

        cleanup_results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            cleanup_services = ["transaction_verification", "fraud_detection", "suggestions"]
            cleanup_futures = {
                executor.submit(cleanup_order, order_id, final_vector_clock[0], service_name): service_name
                for service_name in cleanup_services
            }
            for cleanup_future, service_name in cleanup_futures.items():
                try:
                    cleanup_results.append(cleanup_future.result())
                except Exception as cleanup_error:
                    cleanup_results.append(
                        {
                            "service": service_name,
                            "cleaned": False,
                            "vcValid": False,
                            "message": f"Cleanup call failed: {cleanup_error}",
                            "localVectorClock": [],
                        }
                    )

        cleanup_ok = all(item.get("cleaned") and item.get("vcValid") for item in cleanup_results)

        return {
            "orderId": order_id,
            "success": success,
            "status": status,
            "finalVectorClock": final_vector_clock[0],
            "suggestedBooks": suggestions_out[0],
            "cleanup": {
                "success": cleanup_ok,
                "results": cleanup_results,
            },
        }

    except Exception as e:
        logging.exception("Checkout failed")
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)