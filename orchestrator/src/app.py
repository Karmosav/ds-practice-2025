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

orderqueue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/orderqueue'))
sys.path.insert(0, orderqueue_grpc_path)
import orderqueue_pb2 as orderqueue
import orderqueue_pb2_grpc as orderqueue_grpc

orchestrator_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/orchestrator'))
sys.path.insert(0, orchestrator_grpc_path)
import orchestrator_pb2 as orchestrator
import orchestrator_pb2_grpc as orchestrator_grpc


_ORDER_FAILURES = {}
_ORDER_FAILURES_LOCK = threading.Lock()
_FAILURE_GRPC_SERVER = None


class OrchestratorFailureService(orchestrator_grpc.OrchestratorServiceServicer):
    def ReportFailure(self, request, context):
        order_id = request.order_id
        service_name = request.service_name or "unknown"
        message = request.message or ""
        vector_clock = list(request.vector_clock)

        logging.error(
            "[ORCH][gRPC] Failure reported order=%s service=%s msg=%s vc=%s",
            order_id,
            service_name,
            message,
            vector_clock,
        )

        with _ORDER_FAILURES_LOCK:
            _ORDER_FAILURES.setdefault(order_id, []).append(
                {
                    "service": service_name,
                    "message": message,
                    "vectorClock": vector_clock,
                }
            )

        return orchestrator.Ack(ok=True)


def _start_failure_grpc_server():
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    orchestrator_grpc.add_OrchestratorServiceServicer_to_server(
        OrchestratorFailureService(), server
    )
    server.add_insecure_port("[::]:50060")
    server.start()
    logging.info("Orchestrator gRPC failure listener running on 50060")
    return server

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

        with ThreadPoolExecutor(max_workers=3) as thread_executor:
            futures = [
                thread_executor.submit(initialize_order, order_id, request_data, "transaction_verification"),
                thread_executor.submit(initialize_order, order_id, request_data, "fraud_detection"),
                thread_executor.submit(initialize_order, order_id, request_data, "suggestions"),
            ]
            if not all(f.result() for f in futures):
                return {"error": "Failed to initialize all services"}, 500

        # Start the checkout flow by calling transaction verification, which will orchestrate the rest of the flow
        with grpc.insecure_channel("transaction_verification:50052") as channel:
            stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
            final_resp = stub.StartCheckoutFlow(
                transaction_verification.StartCheckoutFlowRequest(order_id=order_id)
            )

        # Based on the final response, if the order is approved, 
        # enqueue it for execution and return the appropriate response to the client.
        # Default behavior is to not enqueue if the order is not approved
        enqueue_result = {
            "attempted": bool(final_resp.success),
            "enqueued": False,
            "message": "Order not approved, enqueue skipped",
            "queueSize": 0,
        }

        # Enqueue the order for execution if it is approved
        if final_resp.success:
            with grpc.insecure_channel("orderqueue:50054") as channel:
                # Enqueue the order for execution by the order executor service
                queue_stub = orderqueue_grpc.OrderQueueServiceStub(channel)
                enqueue_response = queue_stub.Enqueue(
                    orderqueue.EnqueueRequest(
                        order=orderqueue.OrderEnvelope(
                            order_id=order_id,
                            order_payload_json=json.dumps(request_data),
                        )
                    )
                )
                # successfully enqueued if the order was added to the queue
                enqueue_result = {
                    "attempted": True,
                    "enqueued": bool(enqueue_response.enqueued),
                    "message": enqueue_response.message,
                    "queueSize": enqueue_response.queue_size,
                }

        final_vector_clock = list(final_resp.vector_clock)
        cleanup_results = []
        with ThreadPoolExecutor(max_workers=3) as thread_executor:
            cleanup_services = ["transaction_verification", "fraud_detection", "suggestions"]
            cleanup_futures = {
                thread_executor.submit(cleanup_order, order_id, final_vector_clock, service_name): service_name
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
            "success": final_resp.success,
            "status": final_resp.message,
            "finalVectorClock": final_vector_clock,
            "enqueue": enqueue_result,
            "suggestedBooks": [
                {"title": s.title, "author": s.author}
                for s in final_resp.suggestions
            ],
            "cleanup": {
                "success": cleanup_ok,
                "results": cleanup_results,
            },
        }

    except Exception as e:
        logging.exception("Checkout failed")
        return {"error": str(e)}, 500


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Keep a strong reference so the gRPC server stays alive for the whole process lifetime.
    _FAILURE_GRPC_SERVER = _start_failure_grpc_server()
    app.run(host="0.0.0.0", port=5000)