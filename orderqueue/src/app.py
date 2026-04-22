import json
import threading
import grpc
from concurrent import futures
from utils.pb.orderqueue import orderqueue_pb2 as orderqueue
from utils.pb.orderqueue import orderqueue_pb2_grpc as orderqueue_grpc


class OrderQueueService(orderqueue_grpc.OrderQueueServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []

    # gRPC method implementations below
    # Each method acquires the lock to ensure thread safety when accessing shared state
    def Enqueue(self, request, context):
        order = request.order

        # Basic validation to ensure order_id is provided
        if not order.order_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("order_id is required")
            return orderqueue.EnqueueResponse(enqueued=False, message="order_id is required", queue_size=0)

        # Enqueue the order and return the new queue size.
        with self._lock:
            self._queue.append(
                orderqueue.OrderEnvelope(
                    order_id=order.order_id,
                    order_payload_json=order.order_payload_json,
                )
            )
            queue_size = len(self._queue)

        print(f"[Q] Enqueued order={order.order_id} size={queue_size}")
        return orderqueue.EnqueueResponse(
            enqueued=True,
            message="order enqueued",
            queue_size=queue_size,
        )

    def Dequeue(self, request, context):
        with self._lock:
            if not self._queue:
                return orderqueue.DequeueResponse(
                    has_order=False,
                    message="queue is empty",
                )
            # pop the next order
            next_order = self._queue.pop(0)

        print(
            f"[Q] Dequeued order={next_order.order_id} by executor={request.executor_id} "
            f"(inventory updated)"
        )
        return orderqueue.DequeueResponse(
            has_order=True,
            order=next_order,
            message="order dequeued",
        )


def serve_queue_service():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    orderqueue_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueService(), server)
    server.add_insecure_port("[::]:50054")
    server.start()
    print("Order queue service listening on 50054")
    server.wait_for_termination()


if __name__ == "__main__":
    serve_queue_service()
