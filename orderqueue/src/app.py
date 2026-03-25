import sys
import os
import time
import threading
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

orderqueue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/orderqueue'))
sys.path.insert(0, orderqueue_grpc_path)
import orderqueue_pb2 as orderqueue
import orderqueue_pb2_grpc as orderqueue_grpc

LEADER_LEASE_SECONDS = 5


class OrderQueueService(orderqueue_grpc.OrderQueueServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []
        self._heartbeats = {}
        self._leader_id = ""

    def _now(self):
        return time.time()

    # Lease expiry is current time + lease duration, returned in milliseconds for easier client comparisons.
    # A timestamp in ms since epoch when this leader lease stops being valid
    def _lease_expiry_ms(self):
        return int((self._now() + LEADER_LEASE_SECONDS) * 1000)

    # Returns list of executor ids that have sent a heartbeat within the lease duration, 
    # and also cleans up expired heartbeats.
    def _alive_candidates_locked(self):
        now = self._now()
        alive = []
        expired = []
        for executor_id, last_heartbeat in self._heartbeats.items():
            # An executor is considered alive if its last heartbeat was within the lease duration.
            if (now - last_heartbeat) <= LEADER_LEASE_SECONDS:
                alive.append(executor_id)
            else:
                expired.append(executor_id)

        # Clean up expired heartbeats to prevent memory growth over time. 
        # This also ensures that if an executor goes down, it will eventually 
        # be removed from consideration for leadership.
        for executor_id in expired:
            self._heartbeats.pop(executor_id, None)

        return alive

    # Bully-style choice: highest executor id among alive candidates wins.
    def _refresh_leader_locked(self):
        alive = self._alive_candidates_locked()
        if not alive:
            self._leader_id = ""
            return ""

        self._leader_id = max(alive)
        return self._leader_id

    # Determines if the requester should be granted leadership based on current heartbeats 
    # and updates state accordingly.
    def _leader_response_locked(self, requester_id):
        leader_id = self._refresh_leader_locked()
        granted = bool(leader_id) and requester_id == leader_id
        message = "leader lease granted" if granted else "follower"
        if not leader_id:
            message = "no active leader"
        return orderqueue.LeaderResponse(
            granted=granted,
            leader_id=leader_id,
            lease_expires_unix_ms=self._lease_expiry_ms(),
            message=message,
        )

    # gRPC method implementations below
    # Each method acquires the lock to ensure thread safety when accessing shared state
    def Enqueue(self, request, context):
        order = request.order

        # Basic validation to ensure order_id is provided
        if not order.order_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("order_id is required")
            return orderqueue.EnqueueResponse(enqueued=False, message="order_id is required", queue_size=0)

        # Enqueue the order and return the new queue size. This does not require the requester to be the leader,
        # as we want to allow any service (like the orchestrator) to add orders to the queue regardless of leadership status.
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

    # Only the current leader is allowed to dequeue orders for execution. 
    # This ensures that there is a single source of truth for order processing
    def Dequeue(self, request, context):
        with self._lock:

            # check if leader
            current_leader = self._refresh_leader_locked()
            if not current_leader or request.executor_id != current_leader:
                return orderqueue.DequeueResponse(
                    has_order=False,
                    message=f"dequeue denied, current leader={current_leader}",
                )

            # if no queue
            if not self._queue:
                return orderqueue.DequeueResponse(
                    has_order=False,
                    message="queue is empty",
                )

            next_order = self._queue.pop(0)

        print(f"[Q] Dequeued order={next_order.order_id} by leader={request.executor_id}")
        return orderqueue.DequeueResponse(
            has_order=True,
            order=next_order,
            message="order dequeued",
        )

    # Executors must register and send heartbeats to be considered for leadership. 
    # This method handles both registration and heartbeat updates.
    def RegisterExecutor(self, request, context):
        # Basic validation to ensure executor_id is provided
        if not request.executor_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("executor_id is required")
            return orderqueue.LeaderResponse(granted=False, leader_id="", lease_expires_unix_ms=0, message="executor_id is required")

        # Update the heartbeat for the registering executor and determine leadership status.
        with self._lock:
            self._heartbeats[request.executor_id] = self._now()
            response = self._leader_response_locked(request.executor_id)

        print(f"[Q] RegisterExecutor id={request.executor_id} leader={response.leader_id}")
        return response

    # Executors must send periodic heartbeats to maintain their candidacy for leadership. 
    # This method updates the heartbeat timestamp
    def Heartbeat(self, request, context):
        if not request.executor_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("executor_id is required")
            return orderqueue.LeaderResponse(granted=False, leader_id="", lease_expires_unix_ms=0, message="executor_id is required")

        with self._lock:
            self._heartbeats[request.executor_id] = self._now()
            return self._leader_response_locked(request.executor_id)

    # This method allows any requester to query the current leader without affecting heartbeat timestamps.
    def GetLeader(self, request, context):
        with self._lock:
            leader_id = self._refresh_leader_locked()
            return orderqueue.LeaderResponse(
                granted=False,
                leader_id=leader_id,
                lease_expires_unix_ms=self._lease_expiry_ms(),
                message="current leader",
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
