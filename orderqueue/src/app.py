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

    def _lease_expiry_ms(self):
        return int((self._now() + LEADER_LEASE_SECONDS) * 1000)

    def _alive_candidates_locked(self):
        now = self._now()
        alive = []
        expired = []
        for executor_id, last_heartbeat in self._heartbeats.items():
            if (now - last_heartbeat) <= LEADER_LEASE_SECONDS:
                alive.append(executor_id)
            else:
                expired.append(executor_id)

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

    def Enqueue(self, request, context):
        order = request.order
        if not order.order_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("order_id is required")
            return orderqueue.EnqueueResponse(enqueued=False, message="order_id is required", queue_size=0)

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
            current_leader = self._refresh_leader_locked()
            if not current_leader or request.executor_id != current_leader:
                return orderqueue.DequeueResponse(
                    has_order=False,
                    message=f"dequeue denied, current leader={current_leader}",
                )

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

    def RegisterExecutor(self, request, context):
        if not request.executor_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("executor_id is required")
            return orderqueue.LeaderResponse(granted=False, leader_id="", lease_expires_unix_ms=0, message="executor_id is required")

        with self._lock:
            self._heartbeats[request.executor_id] = self._now()
            response = self._leader_response_locked(request.executor_id)

        print(f"[Q] RegisterExecutor id={request.executor_id} leader={response.leader_id}")
        return response

    def Heartbeat(self, request, context):
        if not request.executor_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("executor_id is required")
            return orderqueue.LeaderResponse(granted=False, leader_id="", lease_expires_unix_ms=0, message="executor_id is required")

        with self._lock:
            self._heartbeats[request.executor_id] = self._now()
            return self._leader_response_locked(request.executor_id)

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
