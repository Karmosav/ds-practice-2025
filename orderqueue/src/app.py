import sys
import os
import json
import time
import threading
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

orderqueue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/orderqueue'))
sys.path.insert(0, orderqueue_grpc_path)
import orderqueue_pb2 as orderqueue
import orderqueue_pb2_grpc as orderqueue_grpc

books_database_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/books_database'))
sys.path.insert(0, books_database_grpc_path)
import booksdatabase_pb2 as books_pb2
import booksdatabase_pb2_grpc as booksdatabase_grpc

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
            # pop the next order
            next_order = self._queue.pop(0)

            # Connect to the primary books database service
            db_channel = grpc.insecure_channel('booksdb-primary:50051')  # Use the correct service name/port
            db_stub = booksdatabase_grpc.BooksDatabaseStub(db_channel)

            # Execute the order (read, check, write) — logic lives in execute_order
            print(
                f"[Q] inventory workflow start order_id={next_order.order_id} "
                f"leader_executor={request.executor_id}"
            )
            success = self.execute_order(next_order.order_payload_json, db_stub)
            if not success:
                print(f"[Q] inventory workflow FAILED order_id={next_order.order_id}")
                return orderqueue.DequeueResponse(
                    has_order=False,
                    message="order could not be fulfilled against inventory",
                )

        print(
            f"[Q] Dequeued order={next_order.order_id} by leader={request.executor_id} "
            f"(inventory updated)"
        )
        return orderqueue.DequeueResponse(
            has_order=True,
            order=next_order,
            message="order dequeued",
        )
    
    def execute_order(self, order_payload_json, db_stub):
        """Read line items from checkout JSON, read stock, validate, write new stock (primary + replication)."""
        try:
            payload = json.loads(order_payload_json or "{}")
        except json.JSONDecodeError as err:
            print(f"[Q] execute_order: invalid JSON ({err})")
            return False

        def split_book_title(display_name):
            display_name = (display_name or "").strip()
            if not display_name:
                return ""
            if " by " in display_name:
                return display_name.split(" by ", 1)[0].strip()
            return display_name

        def positive_int(q):
            if q is None or isinstance(q, bool):
                return None
            if isinstance(q, int):
                return q if q > 0 else None
            if isinstance(q, float) and q > 0 and q.is_integer():
                return int(q)
            if isinstance(q, str):
                try:
                    v = int(q.strip(), 10)
                    return v if v > 0 else None
                except ValueError:
                    return None
            return None

        lines = []
        if isinstance(payload, dict):
            t = str(payload.get("title") or "").strip()
            q = positive_int(payload.get("quantity", 1))
            if t and q:
                lines = [(t, q)]
            else:
                for item in payload.get("items") or []:
                    if not isinstance(item, dict):
                        continue
                    raw = item.get("name") or item.get("title") or ""
                    t = split_book_title(str(raw))
                    q = positive_int(item.get("quantity"))
                    if t and q:
                        lines.append((t, q))

        if not lines:
            print("[Q] execute_order: no valid line items (missing title/quantity or empty items)")
            return False

        print(f"[Q] execute_order: parsed {len(lines)} line(s): {lines}")

        try:
            snapshots = []
            for title, qty in lines:
                response = db_stub.Read(books_pb2.ReadRequest(title=title))
                snapshots.append((title, qty, response.stock))
                print(
                    f"[Q] execute_order: Read ok title={title!r} requested_qty={qty} "
                    f"current_stock={response.stock}"
                )
            for title, qty, stock in snapshots:
                if stock < qty:
                    print(
                        f"[Q] execute_order: insufficient stock title={title!r} "
                        f"stock={stock} required={qty}"
                    )
                    return False
            print("[Q] execute_order: stock check passed for all lines")
            for title, qty, stock in snapshots:
                new_stock = stock - qty
                write_response = db_stub.Write(
                    books_pb2.WriteRequest(title=title, new_stock=new_stock)
                )
                print(
                    f"[Q] execute_order: Write title={title!r} stock {stock} -> {new_stock} "
                    f"success={write_response.success}"
                )
                if not write_response.success:
                    return False
        except grpc.RpcError as err:
            print(
                f"[Q] execute_order: books DB gRPC error code={err.code()} "
                f"details={err.details()!r}"
            )
            return False

        print("[Q] execute_order: all reads/writes completed successfully")
        return True
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
