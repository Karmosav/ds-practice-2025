import os
import time
import threading
import json
import grpc
from concurrent import futures
from utils.pb.orderqueue import orderqueue_pb2 as orderqueue
from utils.pb.orderqueue import orderqueue_pb2_grpc as orderqueue_grpc
from utils.pb.executor import executor_pb2 as executor
from utils.pb.executor import executor_pb2_grpc as executor_grpc
from utils.pb.books_database import booksdatabase_pb2 as books_pb2
from utils.pb.books_database import booksdatabase_pb2_grpc as booksdatabase_grpc
from utils.pb.payment import payment_pb2 as payment_pb2
from utils.pb.payment import payment_pb2_grpc as payment_grpc

HEARTBEAT_INTERVAL_SECONDS = 1.0
LEADER_POLL_INTERVAL_SECONDS = 0.5
EMPTY_QUEUE_BACKOFF_SECONDS = 1.0
RETRY_BACKOFF_SECONDS = 2.0
PAYMENT_TARGET = os.getenv("PAYMENT_TARGET", "payment:50059")
RPC_TIMEOUT_SECONDS = 2.0

class ExecutorService(executor_grpc.OrderExecutorServiceServicer):
    def __init__(self, executor_id, queue_stub, peer_targets):
        self.executor_id = executor_id
        self.queue_stub = queue_stub
        # List of other executor gRPC endpoints, e.g. ["executor-1:50055", "executor-2:50055"]
        self.peer_targets = peer_targets
        self.leader_id = ""
        self.is_leader = False
        self.processed_orders = 0
        self._state_lock = threading.Lock()
    
    # This method handles status requests from clients, providing information about the executor's identity, 
    # leadership status, current leader, and number of processed orders.
    def GetStatus(self, request, context):
        with self._state_lock:
            return executor.GetStatusResponse(
                executor_id=self.executor_id,
                is_leader=self.is_leader,
                leader_id=self.leader_id,
                processed_orders=self.processed_orders,
            )

    # This method updates the executor's leadership status based on the response from the order queue's heartbeat. 
    # Here, we instead derive leadership purely from peer-to-peer checks.
    def _set_leadership(self, leader_id, granted):
        with self._state_lock:
            self.leader_id = leader_id
            self.is_leader = granted

    # This method increments the count of processed orders in a thread-safe manner. 
    # It is called whenever the executor successfully dequeues an order for execution.
    def _increment_processed(self):
        with self._state_lock:
            self.processed_orders += 1

    def _build_stock_reservations(self, order_payload_json):
        """Create stock reservations for DB participant; returns list[(title, quantity)] or None."""
        try:
            payload = json.loads(order_payload_json or "{}")
        except json.JSONDecodeError as err:
            print(f"[Q] execute_order: invalid JSON ({err})")
            return None

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
            return None

        print(f"[Q] execute_order: parsed {len(lines)} reservation(s): {lines}")
        return lines

    def two_phase_commit(self, order_id, order_payload_json, db_stub, payment_stub):
        reservations = self._build_stock_reservations(order_payload_json)
        if reservations is None:
            return False

        db_decrement_response = None
        try:
            db_decrement_response = db_stub.DecrementStock(
                books_pb2.DecrementStockRequest(
                    order_id=order_id,
                    reservations=[
                        books_pb2.StockReservation(title=title, quantity=quantity)
                        for title, quantity in reservations
                    ],
                ),
                timeout=RPC_TIMEOUT_SECONDS,
            )
            print(
                f"[EX:{self.executor_id}] DB DecrementStock order={order_id} "
                f"success={db_decrement_response.success} msg={db_decrement_response.message!r} "
                f"stock={db_decrement_response.stock}"
            )
        except grpc.RpcError as err:
            print(f"[EX:{self.executor_id}] DB DecrementStock failed order={order_id} err={err}")
            return False

        if not db_decrement_response.success:
            return False

        db_ready = False
        payment_ready = False
        db_ready = True

        try:
            payment_prepare_response = payment_stub.Prepare(
                payment_pb2.PrepareRequest(order_id=order_id), timeout=RPC_TIMEOUT_SECONDS
            )
            payment_ready = bool(payment_prepare_response.ready)
            print(f"[EX:{self.executor_id}] Payment Prepare order={order_id} ready={payment_ready}")
        except grpc.RpcError as err:
            print(f"[EX:{self.executor_id}] Payment Prepare failed order={order_id} err={err}")

        decision_commit = db_ready and payment_ready
        if decision_commit:
            db_commit_ok = False
            payment_commit_ok = False
            try:
                db_commit_response = db_stub.Commit(
                    books_pb2.CommitRequest(order_id=order_id), timeout=RPC_TIMEOUT_SECONDS
                )
                db_commit_ok = bool(db_commit_response.success)
                print(f"[EX:{self.executor_id}] DB Commit order={order_id} success={db_commit_ok} msg={db_commit_response.message!r}")
            except grpc.RpcError as err:
                print(f"[EX:{self.executor_id}] DB Commit failed order={order_id} err={err}")

            try:
                payment_commit_response = payment_stub.Commit(
                    payment_pb2.CommitRequest(order_id=order_id), timeout=RPC_TIMEOUT_SECONDS
                )
                payment_commit_ok = bool(payment_commit_response.success)
                print(f"[EX:{self.executor_id}] Payment Commit order={order_id} success={payment_commit_ok}")
            except grpc.RpcError as err:
                print(f"[EX:{self.executor_id}] Payment Commit failed order={order_id} err={err}")

            return db_commit_ok and payment_commit_ok

        try:
            db_stub.Abort(books_pb2.AbortRequest(order_id=order_id), timeout=RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as err:
            print(f"[EX:{self.executor_id}] DB Abort failed order={order_id} err={err}")

        try:
            payment_stub.Abort(payment_pb2.AbortRequest(order_id=order_id), timeout=RPC_TIMEOUT_SECONDS)
        except grpc.RpcError as err:
            print(f"[EX:{self.executor_id}] Payment Abort failed order={order_id} err={err}")

        print(f"[EX:{self.executor_id}] 2PC decision=ABORT order={order_id} db_ready={db_ready} payment_ready={payment_ready}")
        return False
    
    # This is the main loop of the executor service. It continuously sends heartbeats to the order queue 
    # to maintain leadership status, and attempts to dequeue orders for execution if it is the leader.
    # With the bully-style election, leadership is determined by talking to peer executors, not the orderqueue.
    def run(self):
        while True:
            try:
                # Determine the current leader purely based on peer-to-peer checks.
                self._update_leader_from_peers()

                # If this executor is the leader, attempt to dequeue an order for execution.
                # If the queue is empty or if this executor is not the leader, wait before retrying.
                if self.is_leader:
                    dequeue = self.queue_stub.Dequeue(
                        orderqueue.DequeueRequest(executor_id=self.executor_id)
                    )
                    if dequeue.has_order:
                        order = dequeue.order
                        print(f"[EX:{self.executor_id}] Dequeued order {order.order_id} for execution")

                        # Coordinator: run 2PC over DB and payment participants.
                        with grpc.insecure_channel('booksdb-primary:50051') as db_channel:
                            db_stub = booksdatabase_grpc.BooksDatabaseStub(db_channel)
                            with grpc.insecure_channel(PAYMENT_TARGET) as payment_channel:
                                payment_stub = payment_grpc.PaymentStub(payment_channel)
                                success = self.two_phase_commit(
                                    order_id=order.order_id,
                                    order_payload_json=order.order_payload_json,
                                    db_stub=db_stub,
                                    payment_stub=payment_stub,
                                )

                        if not success:
                            print(f"[EX:{self.executor_id}] 2PC FAILED for order {order.order_id}")
                        else:
                            print(f"[EX:{self.executor_id}] 2PC COMMITTED for order {order.order_id}")

                        self._increment_processed()
                    else:
                        time.sleep(EMPTY_QUEUE_BACKOFF_SECONDS)
                else:
                    time.sleep(LEADER_POLL_INTERVAL_SECONDS)

                time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            except grpc.RpcError as rpc_error:
                print(f"[EX:{self.executor_id}] Queue RPC error: {rpc_error}")
                time.sleep(RETRY_BACKOFF_SECONDS)

    # Peer-to-peer leader election: query all known executors via gRPC and
    # elect the one with the highest executor_id as leader (a bully-style rule).
    def _update_leader_from_peers(self):
        # Collect all known executor ids (including ourselves).
        peer_ids = {self.executor_id}

        for target in self.peer_targets:
            try:
                with grpc.insecure_channel(target) as channel:
                    stub = executor_grpc.OrderExecutorServiceStub(channel)
                    resp = stub.GetStatus(executor.GetStatusRequest(), timeout=0.5)
                    if resp.executor_id:
                        peer_ids.add(resp.executor_id)
            except grpc.RpcError:
                # Peer might be down or unreachable; ignore for this round.
                continue

        # Bully rule: the highest id that responded becomes leader.
        leader_id = max(peer_ids) if peer_ids else self.executor_id
        granted = (leader_id == self.executor_id)
        self._set_leadership(leader_id, granted)

# start the executor service and connect it to the order queue
def launch_executor(executor_id):
    queue_target = os.getenv("ORDER_QUEUE_TARGET", "orderqueue:50054")
    peers_env = os.getenv("EXECUTOR_PEERS", "")
    peer_targets = [p.strip() for p in peers_env.split(",") if p.strip()]
    with grpc.insecure_channel(queue_target) as queue_channel:
        queue_stub = orderqueue_grpc.OrderQueueServiceStub(queue_channel)

        svc = ExecutorService(executor_id=executor_id, queue_stub=queue_stub, peer_targets=peer_targets)

        worker = threading.Thread(target=svc.run, daemon=True)
        worker.start()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        executor_grpc.add_OrderExecutorServiceServicer_to_server(svc, server)
        server.add_insecure_port("[::]:50055")
        server.start()
        print(f"Order executor {executor_id} listening on 50055")
        server.wait_for_termination()


if __name__ == "__main__":
    executor_id = os.getenv("EXECUTOR_ID", os.getenv("HOSTNAME", "executor"))
    launch_executor(executor_id)
