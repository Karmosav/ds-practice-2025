import sys
import os
import time
import threading
import json
import grpc
from concurrent import futures

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")

orderqueue_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/orderqueue'))
sys.path.insert(0, orderqueue_grpc_path)
import orderqueue_pb2 as orderqueue
import orderqueue_pb2_grpc as orderqueue_grpc

executor_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/executor'))
sys.path.insert(0, executor_grpc_path)
import executor_pb2 as executor
import executor_pb2_grpc as executor_grpc

books_database_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/books_database'))
sys.path.insert(0, books_database_grpc_path)
import booksdatabase_pb2 as books_pb2
import booksdatabase_pb2_grpc as booksdatabase_grpc

HEARTBEAT_INTERVAL_SECONDS = 1.0
LEADER_POLL_INTERVAL_SECONDS = 0.5
EMPTY_QUEUE_BACKOFF_SECONDS = 1.0
RETRY_BACKOFF_SECONDS = 2.0


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

                        # Connect to the primary books database service and execute
                        # the inventory update workflow for this order.
                        with grpc.insecure_channel('booksdb-primary:50051') as db_channel:
                            db_stub = booksdatabase_grpc.BooksDatabaseStub(db_channel)
                            success = self.execute_order(order.order_payload_json, db_stub)

                        if not success:
                            print(f"[EX:{self.executor_id}] Inventory workflow FAILED for order {order.order_id}")
                        else:
                            print(f"[EX:{self.executor_id}] Inventory workflow completed for order {order.order_id}")

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
