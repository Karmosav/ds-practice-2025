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

executor_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/executor'))
sys.path.insert(0, executor_grpc_path)
import executor_pb2 as executor
import executor_pb2_grpc as executor_grpc

HEARTBEAT_INTERVAL_SECONDS = 1.0
LEADER_POLL_INTERVAL_SECONDS = 0.5
EMPTY_QUEUE_BACKOFF_SECONDS = 1.0
RETRY_BACKOFF_SECONDS = 2.0


class ExecutorService(executor_grpc.OrderExecutorServiceServicer):
    def __init__(self, executor_id, queue_stub):
        self.executor_id = executor_id
        self.queue_stub = queue_stub
        self.leader_id = ""
        self.is_leader = False
        self.processed_orders = 0
        self._state_lock = threading.Lock()
    
    # Initiates the leader election process by registering with the order queue. 
    # This will allow the executor to be considered for leadership and start sending heartbeats.
    def start_leader_election(self):
        self.queue_stub.RegisterExecutor(
            orderqueue.RegisterExecutorRequest(executor_id=self.executor_id)
        )
    
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
    # It ensures that the executor's internal state is consistent with the order queue's view of leadership.
    def _set_leadership(self, response):
        with self._state_lock:
            self.leader_id = response.leader_id
            self.is_leader = response.granted

    # This method increments the count of processed orders in a thread-safe manner. 
    # It is called whenever the executor successfully dequeues an order for execution.
    def _increment_processed(self):
        with self._state_lock:
            self.processed_orders += 1

    # This is the main loop of the executor service. It continuously sends heartbeats to the order queue 
    # to maintain leadership status, and attempts to dequeue orders for execution if it is the leader. 
    # It also handles backoff and retry logic in case of errors or if the queue is empty.
    def run(self):
        while True:
            try:
                # Send a heartbeat to the order queue and update leadership status based on the response. 
                heartbeat = self.queue_stub.Heartbeat(
                    orderqueue.ExecutorHeartbeatRequest(executor_id=self.executor_id)
                )
                self._set_leadership(heartbeat)

                # If this executor is the leader, attempt to dequeue an order for execution. 
                # If the queue is empty or if this executor is not the leader, wait before retrying.
                if heartbeat.granted:
                    dequeue = self.queue_stub.Dequeue(
                        orderqueue.DequeueRequest(executor_id=self.executor_id)
                    )
                    if dequeue.has_order:
                        print(f"[EX:{self.executor_id}] Order {dequeue.order.order_id} is being executed...")
                        self._increment_processed()
                    else:
                        time.sleep(EMPTY_QUEUE_BACKOFF_SECONDS)
                else:
                    time.sleep(LEADER_POLL_INTERVAL_SECONDS)

                time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            except grpc.RpcError as rpc_error:
                print(f"[EX:{self.executor_id}] Queue RPC error: {rpc_error}")
                time.sleep(RETRY_BACKOFF_SECONDS)
                
# start the executor service and connect it to the order queue
def launch_executor(executor_id):
    queue_target = os.getenv("ORDER_QUEUE_TARGET", "orderqueue:50054")
    with grpc.insecure_channel(queue_target) as queue_channel:
        queue_stub = orderqueue_grpc.OrderQueueServiceStub(queue_channel)

        svc = ExecutorService(executor_id=executor_id, queue_stub=queue_stub)
        svc.start_leader_election()

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
