import grpc
from concurrent import futures
import threading

from utils.pb.payment import payment_pb2 as payment_pb2
from utils.pb.payment import payment_pb2_grpc as payment_grpc

PAYMENT_PORT = 50059


# gRPC Python names the servicer base class PaymentServicer (not "PaymentService").
class PaymentService(payment_grpc.PaymentServicer):
    def __init__(self):
        self.prepared_orders = set()
        self._lock = threading.Lock()

    def Prepare(self, request, context):
        # Dummy validation logic, e.g. check funds
        with self._lock:
            self.prepared_orders.add(request.order_id)
        return payment_pb2.PrepareResponse(ready=True)

    def Commit(self, request, context):
        with self._lock:
            prepared = request.order_id in self.prepared_orders
            if prepared:
                self.prepared_orders.remove(request.order_id)

        if prepared:
            print(f"Payment committed for order {request.order_id}")
            return payment_pb2.CommitResponse(success=True)
        return payment_pb2.CommitResponse(success=False)

    def Abort(self, request, context):
        with self._lock:
            self.prepared_orders.discard(request.order_id)
        print(f"Payment aborted for order {request.order_id}")
        return payment_pb2.AbortResponse(aborted=True)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    payment_grpc.add_PaymentServicer_to_server(PaymentService(), server)
    server.add_insecure_port(f"[::]:{PAYMENT_PORT}")
    server.start()
    print(f"Payment (2PC dummy) on {PAYMENT_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
