import grpc
from concurrent import futures

from utils.pb.payment import payment_pb2 as payment_pb2
from utils.pb.payment import payment_pb2_grpc as payment_grpc

PAYMENT_PORT = 50059


# gRPC Python names the servicer base class PaymentServicer (not "PaymentService").
class PaymentService(payment_grpc.PaymentServicer):
    def __init__(self):
        self.prepared = False

    def Prepare(self, request, context):
        # Dummy validation logic, e.g. check funds
        self.prepared = True
        return payment_pb2.PrepareResponse(ready=True)

    def Commit(self, request, context):
        if self.prepared:
            print(f"Payment committed for order {request.order_id}")
            self.prepared = False
        return payment_pb2.CommitResponse(success=True)

    def Abort(self, request, context):
        self.prepared = False
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
