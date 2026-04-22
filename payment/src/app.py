import grpc
from concurrent import futures
import threading
import os
import json
from pathlib import Path

from utils.pb.payment import payment_pb2 as payment_pb2
from utils.pb.payment import payment_pb2_grpc as payment_grpc

PAYMENT_PORT = 50059
DEFAULT_PAYMENT_STATE_FILE = os.getenv("PAYMENT_STATE_FILE", "/app/payment/state/payment_state.json")


# gRPC Python names the servicer base class PaymentServicer (not "PaymentService").
class PaymentService(payment_grpc.PaymentServicer):
    def __init__(self, state_file=DEFAULT_PAYMENT_STATE_FILE):
        self.state_file = Path(state_file)
        self.prepared_orders = set()
        self.committed_orders = set()
        self.aborted_orders = set()
        self._lock = threading.Lock()
        self._load_state()

    def _persist_state_locked(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        temp_path.write_text(
            json.dumps(
                {
                    "prepared_orders": sorted(self.prepared_orders),
                    "committed_orders": sorted(self.committed_orders),
                    "aborted_orders": sorted(self.aborted_orders),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        temp_path.replace(self.state_file)

    def _load_state(self):
        if not self.state_file.exists():
            with self._lock:
                self._persist_state_locked()
            return

        try:
            raw = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as load_error:
            print(f"[payment] failed to load state file, starting fresh: {load_error}")
            with self._lock:
                self.prepared_orders = set()
                self.committed_orders = set()
                self.aborted_orders = set()
                self._persist_state_locked()
            return

        with self._lock:
            self.prepared_orders = set(raw.get("prepared_orders") or [])
            self.committed_orders = set(raw.get("committed_orders") or [])
            self.aborted_orders = set(raw.get("aborted_orders") or [])

        print(
            "[payment] state restored "
            f"prepared={len(self.prepared_orders)} "
            f"committed={len(self.committed_orders)} "
            f"aborted={len(self.aborted_orders)}"
        )

    def Prepare(self, request, context):
        # Dummy validation logic, e.g. check funds
        with self._lock:
            if request.order_id in self.committed_orders:
                return payment_pb2.PrepareResponse(ready=True)
            if request.order_id in self.aborted_orders:
                return payment_pb2.PrepareResponse(ready=False)
            self.prepared_orders.add(request.order_id)
            self._persist_state_locked()
        return payment_pb2.PrepareResponse(ready=True)

    def Commit(self, request, context):
        with self._lock:
            if request.order_id in self.committed_orders:
                return payment_pb2.CommitResponse(success=True)

            prepared = request.order_id in self.prepared_orders
            if prepared:
                self.prepared_orders.remove(request.order_id)
                self.committed_orders.add(request.order_id)
                self.aborted_orders.discard(request.order_id)
                self._persist_state_locked()

        if prepared:
            print(f"Payment committed for order {request.order_id}")
            return payment_pb2.CommitResponse(success=True)
        return payment_pb2.CommitResponse(success=False)

    def Abort(self, request, context):
        with self._lock:
            if request.order_id in self.committed_orders:
                return payment_pb2.AbortResponse(aborted=False)
            self.prepared_orders.discard(request.order_id)
            self.aborted_orders.add(request.order_id)
            self._persist_state_locked()
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
