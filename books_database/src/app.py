import grpc
from concurrent import futures
import os
import threading

from utils.pb.books_database import booksdatabase_pb2
from utils.pb.books_database import booksdatabase_pb2_grpc

# Titles must match checkout line items after splitting on " by " (see orderqueue execute_order).
DEFAULT_INVENTORY = {
    "Harry Potter and the Philosopher's Stone": 500,
    "The Hobbit": 500,
}

class BooksDatabaseTwoPhaseParticipant(booksdatabase_pb2_grpc.BooksDatabaseServicer):
    def __init__(self, backup_stubs):
        self.backups = backup_stubs
        self.store = {}
        self._prepared_updates = {}
        self._prepared_lock = threading.Lock()

    def _replica_label(self):
        return "BooksDB-primary" if self.backups else "BooksDB-backup"

    def Read(self, request, context):
        title = request.title
        stock = self.store.get(title, 0)
        print(f"[{self._replica_label()}] Read title={title!r} stock={stock}")
        return booksdatabase_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        title = request.title
        new_stock = request.new_stock
        self.store[title] = new_stock
        print(f"[{self._replica_label()}] Write title={title!r} new_stock={new_stock}")

        for index, backup in enumerate(self.backups):
            try:
                backup.Write(request)
                print(f"[BooksDB-primary] replicated write to backup[{index}] title={title!r}")
            except Exception as replication_error:
                print(f"[BooksDB-primary] replication to backup[{index}] failed: {replication_error}")

        return booksdatabase_pb2.WriteResponse(success=True)

    def Prepare(self, request, context):
        order_id = request.order_id
        updates = list(request.updates)
        if not order_id:
            return booksdatabase_pb2.PrepareResponse(ready=False, message="missing order_id")
        if not updates:
            return booksdatabase_pb2.PrepareResponse(ready=False, message="no staged updates")

        for update in updates:
            if update.new_stock < 0:
                return booksdatabase_pb2.PrepareResponse(
                    ready=False,
                    message=f"negative stock for title {update.title}",
                )

        with self._prepared_lock:
            self._prepared_updates[order_id] = updates

        print(f"[BooksDB-2PC] Prepared order={order_id} staged_updates={len(updates)}")
        return booksdatabase_pb2.PrepareResponse(ready=True, message="prepared")

    def Commit(self, request, context):
        order_id = request.order_id
        with self._prepared_lock:
            staged_updates = self._prepared_updates.pop(order_id, None)

        if staged_updates is None:
            return booksdatabase_pb2.CommitResponse(success=False, message="order not prepared")

        for update in staged_updates:
            self.Write(
                booksdatabase_pb2.WriteRequest(title=update.title, new_stock=update.new_stock),
                context,
            )

        print(f"[BooksDB-2PC] Committed order={order_id} applied_updates={len(staged_updates)}")
        return booksdatabase_pb2.CommitResponse(success=True, message="committed")

    def Abort(self, request, context):
        order_id = request.order_id
        with self._prepared_lock:
            self._prepared_updates.pop(order_id, None)
        print(f"[BooksDB-2PC] Aborted order={order_id}")
        return booksdatabase_pb2.AbortResponse(aborted=True)

# Helper to get backup addresses from env (comma-separated)
def get_backup_addresses():
    return [addr for addr in os.getenv("BOOKSDB_BACKUPS", "").split(",") if addr]


def serve_books():
    # Create gRPC stubs for backup replicas
    backup_addresses = get_backup_addresses()
    backup_stubs = []
    for addr in backup_addresses:
        channel = grpc.insecure_channel(addr)
        stub = booksdatabase_pb2_grpc.BooksDatabaseStub(channel)
        backup_stubs.append(stub)

    # Create the 2PC-capable primary/backup servicer.
    participant_servicer = BooksDatabaseTwoPhaseParticipant(backup_stubs)
    participant_servicer.store.update(DEFAULT_INVENTORY)
    role = "primary" if backup_stubs else "backup"
    print(
        f"[BooksDB] {role} started with {len(participant_servicer.store)} seeded title(s): "
        f"{list(participant_servicer.store.keys())}"
    )

    # Start the gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    booksdatabase_pb2_grpc.add_BooksDatabaseServicer_to_server(participant_servicer, server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(
        f"[BooksDB] listening on 50051 role={role} backup_targets={backup_addresses!r}"
    )
    server.wait_for_termination()


if __name__ == "__main__":
    serve_books()
